"""
ECDT - Phase 5
Observer Agent - TimescaleDB Consumer.

This module retrieves temporal context from the existing
Digital Twin TimescaleDB layer.

Architecture:

    AnomalyInput
         |
         v
    TimescaleConsumer
         |
         v
    timeseries_queries.py
         |
         v
    TimescaleClient
         |
         v
    TimescaleDB
         |
         v
    TemporalContext

Responsibilities
----------------
- Convert Phase 2 timestamps to UTC datetime.
- Retrieve metric observations around an anomaly.
- Compute lightweight temporal statistics.
- Return a structured TemporalContext.

This module does NOT:
- perform anomaly detection;
- create Neo4j nodes;
- perform root-cause analysis;
- execute raw SQL;
- create a new database connection.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import (
    AnomalyInput,
    TemporalContext,
)

from ...digital_twin.timescale_client import (
    TimescaleClient,
)

from ...digital_twin.timeseries_queries import (
    get_metrics_around_timestamp,
)


class TimescaleConsumer:
    """
    Read-only TimescaleDB consumer for the Observer Agent.

    The consumer reuses the existing Phase 4 TimescaleClient
    and query layer.
    """

    def __init__(
        self,
        client: TimescaleClient,
    ) -> None:
        """
        Initialize the TimescaleDB consumer.

        Parameters
        ----------
        client:
            Existing ECDT TimescaleClient instance.
        """

        if client is None:
            raise ValueError(
                "TimescaleDB client must not be None."
            )

        self.client = client

    # ------------------------------------------------------------------
    # Timestamp conversion
    # ------------------------------------------------------------------

    @staticmethod
    def timestamp_to_datetime(
        timestamp: int | float,
    ) -> datetime:
        """
        Convert an Observer Unix timestamp in seconds into a timezone-aware
        UTC datetime.  Phase 2 millisecond timestamps are normalized by
        ObserverAgent at the boundary between the two phases.

        Example:

            1705354580
                ->
            2024-01-15T...
        """

        try:
            timestamp_value = float(timestamp)

        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Anomaly timestamp must be a valid Unix "
                "epoch timestamp in seconds."
            ) from exc

        return datetime.fromtimestamp(
            timestamp_value,
            tz=timezone.utc,
        )

    # ------------------------------------------------------------------
    # Query TimescaleDB
    # ------------------------------------------------------------------

    def get_observations_around_anomaly(
        self,
        anomaly: AnomalyInput,
        window_minutes: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Retrieve TimescaleDB observations around an anomaly.

        The existing Phase 4 query:

            get_metrics_around_timestamp()

        is reused directly.

        Parameters
        ----------
        anomaly:
            Phase 5 anomaly input.

        window_minutes:
            Number of minutes before and after the anomaly.

        Returns
        -------
        list[dict[str, Any]]
            TimescaleDB observations.
        """

        if window_minutes < 0:
            raise ValueError(
                "window_minutes must be >= 0."
            )

        if not anomaly.resource_id:
            raise ValueError(
                "Anomaly resource_id must not be empty."
            )

        timestamp = self.timestamp_to_datetime(
            anomaly.timestamp
        )

        observations = get_metrics_around_timestamp(
            self.client,
            resource_id=anomaly.resource_id,
            timestamp=timestamp,
            window_minutes=window_minutes,
        )

        return observations

    # ------------------------------------------------------------------
    # Filter metric
    # ------------------------------------------------------------------

    @staticmethod
    def filter_metric_observations(
        observations: list[dict[str, Any]],
        metric_name: str,
    ) -> list[dict[str, Any]]:
        """
        Keep only observations belonging to the requested metric.
        """

        if not metric_name:
            raise ValueError(
                "metric_name must not be empty."
            )

        return [
            observation
            for observation in observations
            if observation.get("metric_name")
            == metric_name
        ]

    @staticmethod
    def filter_case_observations(
        observations: list[dict[str, Any]],
        case_id: str,
    ) -> list[dict[str, Any]]:
        """Keep only observations belonging to the anomaly's RCAEval case."""

        if not case_id:
            raise ValueError(
                "case_id must not be empty."
            )

        return [
            observation
            for observation in observations
            if observation.get("case_id") == case_id
        ]

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @staticmethod
    def compute_statistics(
        observations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Compute lightweight statistics over the temporal window.

        Statistics:

            observation_count
            minimum
            maximum
            mean
            first_value
            last_value
            delta
        """

        values: list[float] = []

        for observation in observations:

            value = observation.get("value")

            if value is None:
                continue

            try:
                numeric_value = float(value)

            except (TypeError, ValueError):
                continue

            values.append(numeric_value)

        if not values:
            return {
                "observation_count": 0,
                "minimum": None,
                "maximum": None,
                "mean": None,
                "first_value": None,
                "last_value": None,
                "delta": None,
            }

        first_value = values[0]
        last_value = values[-1]

        return {
            "observation_count": len(values),
            "minimum": min(values),
            "maximum": max(values),
            "mean": sum(values) / len(values),
            "first_value": first_value,
            "last_value": last_value,
            "delta": last_value - first_value,
        }

    # ------------------------------------------------------------------
    # Temporal context
    # ------------------------------------------------------------------

    def get_temporal_context(
        self,
        anomaly: AnomalyInput,
        window_minutes: int = 5,
    ) -> TemporalContext:
        """
        Retrieve and structure the temporal context associated
        with an anomaly.

        Pipeline:

            AnomalyInput
                 |
                 v
            TimescaleDB
                 |
                 v
            resource + metric
                 |
                 v
            TemporalContext
        """

        observations = (
            self.get_observations_around_anomaly(
                anomaly=anomaly,
                window_minutes=window_minutes,
            )
        )

        case_observations = (
            self.filter_case_observations(
                observations=observations,
                case_id=anomaly.case_id,
            )
        )

        metric_observations = (
            self.filter_metric_observations(
                observations=case_observations,
                metric_name=anomaly.metric_name,
            )
        )

        statistics = self.compute_statistics(
            metric_observations
        )

        statistics["anomaly_value"] = (
            anomaly.value
        )

        statistics["anomaly_score"] = (
            anomaly.score
        )

        return TemporalContext(
            resource_id=anomaly.resource_id,

            metric_name=anomaly.metric_name,

            signal_type=anomaly.signal_type,

            anomaly_timestamp=anomaly.timestamp,

            window_before_seconds=window_minutes * 60,

            window_after_seconds=window_minutes * 60,

            observations=metric_observations,

            statistics=statistics,
        )
