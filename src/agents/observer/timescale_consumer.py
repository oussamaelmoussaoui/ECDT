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
from math import floor, isfinite
from statistics import median
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
        Convert an Observer Unix timestamp in seconds or milliseconds into
        a timezone-aware UTC datetime.

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
                "epoch timestamp."
            ) from exc

        if not isfinite(timestamp_value):
            raise ValueError(
                "Anomaly timestamp must be finite."
            )

        if abs(timestamp_value) >= 10_000_000_000:
            timestamp_value /= 1000.0

        try:
            return datetime.fromtimestamp(
                timestamp_value,
                tz=timezone.utc,
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(
                "Anomaly timestamp is outside the supported range."
            ) from exc

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

            if not isfinite(numeric_value):
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

    @staticmethod
    def compute_temporal_completeness(
        observations: list[dict[str, Any]],
        *,
        requested_start_timestamp: float,
        requested_end_timestamp: float,
    ) -> dict[str, Any]:
        """
        Measure the effective temporal coverage of a requested window.

        Timestamps are expressed as Unix epoch seconds.
        The sampling interval is estimated from the median positive
        interval between consecutive unique observations.
        """

        try:
            requested_start = float(
                requested_start_timestamp
            )
            requested_end = float(
                requested_end_timestamp
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Requested temporal bounds must be valid timestamps."
            ) from exc

        if not (
            isfinite(requested_start)
            and isfinite(requested_end)
        ):
            raise ValueError(
                "Requested temporal bounds must be finite."
            )

        if requested_end < requested_start:
            raise ValueError(
                "Requested temporal end must not precede its start."
            )

        timestamps: set[float] = set()

        for observation in observations:
            timestamp = observation.get("timestamp")

            if isinstance(timestamp, datetime):
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc
                    )

                timestamp_seconds = timestamp.timestamp()

            else:
                try:
                    timestamp_seconds = float(timestamp)
                except (TypeError, ValueError):
                    continue

            if not isfinite(timestamp_seconds):
                continue

            if (
                requested_start
                <= timestamp_seconds
                <= requested_end
            ):
                timestamps.add(timestamp_seconds)

        ordered_timestamps = sorted(timestamps)
        observed_count = len(ordered_timestamps)

        base_result = {
            "requested_start_timestamp": requested_start,
            "requested_end_timestamp": requested_end,
            "requested_duration_seconds": (
                requested_end - requested_start
            ),
            "first_observation_timestamp": None,
            "last_observation_timestamp": None,
            "actual_coverage_duration_seconds": None,
            "estimated_sampling_interval_seconds": None,
            "expected_observation_count": None,
            "observed_unique_timestamp_count": observed_count,
            "missing_observation_count": None,
            "temporal_data_completeness_ratio": None,
        }

        if not ordered_timestamps:
            return base_result

        first_timestamp = ordered_timestamps[0]
        last_timestamp = ordered_timestamps[-1]

        base_result.update(
            {
                "first_observation_timestamp": first_timestamp,
                "last_observation_timestamp": last_timestamp,
                "actual_coverage_duration_seconds": (
                    last_timestamp - first_timestamp
                ),
            }
        )

        intervals = [
            current - previous
            for previous, current in zip(
                ordered_timestamps,
                ordered_timestamps[1:],
            )
            if current > previous
        ]

        if not intervals:
            return base_result

        sampling_interval = float(
            median(intervals)
        )

        requested_duration = (
            requested_end - requested_start
        )

        expected_count = (
            floor(
                requested_duration
                / sampling_interval
            )
            + 1
        )

        missing_count = max(
            expected_count - observed_count,
            0,
        )

        completeness_ratio = min(
            observed_count / expected_count,
            1.0,
        )

        base_result.update(
            {
                "estimated_sampling_interval_seconds": (
                    sampling_interval
                ),
                "expected_observation_count": expected_count,
                "missing_observation_count": missing_count,
                "temporal_data_completeness_ratio": (
                    completeness_ratio
                ),
            }
        )

        return base_result

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
        window_seconds = window_minutes * 60

        requested_start_timestamp = (
            float(anomaly.timestamp)
            - window_seconds
        )

        requested_end_timestamp = (
            float(anomaly.timestamp)
            + window_seconds
        )

        temporal_completeness = (
            self.compute_temporal_completeness(
                metric_observations,
                requested_start_timestamp=(
                    requested_start_timestamp
                ),
                requested_end_timestamp=(
                    requested_end_timestamp
                ),
            )
        )

        return TemporalContext(
            resource_id=anomaly.resource_id,

            metric_name=anomaly.metric_name,

            signal_type=anomaly.signal_type,

            anomaly_timestamp=anomaly.timestamp,
            window_before_seconds=window_seconds,

            window_after_seconds=window_seconds,

            rows_retrieved=len(metric_observations),

            numeric_observation_count=int(
                statistics["observation_count"]
            ),

            requested_start_timestamp=(
                temporal_completeness[
                    "requested_start_timestamp"
                ]
            ),

            requested_end_timestamp=(
                temporal_completeness[
                    "requested_end_timestamp"
                ]
            ),

            requested_duration_seconds=(
                temporal_completeness[
                    "requested_duration_seconds"
                ]
            ),

            first_observation_timestamp=(
                temporal_completeness[
                    "first_observation_timestamp"
                ]
            ),

            last_observation_timestamp=(
                temporal_completeness[
                    "last_observation_timestamp"
                ]
            ),

            actual_coverage_duration_seconds=(
                temporal_completeness[
                    "actual_coverage_duration_seconds"
                ]
            ),

            estimated_sampling_interval_seconds=(
                temporal_completeness[
                    "estimated_sampling_interval_seconds"
                ]
            ),

            expected_observation_count=(
                temporal_completeness[
                    "expected_observation_count"
                ]
            ),

            observed_unique_timestamp_count=(
                temporal_completeness[
                    "observed_unique_timestamp_count"
                ]
            ),

            missing_observation_count=(
                temporal_completeness[
                    "missing_observation_count"
                ]
            ),

            temporal_data_completeness_ratio=(
                temporal_completeness[
                    "temporal_data_completeness_ratio"
                ]
            ),

            observations=metric_observations,

            statistics=statistics,
        )
