"""
ECDT - Phase 5.5
Observer Agent.

The Observer is the first cognitive component of ECDT.

Responsibilities
----------------
1. Receive an AnomalyEvent produced by Phase 2.
2. Convert it into the Observer input model.
3. Retrieve temporal context from TimescaleDB.
4. Qualify the anomaly.
5. Build a structured Incident.
6. Persist the Incident into Neo4j.
7. Return an IncidentContext for downstream agents.

The Observer does NOT perform root-cause analysis.

Pipeline
--------

    AnomalyEvent
         |
         v
    AnomalyInput
         |
         v
    TimescaleDB
         |
         v
    TemporalContext
         |
         v
    Qualification
         |
         v
    Incident
         |
         v
    Neo4j
         |
         v
    IncidentContext
"""

from __future__ import annotations

from typing import Any

from src.agents.observer.incident_builder import (
    build_incident,
    determine_confidence,
)

from src.agents.observer.incident_persistence import (
    IncidentPersistence,
)

from src.agents.observer.models import (
    AnomalyInput,
    IncidentContext,
    TemporalContext,
)

from src.agents.observer.timescale_consumer import (
    TimescaleConsumer,
)

from src.ingestion.models import (
    AnomalyEvent,
    validate_operational_metadata,
)


class ObserverAgent:
    """
    First cognitive agent of ECDT.

    The agent orchestrates:

        AnomalyEvent
            ↓
        TimescaleConsumer
            ↓
        IncidentBuilder
            ↓
        IncidentPersistence
            ↓
        IncidentContext
    """

    def __init__(
        self,
        timescale_consumer: TimescaleConsumer,
        incident_persistence: IncidentPersistence,
        window_minutes: int = 5,
        minimum_confidence: float = 0.0,
    ) -> None:
        """
        Initialize the ObserverAgent.

        Parameters
        ----------
        timescale_consumer:
            Component responsible for retrieving temporal
            context from TimescaleDB.

        incident_persistence:
            Component responsible for writing incidents
            to Neo4j.

        window_minutes:
            Temporal window used around the anomaly.

        minimum_confidence:
            Minimum confidence required to create an incident.
            Default is 0.0 because the Observer should preserve
            detected anomalies rather than silently discard them.
        """

        if timescale_consumer is None:
            raise ValueError(
                "timescale_consumer must not be None."
            )

        if incident_persistence is None:
            raise ValueError(
                "incident_persistence must not be None."
            )

        if window_minutes < 0:
            raise ValueError(
                "window_minutes must be >= 0."
            )

        if not 0.0 <= minimum_confidence <= 1.0:
            raise ValueError(
                "minimum_confidence must be between 0.0 and 1.0."
            )

        self.timescale_consumer = (
            timescale_consumer
        )

        self.incident_persistence = (
            incident_persistence
        )

        self.window_minutes = window_minutes

        self.minimum_confidence = (
            minimum_confidence
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(
        self,
        anomaly: AnomalyEvent,
    ) -> IncidentContext:
        """
        Process one anomaly from Phase 2.

        Pipeline:

            AnomalyEvent
                ↓
            AnomalyInput
                ↓
            TemporalContext
                ↓
            Qualification
                ↓
            Incident
                ↓
            Neo4j
                ↓
            IncidentContext
        """

        if anomaly is None:
            raise ValueError(
                "anomaly must not be None."
            )

        # --------------------------------------------------------------
        # 1. Convert Phase 2 anomaly
        # --------------------------------------------------------------

        anomaly_input = (
            self._to_anomaly_input(
                anomaly
            )
        )

        # --------------------------------------------------------------
        # 2. Retrieve temporal context
        # --------------------------------------------------------------

        temporal_context = (
            self.timescale_consumer
            .get_temporal_context(
                anomaly_input,
                window_minutes=self.window_minutes,
            )
        )

        # --------------------------------------------------------------
        # 3. Qualify anomaly
        # --------------------------------------------------------------

        qualification = (
            self._qualify(
                anomaly_input,
                temporal_context,
            )
        )

        if (
            qualification["confidence"]
            < self.minimum_confidence
        ):
            raise ValueError(
                "Anomaly confidence is below "
                "the Observer minimum threshold."
            )

        # --------------------------------------------------------------
        # 4. Build Incident
        # --------------------------------------------------------------

        incident = build_incident(
            anomaly_input
        )

        # --------------------------------------------------------------
        # 5. Persist Incident
        # --------------------------------------------------------------

        self.incident_persistence.persist_incident(
            incident
        )

        # --------------------------------------------------------------
        # 6. Build downstream context
        # --------------------------------------------------------------

        return IncidentContext(
            incident=incident,

            resource_id=incident.resource_id,

            detection_timestamp=incident.detected_at,

            signal_type=incident.signal_type,

            metric_name=incident.metric_name,

            observed_value=incident.observed_value,

            anomaly_score=incident.anomaly_score,

            temporal_context=(
                temporal_context
            ),

            qualification=qualification,

            persisted=True,

            incident_id=(
                incident.incident_id
            ),

            case_id=(
                incident.case_id
            ),
        )

    # ------------------------------------------------------------------
    # Qualification
    # ------------------------------------------------------------------

    def _qualify(
        self,
        anomaly: AnomalyInput,
        temporal_context: TemporalContext,
    ) -> dict[str, Any]:
        """
        Qualify an anomaly using the information already
        available from Phase 2 and TimescaleDB.

        This is NOT root-cause analysis.

        The Observer only determines whether the anomaly
        is sufficiently supported to become an Incident.
        """

        confidence = self._extract_confidence(
            anomaly,
            temporal_context,
        )

        observation_count = (
            temporal_context.statistics.get(
                "observation_count",
                0,
            )
        )

        return {
            "is_qualified": (
                confidence
                >= self.minimum_confidence
            ),

            "confidence": confidence,

            "observation_count": (
                observation_count
            ),

            "detection_method": (
                self._enum_value(
                    anomaly.detection_method
                )
            ),

            "score": float(
                anomaly.score
            ),

            "resource_id": (
                anomaly.resource_id
            ),

            "signal_type": (
                anomaly.signal_type
            ),

            "metric_name": (
                anomaly.metric_name
            ),
        }

    # ------------------------------------------------------------------
    # Anomaly conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _to_anomaly_input(
        anomaly: AnomalyEvent,
    ) -> AnomalyInput:
        """
        Convert the Phase 2 AnomalyEvent into the Observer
        AnomalyInput contract.

        Phase 2 uses `service`.
        The Observer uses `resource_id`.

        Phase 2 timestamps are represented as Unix milliseconds, while the
        TimescaleDB query layer expects seconds. This adapter owns that
        conversion so neither validated earlier phase needs to change.
        """

        if not anomaly.service:
            raise ValueError(
                "Phase 2 anomaly service must not be empty."
            )

        if not anomaly.is_anomaly:
            raise ValueError(
                "Observer only accepts anomalies marked as anomalous."
            )

        validate_operational_metadata(
            anomaly.metadata,
            context="Observer input",
        )

        timestamp = ObserverAgent._timestamp_to_seconds(
            anomaly.timestamp
        )

        signal_type = ObserverAgent._enum_value(
            anomaly.signal_type
        )

        metadata = dict(anomaly.metadata)
        metadata["source_timestamp"] = anomaly.timestamp
        metadata["source_timestamp_unit"] = (
            "milliseconds"
            if timestamp != anomaly.timestamp
            else "seconds"
        )

        if anomaly.threshold is not None:
            metadata["threshold"] = anomaly.threshold

        return AnomalyInput(
            event_id=anomaly.event_id,

            case_id=anomaly.case_id,

            timestamp=timestamp,

            resource_id=anomaly.service,

            signal_type=(
                signal_type
            ),

            metric_name=(
                f"{anomaly.service}_"
                f"{signal_type}"
            ),

            value=float(
                anomaly.value
            ),

            score=float(
                anomaly.score
            ),

            detection_method=(
                anomaly.detection_method
            ),

            incident_type=(
                anomaly.incident_type
            ),

            threshold=anomaly.threshold,

            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_confidence(
        anomaly: AnomalyInput,
        temporal_context: TemporalContext,
    ) -> float:
        """
        Extract the qualification confidence.

        Use exactly the same deterministic confidence rule as
        IncidentBuilder. The temporal context is deliberately not treated as
        a root-cause or confidence signal at this stage.
        """

        del temporal_context

        return determine_confidence(
            score=float(anomaly.score),
            detection_method=anomaly.detection_method,
        )

    @staticmethod
    def _timestamp_to_seconds(
        timestamp: int | float,
    ) -> int:
        """Normalize Phase 2 milliseconds or legacy seconds to seconds."""

        try:
            numeric_timestamp = float(timestamp)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Phase 2 anomaly timestamp must be numeric."
            ) from exc

        # Epoch values in milliseconds are currently about 10^12; seconds
        # are about 10^9. Supporting both keeps existing integrations valid.
        if abs(numeric_timestamp) >= 10_000_000_000:
            numeric_timestamp /= 1000

        return int(numeric_timestamp)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _enum_value(
        value: Any,
    ) -> str:
        """
        Return enum.value when available.
        """

        if hasattr(
            value,
            "value",
        ):
            return str(
                value.value
            )

        return str(value)
