"""
ECDT - Phase 5
Incident Builder.

This module transforms a Phase 2 anomaly into a structured
incident and prepares the context that will later be consumed
by the Diagnostic/Impact Agent.

Pipeline:

    AnomalyInput
        |
        +--> incident type
        +--> severity
        +--> confidence
        +--> deterministic incident ID
        |
        v
      Incident
        |
        v
  IncidentContext

Important:
    This module does NOT perform root-cause analysis.
    It only qualifies an already detected anomaly.
"""

from __future__ import annotations

import hashlib

from .models import (
    AnomalyInput,
    Incident,
    IncidentContext,
    IncidentSeverity,
    IncidentSource,
    IncidentStatus,
)

from ...ingestion.models import (
    DetectionMethod,
    IncidentType,
    validate_operational_metadata,
)


# ---------------------------------------------------------------------------
# Incident ID
# ---------------------------------------------------------------------------


def generate_incident_id(anomaly: AnomalyInput) -> str:
    """
    Generate a deterministic incident identifier.

    The same anomaly always produces the same incident ID.

    This is important for idempotent persistence in Neo4j later.

    Format:

        inc_<short_hash>

    The hash is generated from:

        case_id
        event_id
        timestamp
    """

    raw_identifier = (
        f"{anomaly.case_id}:"
        f"{anomaly.event_id}:"
        f"{anomaly.timestamp}"
    )

    digest = hashlib.sha256(
        raw_identifier.encode("utf-8")
    ).hexdigest()[:16]

    return f"inc_{digest}"


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------


def determine_severity(score: float) -> IncidentSeverity:
    """
    Determine incident severity from the anomaly score.

    Current thresholds:

        score < 3       -> LOW
        3 <= score < 6  -> MEDIUM
        6 <= score < 10 -> HIGH
        score >= 10     -> CRITICAL
    """

    if score < 0:
        raise ValueError(
            "Anomaly score must be >= 0."
        )

    if score < 3:
        return IncidentSeverity.LOW

    if score < 6:
        return IncidentSeverity.MEDIUM

    if score < 10:
        return IncidentSeverity.HIGH

    return IncidentSeverity.CRITICAL


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------


def determine_confidence(
    score: float,
    detection_method: DetectionMethod,
) -> float:
    """
    Determine the confidence assigned to the observation.

    The confidence is intentionally kept simple at this stage.

    Threshold-based detections are considered explicit detections
    and receive confidence 1.0.

    Z-score detections use the magnitude of the z-score as a
    bounded confidence signal.

    The returned value is always in [0.0, 1.0].
    """

    if score < 0:
        raise ValueError(
            "Anomaly score must be >= 0."
        )

    if detection_method == DetectionMethod.THRESHOLD:
        return 1.0

    if detection_method == DetectionMethod.Z_SCORE:
        return min(score / 10.0, 1.0)

    raise ValueError(
        f"Unsupported detection method: {detection_method!r}"
    )


# ---------------------------------------------------------------------------
# Incident type
# ---------------------------------------------------------------------------


def determine_incident_type(
    anomaly: AnomalyInput,
) -> IncidentType:
    """
    Determine the incident type.

    If Phase 2 already supplied an incident type, reuse it.

    Otherwise infer the type from the normalized signal.
    """

    if anomaly.incident_type is not None:
        return anomaly.incident_type

    signal_type = anomaly.signal_type.lower()

    if signal_type == "cpu":
        return IncidentType.CPU_SATURATION

    if signal_type in {
        "latency_50",
        "latency_90",
        "latency-50",
        "latency-90",
        "latency",
    }:
        return IncidentType.DB_LATENCY

    if signal_type in {
        "socket",
        "error",
        "network",
    }:
        return IncidentType.NETWORK_FAILURE

    raise ValueError(
        "Unable to determine incident type from signal type: "
        f"{anomaly.signal_type!r}"
    )


# ---------------------------------------------------------------------------
# Incident builder
# ---------------------------------------------------------------------------


def build_incident(
    anomaly: AnomalyInput,
) -> Incident:
    """
    Transform an AnomalyInput into a structured Incident.

    This function performs qualification only.

    It does not:
        - query TimescaleDB,
        - query Neo4j,
        - perform RCA,
        - determine root cause.
    """
    validate_operational_metadata(
        anomaly.metadata,
        context="Incident Builder input",
    )
    if not anomaly.resource_id:
        raise ValueError(
            "resource_id must not be empty."
        )

    if not anomaly.case_id:
        raise ValueError(
            "case_id must not be empty."
        )

    if not anomaly.metric_name:
        raise ValueError(
            "metric_name must not be empty."
        )

    if anomaly.value is None:
        raise ValueError(
            "Anomaly value must not be None."
        )

    if anomaly.score < 0:
        raise ValueError(
            "Anomaly score must be >= 0."
        )

    incident_type = determine_incident_type(
        anomaly
    )

    severity = determine_severity(
        anomaly.score
    )

    confidence = determine_confidence(
        anomaly.score,
        anomaly.detection_method,
    )

    incident_id = generate_incident_id(
        anomaly
    )

    return Incident(
        incident_id=incident_id,
        case_id=anomaly.case_id,

        incident_type=incident_type,

        status=IncidentStatus.DETECTED,

        severity=severity,

        resource_id=anomaly.resource_id,

        detected_at=anomaly.timestamp,

        signal_type=anomaly.signal_type,

        metric_name=anomaly.metric_name,

        observed_value=anomaly.value,

        anomaly_score=anomaly.score,

        detection_method=anomaly.detection_method,

        confidence=confidence,

        source=IncidentSource.OBSERVER,

        metadata=dict(anomaly.metadata),
    )


# ---------------------------------------------------------------------------
# Incident context
# ---------------------------------------------------------------------------


def build_incident_context(
    incident: Incident,
) -> IncidentContext:
    """
    Build the context that will be passed to the future
    Diagnostic/Impact Agent.
    """

    return IncidentContext(
        incident=incident,

        resource_id=incident.resource_id,

        detection_timestamp=incident.detected_at,

        signal_type=incident.signal_type,

        metric_name=incident.metric_name,

        observed_value=incident.observed_value,

        anomaly_score=incident.anomaly_score,

        temporal_context={},

        graph_context={},

        metadata={
            "source": incident.source.value,
        },
    )