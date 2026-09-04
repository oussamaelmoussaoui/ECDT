"""
ECDT - Phase 2
Common data models for the ingestion pipeline.

This module defines the normalized event representation used to
standardize metrics, logs and traces before they are consumed by
the Digital Twin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

FORBIDDEN_GROUND_TRUTH_FIELDS = frozenset(
    {
        "expected_root_cause",
        "expected_service",
        "fault",
        "fault_description",
        "ground_truth",
        "inject_time",
        "injected_service",
        "injection_time",
        "root_cause_service",
        "time_end",
        "time_start",
    }
)


def find_ground_truth_fields(
    value: Any,
    path: str = "metadata",
) -> list[str]:
    """Return forbidden RCAEval-label paths found in nested metadata."""

    found: list[str] = []

    if isinstance(value, dict):
        for key, nested_value in value.items():
            field_path = f"{path}.{key}"

            if str(key).lower() in FORBIDDEN_GROUND_TRUTH_FIELDS:
                found.append(field_path)

            found.extend(
                find_ground_truth_fields(
                    nested_value,
                    field_path,
                )
            )

        return found

    if isinstance(value, (list, tuple, set, frozenset)):
        for index, nested_value in enumerate(value):
            found.extend(
                find_ground_truth_fields(
                    nested_value,
                    f"{path}[{index}]",
                )
            )

    return found


def validate_operational_metadata(
    metadata: Dict[str, Any],
    *,
    context: str,
) -> None:
    """Reject RCAEval ground truth at an operational boundary."""

    leaked_paths = find_ground_truth_fields(metadata)

    if leaked_paths:
        raise ValueError(
            f"{context} contains forbidden RCAEval ground truth: "
            + ", ".join(leaked_paths)
        )

class EventType(str, Enum):
    """Type of telemetry signal."""

    METRIC = "metric"
    LOG = "log"
    TRACE = "trace"


class SignalType(str, Enum):
    """Normalized signal categories."""

    CPU = "cpu"
    MEMORY = "memory"
    DISK_IO = "disk_io"
    SOCKET = "socket"
    WORKLOAD = "workload"
    ERROR = "error"
    LATENCY_50 = "latency_50"
    LATENCY_90 = "latency_90"
    TRACE_DURATION = "trace_duration"
    TRACE_STATUS = "trace_status"
    LOG_MESSAGE = "log_message"
    UNKNOWN = "unknown"


class IncidentType(str, Enum):
    """ECDT target incident categories."""

    CPU_SATURATION = "cpu_saturation"
    DB_LATENCY = "db_latency"
    NETWORK_FAILURE = "network_failure"


class FaultType(str, Enum):
    """RCAEval fault categories relevant to ECDT."""

    CPU = "cpu"
    DELAY = "delay"
    LOSS = "loss"
    SOCKET = "socket"


class DetectionMethod(str, Enum):
    """Methods supported by the Phase 2 anomaly detector."""

    THRESHOLD = "threshold"
    Z_SCORE = "z_score"


@dataclass(slots=True)
class NormalizedEvent:
    """
    Common representation of a telemetry event.

    Metrics, logs and traces are converted to this representation
    before being passed to downstream components.

    Attributes
    ----------
    event_id:
        Unique identifier of the normalized event.

    case_id:
        RCAEval/ECDT case identifier.

    timestamp:
        Event timestamp expressed as Unix epoch seconds.

    event_type:
        Original telemetry type: metric, log or trace.

    service:
        Service/container associated with the event.

    signal_type:
        Normalized signal category.

    metric_name:
        Original metric or signal name when available.

    value:
        Numeric value when the event represents a numerical signal.

    unit:
        Optional unit associated with the value.

    message:
        Log message when the event comes from logs.

    attributes:
        Additional source-specific information.

    dataset:
        Dataset/suite identifier such as RE1-OB, RE2-OB or RE3-OB.

    RCAEval fault labels and root-cause labels are deliberately excluded.
    They belong to the evaluation path, not to operational telemetry.
    """

    event_id: str
    case_id: str
    timestamp: int

    event_type: EventType
    service: Optional[str] = None
    signal_type: SignalType = SignalType.UNKNOWN

    metric_name: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None

    message: Optional[str] = None
    log_level: Optional[str] = None  # Added for explicit log severity filtering

    trace_id: Optional[str] = None
    span_id: Optional[str] = None

    attributes: Dict[str, Any] = field(default_factory=dict)

    dataset: Optional[str] = None


@dataclass(slots=True)
class AnomalyEvent:
    """
    Representation of an anomaly detected from normalized telemetry.

    This object is intentionally separate from NormalizedEvent:
    a telemetry event is not necessarily anomalous.
    """

    event_id: str
    case_id: str
    timestamp: int

    service: Optional[str]
    signal_type: SignalType

    value: float

    detection_method: DetectionMethod

    score: float
    threshold: Optional[float] = None

    is_anomaly: bool = True

    incident_type: Optional[IncidentType] = None

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DetectionResult:
    """
    Result of validating anomaly detection against a labelled case.
    """
    case_id: str
    fault: str
    incident_type: Optional[IncidentType]
    detected: bool

    first_detection_timestamp: Optional[int] = None
    detection_delay_seconds: Optional[float] = None
    anomaly_count: int = 0
    detection_method: Optional[DetectionMethod] = None
    root_cause_service: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)