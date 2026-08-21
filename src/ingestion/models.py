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

    fault:
        Original RCAEval fault label when available.

    root_cause_service:
        Ground-truth root-cause service when available.
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

    trace_id: Optional[str] = None   # Added for explicit trace correlation
    span_id: Optional[str] = None    # Added for explicit trace correlation
    
    attributes: Dict[str, Any] = field(default_factory=dict)

    dataset: Optional[str] = None
    fault: Optional[str] = None
    root_cause_service: Optional[str] = None


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