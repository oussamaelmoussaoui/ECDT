"""
ECDT - Phase 2
Anomaly detector for normalized telemetry.

This module implements simple statistical anomaly detection
(threshold and z-score) on normalized metric time series.

Target ECDT incident types:

- CPU_SATURATION
    -> CPU signals

- DB_LATENCY
    -> LATENCY_50 / LATENCY_90 signals

- NETWORK_FAILURE
    -> SOCKET / ERROR signals

The detector follows this pipeline:

    NormalizedEvent
          |
          v
    metric time series
          |
          v
    normal baseline
          |
          v
    z-score / threshold
          |
          v
    AnomalyEvent
          |
          v
    validation against ground truth

Important:
The detector must never use the faulty period to build the baseline.
The baseline is constructed exclusively from observations before
the injection timestamp.
"""

from __future__ import annotations

import logging
import math
import statistics

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .models import (
    AnomalyEvent,
    DetectionMethod,
    DetectionResult,
    EventType,
    IncidentType,
    NormalizedEvent,
    SignalType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# INCIDENT / SIGNAL MAPPING
# ============================================================================

"""
Signals relevant to each ECDT incident type.

The Phase 1 dataset validation established the following organization:

    CPU
        -> cpu_saturation

    DELAY
        -> db_latency

    LOSS + SOCKET
        -> network_failure

For Phase 2, detection is based on telemetry signals rather than directly
on the RCAEval fault label.

Signal types are now plain strings matching the normalized schema:
    "cpu", "mem", "diskio", "socket", "workload",
    "error", "latency-50", "latency-90"
"""

INCIDENT_SIGNALS: Dict[IncidentType, set[str]] = {
    IncidentType.CPU_SATURATION: {
        "cpu",
    },

    IncidentType.DB_LATENCY: {
        "latency-50",
        "latency-90",
    },

    IncidentType.NETWORK_FAILURE: {
        "socket",
        "error",
    },
}


# Signals that can be processed by the detector.
#
# MEMORY / DISK_IO / WORKLOAD are intentionally supported here even though
# they are not primary signals for the three current target scenarios.
DETECTABLE_SIGNALS: set[str] = {
    "cpu",
    "latency-50",
    "latency-90",
    "socket",
    "error",
    "mem",
    "diskio",
    "workload",
}

# ============================================================================
# FAULT -> INCIDENT MAPPING
# ============================================================================

FAULT_TO_INCIDENT: Dict[str, IncidentType] = {
    "cpu": IncidentType.CPU_SATURATION,
    "delay": IncidentType.DB_LATENCY,
    "loss": IncidentType.NETWORK_FAILURE,
    "socket": IncidentType.NETWORK_FAILURE,
}

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class DetectorConfig:
    """
    Configuration of the statistical anomaly detector.

    Parameters
    ----------
    method:
        Detection method.

        - DetectionMethod.Z_SCORE
        - DetectionMethod.THRESHOLD

    z_threshold:
        Absolute z-score required to flag an anomaly.

        Default:
            3.0

    threshold_quantile:
        Quantile calculated on the normal baseline when using the threshold
        method.

        Default:
            0.95

    min_baseline_samples:
        Minimum number of valid observations required to construct a baseline.

        Default:
            10

    min_value:
        Optional absolute minimum value.

        Values below this limit are ignored by the detector.
    """

    method: DetectionMethod = DetectionMethod.Z_SCORE

    z_threshold: float = 3.0

    threshold_quantile: float = 0.95

    min_baseline_samples: int = 10

    min_value: Optional[float] = None


# ============================================================================
# TIME SERIES REPRESENTATION
# ============================================================================

@dataclass
class TimeSeries:
    """
    A single metric time series.

    One TimeSeries corresponds to:

        case_id + service_name + signal_type

    Example:

        re2ob_checkoutservice_cpu_1
        checkoutservice
        cpu
    """

    case_id: str

    service_name: str

    signal_type: str

    timestamps: List[float] = field(default_factory=list)

    values: List[float] = field(default_factory=list)

    fault: Optional[str] = None

    incident_type: Optional[IncidentType] = None

    root_cause_service: Optional[str] = None

    def __len__(self) -> int:
        """Return the number of observations."""
        return len(self.values)

    def sorted(self) -> "TimeSeries":
        """
        Return a copy of the series sorted by timestamp.
        """

        order = sorted(
            range(len(self.timestamps)),
            key=lambda i: self.timestamps[i],
        )

        return TimeSeries(
            case_id=self.case_id,
            service_name=self.service_name,
            signal_type=self.signal_type,
            timestamps=[
                self.timestamps[i]
                for i in order
            ],
            values=[
                self.values[i]
                for i in order
            ],
            fault=self.fault,
            incident_type=self.incident_type,
            root_cause_service=self.root_cause_service,
        )


# ============================================================================
# ANOMALY DETECTOR
# ============================================================================

class AnomalyDetector:
    """
    Statistical anomaly detector for normalized telemetry.

    The detector works in two stages.

    Stage 1:
        Build a baseline using only the normal period.

    Stage 2:
        Compare observations against the baseline.

    Supported methods:

    1. Z-score

        z = (x - mean) / std

        An anomaly is generated when:

            abs(z) >= z_threshold

    2. Threshold

        threshold = baseline quantile

        An anomaly is generated when:

            value > threshold
    """

    def __init__(
        self,
        config: Optional[DetectorConfig] = None,
    ):
        self.config = config or DetectorConfig()

        self._validate_config()

    # ----------------------------------------------------------------------
    # Configuration validation
    # ----------------------------------------------------------------------

    def _validate_config(self) -> None:
        """Validate detector configuration."""

        if self.config.z_threshold <= 0:
            raise ValueError(
                "z_threshold must be strictly greater than 0."
            )

        if not 0 < self.config.threshold_quantile <= 1:
            raise ValueError(
                "threshold_quantile must be in the interval (0, 1]."
            )

        if self.config.min_baseline_samples < 2:
            raise ValueError(
                "min_baseline_samples must be at least 2."
            )

    # ----------------------------------------------------------------------
    # Value cleaning
    # ----------------------------------------------------------------------

    @staticmethod
    def _clean_values(
        values: Sequence[Optional[float]],
    ) -> List[float]:
        """
        Remove invalid numerical values.

        Filters:

        - None
        - NaN
        - infinity
        - negative infinity
        - non-convertible values
        """

        cleaned: List[float] = []

        for value in values:

            if value is None:
                continue

            try:
                numeric_value = float(value)

            except (TypeError, ValueError):
                continue

            if math.isnan(numeric_value):
                continue

            if math.isinf(numeric_value):
                continue

            cleaned.append(numeric_value)

        return cleaned

    # ----------------------------------------------------------------------
    # Baseline statistics
    # ----------------------------------------------------------------------

    @staticmethod
    def compute_baseline(
        values: Sequence[float],
    ) -> Tuple[float, float, float]:
        """
        Compute baseline statistics.

        Returns
        -------
        tuple
            mean, standard deviation, maximum value
        """

        if not values:
            return 0.0, 0.0, 0.0

        mean = statistics.fmean(values)

        if len(values) > 1:
            std = statistics.pstdev(values)
        else:
            std = 0.0

        maximum = max(values)

        return mean, std, maximum

    # ----------------------------------------------------------------------
    # Quantile
    # ----------------------------------------------------------------------

    @staticmethod
    def _quantile(
        values: Sequence[float],
        q: float,
    ) -> float:
        """
        Compute a quantile using linear interpolation.

        This avoids requiring NumPy for the detector.
        """

        if not values:
            return 0.0

        if not 0 <= q <= 1:
            raise ValueError(
                "q must be between 0 and 1."
            )

        sorted_values = sorted(values)

        n = len(sorted_values)

        if n == 1:
            return sorted_values[0]

        position = (n - 1) * q

        lower = int(math.floor(position))
        upper = int(math.ceil(position))

        if lower == upper:
            return sorted_values[lower]

        fraction = position - lower

        return (
            sorted_values[lower] * (1 - fraction)
            + sorted_values[upper] * fraction
        )

    # ----------------------------------------------------------------------
    # Z-score
    # ----------------------------------------------------------------------

    @staticmethod
    def zscore(
        value: float,
        mean: float,
        std: float,
    ) -> float:
        """
        Compute a z-score.

        When the baseline standard deviation is zero, return 0.

        This prevents division by zero for perfectly constant baselines.
        """

        if std == 0.0:
            return 0.0

        return (value - mean) / std

    # =========================================================================
    # SERIES EXTRACTION
    # =========================================================================

    def build_series(
        self,
        events: Iterable[dict],
        restrict_to_incident: Optional[IncidentType] = None,
    ) -> Dict[
        Tuple[str, str],
        TimeSeries,
    ]:
        """
        Build metric time series from normalized event dicts.

        Expected dict keys (from SchemaNormalizer + to_dicts()):

            event_id, case_id, timestamp_ms, dataset, fault,
            root_cause_service, source, service_name, signal_type,
            metric_name, value, message, trace_id, span_id,
            parent_span_id, method_name, operation_name,
            duration_ms, status_code

        Only metric events are considered.

        Parameters
        ----------
        events:
            Iterable of dicts produced by DataFrame.to_dicts().

        restrict_to_incident:
            Optional incident type.

            When provided, only signals associated with that incident
            type are retained.

        Returns
        -------
        dict
            Mapping:

                (service_name, signal_type)
                    ->
                TimeSeries
        """

        allowed_signals: Optional[set[str]] = None

        if restrict_to_incident is not None:

            allowed_signals = INCIDENT_SIGNALS.get(
                restrict_to_incident
            )

        series_map: Dict[
            Tuple[str, str],
            TimeSeries,
        ] = defaultdict(
            lambda: TimeSeries(
                case_id="",
                service_name="",
                signal_type="unknown",
            )
        )

        for event in events:

            # --------------------------------------------------------------
            # Only metrics
            # --------------------------------------------------------------

            if event.get("source") != "metric":
                continue

            # --------------------------------------------------------------
            # Numeric values only
            # --------------------------------------------------------------

            value = event.get("value")

            if value is None:
                continue

            try:
                value = float(value)

            except (TypeError, ValueError):
                continue

            if math.isnan(value) or math.isinf(value):
                continue

            # --------------------------------------------------------------
            # Detectable signal
            # --------------------------------------------------------------

            signal_type = event.get("signal_type")

            if signal_type is None or signal_type not in DETECTABLE_SIGNALS:
                continue

            # --------------------------------------------------------------
            # Incident-specific filtering
            # --------------------------------------------------------------

            if (
                allowed_signals is not None
                and signal_type not in allowed_signals
            ):
                continue

            # --------------------------------------------------------------
            # Service
            # --------------------------------------------------------------

            service_name = event.get("service_name") or "unknown"

            key = (
                service_name,
                signal_type,
            )

            series = series_map[key]

            # Initialize metadata on first observation
            if not series.case_id:

                series.case_id = event.get("case_id", "")

                series.service_name = service_name

                series.signal_type = signal_type

                series.fault = event.get("fault")

                series.root_cause_service = event.get(
                    "root_cause_service"
                )

                # Map fault to incident type if available
                fault = event.get("fault")
                if fault:
                    series.incident_type = FAULT_TO_INCIDENT.get(
                        fault
                    )

            series.timestamps.append(
                float(event.get("timestamp_ms", 0))
            )

            series.values.append(
                value
            )

        # Sort all series chronologically
        return {
            key: series.sorted()
            for key, series in series_map.items()
        }

    # =========================================================================
    # DETECTION ON ONE SERIES
    # =========================================================================

    def detect_series(
        self,
        ts: TimeSeries,
        baseline_end: Optional[float] = None,
        incident_type: Optional[IncidentType] = None,
    ) -> List[AnomalyEvent]:
        """
        Detect anomalies on one time series.

        Parameters
        ----------
        ts:
            Time series.

        baseline_end:
            End of the normal baseline.

            All observations:

                timestamp < baseline_end

            are used to construct the baseline.

            Observations at or after baseline_end are evaluated.

        incident_type:
            Incident type attached to generated anomaly events.

        Returns
        -------
        list[AnomalyEvent]
        """

        # ------------------------------------------------------------------
        # Basic validation
        # ------------------------------------------------------------------

        if len(ts) < self.config.min_baseline_samples:

            logger.debug(
                "Series %s/%s too short: %d observations.",
                ts.service_name,
                ts.signal_type,
                len(ts),
            )

            return []

        # ------------------------------------------------------------------
        # Baseline
        # ------------------------------------------------------------------

        if baseline_end is not None:

            baseline_values = [
                value
                for timestamp, value in zip(
                    ts.timestamps,
                    ts.values,
                )
                if timestamp < baseline_end
            ]

        else:

            # Fallback only.
            #
            # In the ECDT pipeline, baseline_end should normally be the
            # injection timestamp.
            split_index = len(ts) // 2

            baseline_values = ts.values[:split_index]

        baseline_values = self._clean_values(
            baseline_values
        )

        if len(baseline_values) < self.config.min_baseline_samples:

            logger.debug(
                "Series %s/%s has only %d baseline observations.",
                ts.service_name,
                ts.signal_type,
                len(baseline_values),
            )

            return []

        # ------------------------------------------------------------------
        # Baseline statistics
        # ------------------------------------------------------------------

        mean, std, max_value = self.compute_baseline(
            baseline_values
        )

        quantile_threshold = self._quantile(
            baseline_values,
            self.config.threshold_quantile,
        )

        # ------------------------------------------------------------------
        # Observation
        # ------------------------------------------------------------------

        anomalies: List[AnomalyEvent] = []

        for timestamp, value in zip(
            ts.timestamps,
            ts.values,
        ):

            # Never evaluate the baseline itself.
            if (
                baseline_end is not None
                and timestamp < baseline_end
            ):
                continue

            if value is None:
                continue

            try:
                value = float(value)

            except (TypeError, ValueError):
                continue

            if math.isnan(value) or math.isinf(value):
                continue

            # Optional minimum absolute value.
            if (
                self.config.min_value is not None
                and value < self.config.min_value
            ):
                continue

            # --------------------------------------------------------------
            # Detection
            # --------------------------------------------------------------

            score = 0.0

            threshold_value: Optional[float] = None

            is_anomaly = False

            if self.config.method == DetectionMethod.Z_SCORE:

                score = self.zscore(
                    value,
                    mean,
                    std,
                )

                is_anomaly = (
                    abs(score)
                    >= self.config.z_threshold
                )

                threshold_value = (
                    self.config.z_threshold
                )

            elif self.config.method == DetectionMethod.THRESHOLD:

                score = value

                threshold_value = (
                    quantile_threshold
                )

                is_anomaly = (
                    value > quantile_threshold
                )

            else:

                raise ValueError(
                    f"Unsupported detection method: "
                    f"{self.config.method}"
                )

            # --------------------------------------------------------------
            # Build anomaly event
            # --------------------------------------------------------------

            if not is_anomaly:
                continue

            event_id = (
                f"{ts.case_id}|"
                f"{ts.service_name}|"
                f"{ts.signal_type}|"
                f"{timestamp}|"
                f"{self.config.method.value}"
            )

            anomaly = AnomalyEvent(
                event_id=event_id,

                case_id=ts.case_id,

                timestamp=timestamp,

                service=ts.service_name,

                signal_type=ts.signal_type,

                value=value,

                detection_method=self.config.method,

                score=score,

                threshold=threshold_value,

                is_anomaly=True,

                incident_type=(
                    incident_type
                    or ts.incident_type
                ),

                metadata={
                    "baseline_mean": mean,
                    "baseline_std": std,
                    "baseline_max": max_value,
                    "baseline_quantile": (
                        quantile_threshold
                    ),
                    "baseline_samples": len(
                        baseline_values
                    ),
                    "root_cause_service": (
                        ts.root_cause_service
                    ),
                },
            )

            anomalies.append(anomaly)

        return anomalies

    # =========================================================================
    # DETECTION ON EVENTS
    # =========================================================================

    def detect_in_events(
        self,
        events: Iterable[NormalizedEvent],
        baseline_end: Optional[float] = None,
        incident_type: Optional[IncidentType] = None,
    ) -> List[AnomalyEvent]:
        """
        Detect anomalies across normalized events.

        Parameters
        ----------
        events:
            Normalized telemetry events.

        baseline_end:
            Timestamp defining the end of the normal baseline.

            For ECDT this should normally be:

                inject_time * 1000

            when normalized events use milliseconds.

        incident_type:
            Optional incident type restriction.

        Returns
        -------
        list[AnomalyEvent]
        """

        series_map = self.build_series(
            events,
            restrict_to_incident=incident_type,
        )

        logger.info(
            "Running anomaly detection on %d series "
            "(method=%s).",
            len(series_map),
            self.config.method.value,
        )

        all_anomalies: List[AnomalyEvent] = []

        for series in series_map.values():

            detected = self.detect_series(
                series,
                baseline_end=baseline_end,
                incident_type=incident_type,
            )

            all_anomalies.extend(
                detected
            )

        # Chronological ordering
        all_anomalies.sort(
            key=lambda anomaly: anomaly.timestamp
        )

        logger.info(
            "Detected %d anomalies.",
            len(all_anomalies),
        )

        return all_anomalies


# ============================================================================
# SIMPLE GROUND-TRUTH VALIDATION
# ============================================================================

def validate_detection(
    case_id: str,
    fault: str,
    incident_type: Optional[IncidentType],
    anomalies: Sequence[AnomalyEvent],
    inject_time: Optional[float] = None,
    root_cause_service: Optional[str] = None,
    detection_window: Optional[float] = None,
) -> DetectionResult:
    """
    Validate anomaly detection against ground truth.

    A detection is considered valid when:

        anomaly.timestamp >= inject_time

    and, when detection_window is provided:

        anomaly.timestamp <= inject_time + detection_window
    """

    # ----------------------------------------------------------------------
    # Filter valid detections
    # ----------------------------------------------------------------------

    if inject_time is not None:

        valid_anomalies = [
            anomaly
            for anomaly in anomalies
            if anomaly.timestamp >= inject_time
        ]

        if detection_window is not None:

            valid_anomalies = [
                anomaly
                for anomaly in valid_anomalies
                if anomaly.timestamp
                <= inject_time + detection_window
            ]

    else:

        valid_anomalies = list(anomalies)

    # ----------------------------------------------------------------------
    # Detection status
    # ----------------------------------------------------------------------

    detected = len(valid_anomalies) > 0

    first_detection_timestamp: Optional[float] = None

    detection_delay: Optional[float] = None

    if detected:

        first_detection_timestamp = min(
            anomaly.timestamp
            for anomaly in valid_anomalies
        )

        if inject_time is not None:

            detection_delay = (
                first_detection_timestamp
                - inject_time
            )

    # ----------------------------------------------------------------------
    # Result
    # ----------------------------------------------------------------------

    return DetectionResult(
        case_id=case_id,

        fault=fault,

        incident_type=incident_type,

        detected=detected,

        first_detection_timestamp=(
            first_detection_timestamp
        ),

        detection_delay_seconds=detection_delay,

        anomaly_count=len(valid_anomalies),

        detection_method=(
            valid_anomalies[0].detection_method
            if valid_anomalies
            else None
        ),

        root_cause_service=root_cause_service,

        metadata={
            "total_anomalies": len(anomalies),
            "valid_anomalies": len(valid_anomalies),
        },
    )


# ============================================================================
# SIMPLE VALIDATION SUMMARY
# ============================================================================

def summarize_results(
    results: Sequence[DetectionResult],
) -> dict:
    """
    Aggregate DetectionResult objects.

    Returns
    -------
    dict
        Global and per-incident detection statistics.
    """

    total = len(results)

    detected_count = sum(
        1
        for result in results
        if result.detected
    )

    detection_rate = (
        detected_count / total
        if total > 0
        else 0.0
    )

    delays = [
        result.detection_delay_seconds
        for result in results
        if (
            result.detected
            and result.detection_delay_seconds is not None
        )
    ]

    average_delay = (
        statistics.fmean(delays)
        if delays
        else None
    )

    per_incident: Dict[str, dict] = {}

    for result in results:

        key = (
            result.incident_type.value
            if result.incident_type
            else "unknown"
        )

        if key not in per_incident:

            per_incident[key] = {
                "total": 0,
                "detected": 0,
            }

        per_incident[key]["total"] += 1

        if result.detected:

            per_incident[key]["detected"] += 1

    for stats in per_incident.values():

        total_incidents = stats["total"]

        stats["detection_rate"] = (
            stats["detected"]
            / total_incidents
            if total_incidents > 0
            else 0.0
        )

    return {
        "total_cases": total,

        "detected_cases": detected_count,

        "detection_rate": detection_rate,

        "avg_detection_delay_seconds": (
            average_delay
        ),

        "per_incident_type": per_incident,
    }


# ============================================================================
# FACTORY
# ============================================================================

def create_detector(
    method: DetectionMethod = DetectionMethod.Z_SCORE,
    z_threshold: float = 3.0,
    threshold_quantile: float = 0.95,
    min_baseline_samples: int = 10,
    min_value: Optional[float] = None,
) -> AnomalyDetector:
    """
    Factory function for the anomaly detector.
    """

    config = DetectorConfig(
        method=method,

        z_threshold=z_threshold,

        threshold_quantile=threshold_quantile,

        min_baseline_samples=min_baseline_samples,

        min_value=min_value,
    )

    return AnomalyDetector(config)


# ============================================================================
# DETAILED CASE METRICS
# ============================================================================

@dataclass
class CaseMetrics:
    """
    Detailed anomaly detection metrics for one case.
    """

    case_id: str

    fault: str

    incident_type: Optional[IncidentType]

    detected: bool

    detection_delay_seconds: Optional[float] = None

    true_positives: int = 0

    false_positives: int = 0

    total_faulty_points: int = 0

    total_normal_points: int = 0

    recall: float = 0.0

    precision: float = 0.0

    f1_score: float = 0.0

    root_cause_service: Optional[str] = None

    detected_services: set = field(
        default_factory=set
    )

    root_cause_detected: bool = False

    first_detection_timestamp: Optional[float] = None

    detection_method: Optional[DetectionMethod] = None

    anomaly_count: int = 0

    metadata: Dict = field(
        default_factory=dict
    )


@dataclass
class AggregateMetrics:
    """
    Aggregated metrics across multiple evaluated cases.
    """

    total_cases: int = 0

    detected_cases: int = 0

    detection_rate: float = 0.0

    avg_recall: float = 0.0

    avg_precision: float = 0.0

    avg_f1_score: float = 0.0

    total_true_positives: int = 0

    total_false_positives: int = 0

    global_precision: float = 0.0

    global_recall: float = 0.0

    global_f1_score: float = 0.0

    avg_detection_delay_seconds: Optional[float] = None

    median_detection_delay_seconds: Optional[float] = None

    max_detection_delay_seconds: Optional[float] = None

    root_cause_detection_rate: float = 0.0

    per_incident_type: Dict[str, dict] = field(
        default_factory=dict
    )

    per_fault: Dict[str, dict] = field(
        default_factory=dict
    )


# ============================================================================
# CASE EVALUATION
# ============================================================================

def evaluate_case(
    case_id: str,
    fault: str,
    incident_type: Optional[IncidentType],
    anomalies: Sequence[AnomalyEvent],
    inject_time: float,
    time_start: float,
    time_end: float,
    root_cause_service: Optional[str] = None,
    total_data_points: Optional[int] = None,
) -> CaseMetrics:
    """
    Compute detailed metrics for one case.

    Time units must be consistent.

    For example:

        inject_time
        time_start
        time_end
        anomaly.timestamp

    must all be expressed in milliseconds if the normalized events use
    milliseconds.
    """

    # ----------------------------------------------------------------------
    # True positives
    # ----------------------------------------------------------------------

    true_positives = [
        anomaly
        for anomaly in anomalies
        if (
            anomaly.timestamp >= inject_time
            and anomaly.timestamp <= time_end
        )
    ]

    # ----------------------------------------------------------------------
    # False positives
    # ----------------------------------------------------------------------

    false_positives = [
        anomaly
        for anomaly in anomalies
        if (
            anomaly.timestamp >= time_start
            and anomaly.timestamp < inject_time
        )
    ]

    tp_count = len(true_positives)

    fp_count = len(false_positives)

    # ----------------------------------------------------------------------
    # Detection
    # ----------------------------------------------------------------------

    detected = tp_count > 0

    first_detection_timestamp: Optional[float] = None

    detection_delay: Optional[float] = None

    if detected:

        first_detection_timestamp = min(
            anomaly.timestamp
            for anomaly in true_positives
        )

        detection_delay = (
            first_detection_timestamp
            - inject_time
        )

    # ----------------------------------------------------------------------
    # Precision
    # ----------------------------------------------------------------------

    precision = (
        tp_count / (tp_count + fp_count)
        if (tp_count + fp_count) > 0
        else 0.0
    )

    # ----------------------------------------------------------------------
    # Recall
    # ----------------------------------------------------------------------

    faulty_duration = max(
        0.0,
        time_end - inject_time,
    )

    normal_duration = max(
        0.0,
        inject_time - time_start,
    )

    if (
        total_data_points is not None
        and total_data_points > 0
    ):

        total_duration = (
            time_end - time_start
        )

        if total_duration > 0:

            faulty_points_estimate = max(
                1,
                int(
                    total_data_points
                    * (
                        faulty_duration
                        / total_duration
                    )
                ),
            )

            normal_points_estimate = max(
                1,
                int(
                    total_data_points
                    * (
                        normal_duration
                        / total_duration
                    )
                ),
            )

        else:

            faulty_points_estimate = 0

            normal_points_estimate = 0

        recall = (
            tp_count / faulty_points_estimate
            if faulty_points_estimate > 0
            else 0.0
        )

        total_faulty_points = (
            faulty_points_estimate
        )

        total_normal_points = (
            normal_points_estimate
        )

    else:

        # When no number of points is provided,
        # detection is treated as a binary event.
        recall = (
            1.0
            if detected
            else 0.0
        )

        total_faulty_points = (
            int(faulty_duration)
        )

        total_normal_points = (
            int(normal_duration)
        )

    # ----------------------------------------------------------------------
    # F1
    # ----------------------------------------------------------------------

    if precision + recall > 0:

        f1 = (
            2
            * precision
            * recall
            / (precision + recall)
        )

    else:

        f1 = 0.0

    # ----------------------------------------------------------------------
    # Detected services
    # ----------------------------------------------------------------------

    detected_services = {
        anomaly.service
        for anomaly in anomalies
        if anomaly.service
    }

    # ----------------------------------------------------------------------
    # Root cause detection
    # ----------------------------------------------------------------------

    root_cause_detected = False

    if root_cause_service:

        root_cause_detected = (
            root_cause_service
            in detected_services
        )

    # ----------------------------------------------------------------------
    # Detection method
    # ----------------------------------------------------------------------

    detection_method = None

    if true_positives:

        detection_method = (
            true_positives[0].detection_method
        )

    # ----------------------------------------------------------------------
    # Result
    # ----------------------------------------------------------------------

    return CaseMetrics(
        case_id=case_id,

        fault=fault,

        incident_type=incident_type,

        detected=detected,

        detection_delay_seconds=detection_delay,

        true_positives=tp_count,

        false_positives=fp_count,

        total_faulty_points=total_faulty_points,

        total_normal_points=total_normal_points,

        recall=recall,

        precision=precision,

        f1_score=f1,

        root_cause_service=root_cause_service,

        detected_services=detected_services,

        root_cause_detected=root_cause_detected,

        first_detection_timestamp=(
            first_detection_timestamp
        ),

        detection_method=detection_method,

        anomaly_count=len(anomalies),

        metadata={
            "inject_time": inject_time,
            "time_start": time_start,
            "time_end": time_end,
            "faulty_duration": faulty_duration,
            "normal_duration": normal_duration,
        },
    )


# ============================================================================
# AGGREGATE METRICS
# ============================================================================

def aggregate_metrics(
    case_metrics: Sequence[CaseMetrics],
) -> AggregateMetrics:
    """
    Aggregate detailed case metrics into global metrics.
    """

    total = len(case_metrics)

    if total == 0:
        return AggregateMetrics()

    # ----------------------------------------------------------------------
    # Detection
    # ----------------------------------------------------------------------

    detected_count = sum(
        1
        for metrics in case_metrics
        if metrics.detected
    )

    detection_rate = (
        detected_count / total
    )

    # ----------------------------------------------------------------------
    # Average per-case metrics
    # ----------------------------------------------------------------------

    avg_recall = statistics.fmean(
        metrics.recall
        for metrics in case_metrics
    )

    avg_precision = statistics.fmean(
        metrics.precision
        for metrics in case_metrics
    )

    avg_f1 = statistics.fmean(
        metrics.f1_score
        for metrics in case_metrics
    )

    # ----------------------------------------------------------------------
    # Global classification metrics
    # ----------------------------------------------------------------------

    total_tp = sum(
        metrics.true_positives
        for metrics in case_metrics
    )

    total_fp = sum(
        metrics.false_positives
        for metrics in case_metrics
    )

    total_faulty_points = sum(
        metrics.total_faulty_points
        for metrics in case_metrics
    )

    global_precision = (
        total_tp / (total_tp + total_fp)
        if total_tp + total_fp > 0
        else 0.0
    )

    global_recall = (
        total_tp / total_faulty_points
        if total_faulty_points > 0
        else 0.0
    )

    global_f1 = (
        2
        * global_precision
        * global_recall
        / (
            global_precision
            + global_recall
        )
        if (
            global_precision
            + global_recall
        ) > 0
        else 0.0
    )

    # ----------------------------------------------------------------------
    # Detection delays
    # ----------------------------------------------------------------------

    delays = [
        metrics.detection_delay_seconds
        for metrics in case_metrics
        if (
            metrics.detected
            and metrics.detection_delay_seconds
            is not None
        )
    ]

    if delays:

        avg_delay = statistics.fmean(
            delays
        )

        median_delay = statistics.median(
            delays
        )

        max_delay = max(delays)

    else:

        avg_delay = None

        median_delay = None

        max_delay = None

    # ----------------------------------------------------------------------
    # Root cause detection
    # ----------------------------------------------------------------------

    root_cause_count = sum(
        1
        for metrics in case_metrics
        if metrics.root_cause_detected
    )

    root_cause_rate = (
        root_cause_count / total
        if total > 0
        else 0.0
    )

    # ----------------------------------------------------------------------
    # Per incident type
    # ----------------------------------------------------------------------

    per_incident: Dict[str, dict] = {}

    for metrics in case_metrics:

        key = (
            metrics.incident_type.value
            if metrics.incident_type
            else "unknown"
        )

        if key not in per_incident:

            per_incident[key] = {
                "total": 0,
                "detected": 0,
                "false_positives": 0,
                "avg_delay": [],
                "avg_recall": [],
                "avg_precision": [],
                "avg_f1": [],
            }

        stats = per_incident[key]

        stats["total"] += 1

        if metrics.detected:

            stats["detected"] += 1

            if (
                metrics.detection_delay_seconds
                is not None
            ):

                stats["avg_delay"].append(
                    metrics.detection_delay_seconds
                )

        stats["false_positives"] += (
            metrics.false_positives
        )

        stats["avg_recall"].append(
            metrics.recall
        )

        stats["avg_precision"].append(
            metrics.precision
        )

        stats["avg_f1"].append(
            metrics.f1_score
        )

    for stats in per_incident.values():

        stats["detection_rate"] = (
            stats["detected"]
            / stats["total"]
            if stats["total"] > 0
            else 0.0
        )

        stats["avg_delay_seconds"] = (
            statistics.fmean(
                stats["avg_delay"]
            )
            if stats["avg_delay"]
            else None
        )

        stats["avg_recall"] = (
            statistics.fmean(
                stats["avg_recall"]
            )
            if stats["avg_recall"]
            else 0.0
        )

        stats["avg_precision"] = (
            statistics.fmean(
                stats["avg_precision"]
            )
            if stats["avg_precision"]
            else 0.0
        )

        stats["avg_f1"] = (
            statistics.fmean(
                stats["avg_f1"]
            )
            if stats["avg_f1"]
            else 0.0
        )

        del stats["avg_delay"]

    # ----------------------------------------------------------------------
    # Per fault
    # ----------------------------------------------------------------------

    per_fault: Dict[str, dict] = {}

    for metrics in case_metrics:

        key = metrics.fault

        if key not in per_fault:

            per_fault[key] = {
                "total": 0,
                "detected": 0,
                "false_positives": 0,
                "avg_delay": [],
            }

        stats = per_fault[key]

        stats["total"] += 1

        if metrics.detected:

            stats["detected"] += 1

            if (
                metrics.detection_delay_seconds
                is not None
            ):

                stats["avg_delay"].append(
                    metrics.detection_delay_seconds
                )

        stats["false_positives"] += (
            metrics.false_positives
        )

    for stats in per_fault.values():

        stats["detection_rate"] = (
            stats["detected"]
            / stats["total"]
            if stats["total"] > 0
            else 0.0
        )

        stats["avg_delay_seconds"] = (
            statistics.fmean(
                stats["avg_delay"]
            )
            if stats["avg_delay"]
            else None
        )

        del stats["avg_delay"]

    return AggregateMetrics(
        total_cases=total,

        detected_cases=detected_count,

        detection_rate=detection_rate,

        avg_recall=avg_recall,

        avg_precision=avg_precision,

        avg_f1_score=avg_f1,

        total_true_positives=total_tp,

        total_false_positives=total_fp,

        global_precision=global_precision,

        global_recall=global_recall,

        global_f1_score=global_f1,

        avg_detection_delay_seconds=avg_delay,

        median_detection_delay_seconds=median_delay,

        max_detection_delay_seconds=max_delay,

        root_cause_detection_rate=root_cause_rate,

        per_incident_type=per_incident,

        per_fault=per_fault,
    )


# ============================================================================
# HUMAN-READABLE REPORT
# ============================================================================

def format_detection_report(
    case_metrics: Sequence[CaseMetrics],
    agg: AggregateMetrics,
) -> str:
    """
    Generate a human-readable anomaly detection report.
    """

    lines: List[str] = []

    lines.append("=" * 80)

    lines.append(
        "PHASE 2 - ANOMALY DETECTION REPORT"
    )

    lines.append("=" * 80)

    lines.append("")

    # ----------------------------------------------------------------------
    # Global
    # ----------------------------------------------------------------------

    lines.append("GLOBAL SUMMARY")

    lines.append("-" * 80)

    lines.append(
        f"  Total cases:              "
        f"{agg.total_cases}"
    )

    lines.append(
        f"  Detected cases:           "
        f"{agg.detected_cases}"
    )

    lines.append(
        f"  Detection rate:           "
        f"{agg.detection_rate:.2%}"
    )

    lines.append(
        f"  Root cause detection:     "
        f"{agg.root_cause_detection_rate:.2%}"
    )

    lines.append("")

    # ----------------------------------------------------------------------
    # Classification
    # ----------------------------------------------------------------------

    lines.append(
        "CLASSIFICATION METRICS"
    )

    lines.append("-" * 80)

    lines.append(
        f"  Total true positives:     "
        f"{agg.total_true_positives}"
    )

    lines.append(
        f"  Total false positives:    "
        f"{agg.total_false_positives}"
    )

    lines.append(
        f"  Global precision:         "
        f"{agg.global_precision:.4f}"
    )

    lines.append(
        f"  Global recall:            "
        f"{agg.global_recall:.4f}"
    )

    lines.append(
        f"  Global F1 score:          "
        f"{agg.global_f1_score:.4f}"
    )

    lines.append(
        f"  Avg recall (per case):    "
        f"{agg.avg_recall:.4f}"
    )

    lines.append(
        f"  Avg precision (per case): "
        f"{agg.avg_precision:.4f}"
    )

    lines.append(
        f"  Avg F1 (per case):        "
        f"{agg.avg_f1_score:.4f}"
    )

    lines.append("")

    # ----------------------------------------------------------------------
    # Detection delay
    # ----------------------------------------------------------------------

    lines.append(
        "DETECTION DELAYS"
    )

    lines.append("-" * 80)

    if agg.avg_detection_delay_seconds is not None:

        lines.append(
            f"  Average delay:            "
            f"{agg.avg_detection_delay_seconds:.2f}s"
        )

        lines.append(
            f"  Median delay:             "
            f"{agg.median_detection_delay_seconds:.2f}s"
        )

        lines.append(
            f"  Max delay:                "
            f"{agg.max_detection_delay_seconds:.2f}s"
        )

    else:

        lines.append(
            "  No detections recorded"
        )

    lines.append("")

    # ----------------------------------------------------------------------
    # Incident type
    # ----------------------------------------------------------------------

    lines.append(
        "PER INCIDENT TYPE"
    )

    lines.append("-" * 80)

    lines.append(
        f"  {'Type':<22}"
        f"{'Total':>7}"
        f"{'Detected':>10}"
        f"{'Rate':>9}"
        f"{'FP':>7}"
        f"{'Avg Delay':>12}"
        f"{'Avg Recall':>12}"
    )

    lines.append(
        "  "
        + "-" * 22
        + " "
        + "-" * 7
        + " "
        + "-" * 10
        + " "
        + "-" * 9
        + " "
        + "-" * 7
        + " "
        + "-" * 12
        + " "
        + "-" * 12
    )

    for incident_type, stats in sorted(
        agg.per_incident_type.items()
    ):

        avg_delay = stats[
            "avg_delay_seconds"
        ]

        delay_string = (
            f"{avg_delay:.1f}s"
            if avg_delay is not None
            else "N/A"
        )

        lines.append(
            f"  {incident_type:<22}"
            f"{stats['total']:>7}"
            f"{stats['detected']:>10}"
            f"{stats['detection_rate']:>8.1%}"
            f"{stats['false_positives']:>7}"
            f"{delay_string:>12}"
            f"{stats['avg_recall']:>12.4f}"
        )

    lines.append("")

    # ----------------------------------------------------------------------
    # Per fault
    # ----------------------------------------------------------------------

    lines.append(
        "PER FAULT"
    )

    lines.append("-" * 80)

    for fault, stats in sorted(
        agg.per_fault.items()
    ):

        avg_delay = stats[
            "avg_delay_seconds"
        ]

        delay_string = (
            f"{avg_delay:.2f}s"
            if avg_delay is not None
            else "N/A"
        )

        lines.append(
            f"  {fault:<12}"
            f"total={stats['total']:<5}"
            f"detected={stats['detected']:<5}"
            f"rate={stats['detection_rate']:.2%}"
            f"fp={stats['false_positives']:<5}"
            f"avg_delay={delay_string}"
        )

    lines.append("")

    # ----------------------------------------------------------------------
    # Per case
    # ----------------------------------------------------------------------

    lines.append(
        "PER CASE DETAIL"
    )

    lines.append("-" * 80)

    lines.append(
        f"  {'Case ID':<42}"
        f"{'Fault':<8}"
        f"{'Det':>5}"
        f"{'Delay':>10}"
        f"{'TP':>6}"
        f"{'FP':>6}"
        f"{'RootCause':>12}"
    )

    lines.append(
        "  "
        + "-" * 42
        + " "
        + "-" * 8
        + " "
        + "-" * 5
        + " "
        + "-" * 10
        + " "
        + "-" * 6
        + " "
        + "-" * 6
        + " "
        + "-" * 12
    )

    for metrics in sorted(
        case_metrics,
        key=lambda item: item.case_id,
    ):

        if (
            metrics.detection_delay_seconds
            is not None
        ):

            delay_string = (
                f"{metrics.detection_delay_seconds:.1f}s"
            )

        else:

            delay_string = "N/A"

        detected_string = (
            "YES"
            if metrics.detected
            else "NO"
        )

        root_cause_string = (
            "YES"
            if metrics.root_cause_detected
            else "NO"
        )

        lines.append(
            f"  {metrics.case_id:<42}"
            f"{metrics.fault:<8}"
            f"{detected_string:>5}"
            f"{delay_string:>10}"
            f"{metrics.true_positives:>6}"
            f"{metrics.false_positives:>6}"
            f"{root_cause_string:>12}"
        )

    lines.append("")

    lines.append("=" * 80)

    return "\n".join(lines)