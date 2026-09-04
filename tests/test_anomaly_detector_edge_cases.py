"""Synthetic edge cases for the unchanged Phase 2 detector."""

import math

from scripts.run_observer_pipeline import _adapt_anomaly_for_observer
from src.ingestion.anomaly_detector import (
    AnomalyDetector,
    DetectorConfig,
    TimeSeries,
)
from src.ingestion.models import DetectionMethod


def _detector(minimum=6):
    return AnomalyDetector(
        DetectorConfig(
            method=DetectionMethod.Z_SCORE,
            z_threshold=3.0,
            min_baseline_samples=minimum,
        )
    )


def _series(values):
    return TimeSeries(
        case_id="case-001",
        service_name="service-a",
        signal_type="cpu",
        timestamps=list(range(len(values))),
        values=values,
    )


def test_empty_short_and_constant_series_are_explicitly_non_detectable():
    detector = _detector()

    assert detector.detect_series(_series([]), baseline_end=0) == []
    assert detector.detect_series(_series([1.0] * 5), baseline_end=4) == []
    assert detector.detect_series(
        _series([1.0] * 6 + [10.0]),
        baseline_end=6,
    ) == []


def test_nan_and_infinite_baseline_values_do_not_count_as_samples():
    detector = _detector()
    values = [-1.0, 0.0, 1.0, math.nan, math.inf, -math.inf, 10.0]

    assert detector.detect_series(_series(values), baseline_end=6) == []


def test_positive_and_negative_z_scores_keep_their_direction():
    detector = _detector()
    values = [-1.0, 0.0, 1.0, -1.0, 0.0, 1.0, 10.0, -10.0]

    anomalies = detector.detect_series(_series(values), baseline_end=6)

    assert len(anomalies) == 2
    assert anomalies[0].score > 0
    assert anomalies[1].score < 0


def test_observer_adapter_uses_magnitude_and_preserves_signed_score():
    detector = _detector()
    values = [-1.0, 0.0, 1.0, -1.0, 0.0, 1.0, -10.0]
    anomaly = detector.detect_series(_series(values), baseline_end=6)[0]

    adapted = _adapt_anomaly_for_observer(anomaly)

    assert anomaly.score < 0
    assert adapted.score == abs(anomaly.score)
    assert adapted.metadata["signed_detection_score"] == anomaly.score
    assert adapted.metadata["observer_score_adaptation"] == "absolute_z_score"
