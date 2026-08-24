from pathlib import Path

import pytest

from src.ingestion.dataset_loader import create_default_loader
from src.ingestion.schema_normalizer import SchemaNormalizer
from src.ingestion.anomaly_detector import AnomalyDetector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASE = "re2ob_checkoutservice_cpu_1"


@pytest.fixture(scope="module")
def loader():
    return create_default_loader(PROJECT_ROOT)


@pytest.fixture(scope="module")
def normalizer():
    return SchemaNormalizer()


@pytest.fixture(scope="module")
def detector():
    return AnomalyDetector()


def test_zscore(detector):
    assert detector.zscore(10.0, 10.0, 1.0) == 0.0
    assert detector.zscore(11.0, 10.0, 1.0) == 1.0
    assert detector.zscore(13.0, 10.0, 1.0) == 3.0
    assert detector.zscore(7.0, 10.0, 1.0) == -3.0


def test_compute_baseline(detector):
    values = [
        0.4,
        0.42,
        0.43,
        0.41,
        0.45,
    ]

    mean, std, maximum = detector.compute_baseline(values)

    assert mean > 0
    assert std > 0
    assert maximum == 0.45


def test_build_series(loader, normalizer, detector):
    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    events = events_df.to_dicts()

    series = detector.build_series(events)

    assert isinstance(series, dict)
    assert len(series) == 72

    assert ("checkoutservice", "cpu") in series


def test_checkoutservice_cpu_series(loader, normalizer, detector):
    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    series = detector.build_series(
        events_df.to_dicts()
    )

    ts = series[("checkoutservice", "cpu")]

    assert len(ts) == 1441

    assert min(ts.timestamps) == 1705353846000
    assert max(ts.timestamps) == 1705355286000


def test_cpu_baseline(loader, detector):
    info = loader.get_case_info(CASE)

    metrics = loader.load_metrics(
        case_id=CASE,
        long_format=True,
    )

    x = metrics.filter(
        (metrics["metric_name"] == "checkoutservice_cpu")
        & (metrics["timestamp"] < info.inject_time_ms)
    )

    values = x["value"].to_list()

    mean, std, maximum = detector.compute_baseline(values)

    assert len(values) == 720

    assert abs(mean - 0.42885440894546123) < 1e-6
    assert abs(std - 0.07882683449695392) < 1e-6
    assert abs(maximum - 0.6869909369114802) < 1e-6


def test_cpu_detection(loader, normalizer, detector):
    info = loader.get_case_info(CASE)

    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    series = detector.build_series(
        events_df.to_dicts()
    )

    ts = series[("checkoutservice", "cpu")]

    anomalies = detector.detect_series(
        ts,
        baseline_end=info.inject_time_ms,
    )

    assert len(anomalies) == 707

    before = [
        anomaly
        for anomaly in anomalies
        if anomaly.timestamp < info.inject_time_ms
    ]

    after = [
        anomaly
        for anomaly in anomalies
        if anomaly.timestamp >= info.inject_time_ms
    ]

    assert len(before) == 0
    assert len(after) == 707


def test_cpu_detection_delay(loader, normalizer, detector):
    info = loader.get_case_info(CASE)

    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    series = detector.build_series(
        events_df.to_dicts()
    )

    ts = series[("checkoutservice", "cpu")]

    anomalies = detector.detect_series(
        ts,
        baseline_end=info.inject_time_ms,
    )

    after = [
        anomaly
        for anomaly in anomalies
        if anomaly.timestamp >= info.inject_time_ms
    ]

    assert after

    first_detection = min(
        anomaly.timestamp
        for anomaly in after
    )

    delay_seconds = (
        first_detection - info.inject_time_ms
    ) / 1000.0

    assert delay_seconds == 14.0


def test_first_cpu_anomaly(loader, normalizer, detector):
    info = loader.get_case_info(CASE)

    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    series = detector.build_series(
        events_df.to_dicts()
    )

    ts = series[("checkoutservice", "cpu")]

    anomalies = detector.detect_series(
        ts,
        baseline_end=info.inject_time_ms,
    )

    first = min(
        (
            anomaly
            for anomaly in anomalies
            if anomaly.timestamp >= info.inject_time_ms
        ),
        key=lambda anomaly: anomaly.timestamp,
    )

    assert first.value > 5.0
    assert first.score > 60.0
    assert first.is_anomaly is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])