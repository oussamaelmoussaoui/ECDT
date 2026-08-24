from pathlib import Path

import pytest

from src.ingestion.dataset_loader import create_default_loader
from src.ingestion.schema_normalizer import SchemaNormalizer
from src.ingestion.anomaly_detector import AnomalyDetector


PROJECT_ROOT = Path(__file__).resolve().parents[1]


CASES = {
    "CPU": "re2ob_checkoutservice_cpu_1",
    "DELAY": "re2ob_checkoutservice_delay_1",
    "LOSS": "re2ob_checkoutservice_loss_1",
    "SOCKET": "re2ob_checkoutservice_socket_1",
}


EXPECTED = {
    "CPU": {
        "anomalies": 707,
        "before": 0,
        "after": 707,
        "delay_seconds": 14.0,
    },
    "DELAY": {
        "anomalies": 15,
        "before": 0,
        "after": 15,
        "delay_seconds": 188.0,
    },
    "LOSS": {
        "anomalies": 51,
        "before": 0,
        "after": 51,
        "delay_seconds": 58.0,
    },
    "SOCKET": {
        "anomalies": 236,
        "before": 0,
        "after": 236,
        "delay_seconds": 7.0,
    },
}


@pytest.fixture(scope="module")
def loader():
    return create_default_loader(PROJECT_ROOT)


@pytest.fixture(scope="module")
def normalizer():
    return SchemaNormalizer()


@pytest.fixture(scope="module")
def detector():
    return AnomalyDetector()


def run_incident_test(
    loader,
    normalizer,
    detector,
    fault,
):
    case = CASES[fault]
    expected = EXPECTED[fault]

    info = loader.get_case_info(case)

    data = loader.load_case(
        case,
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

    ts = series[("checkoutservice", "cpu")]

    anomalies = detector.detect_series(
        ts,
        baseline_end=info.inject_time_ms,
    )

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

    assert len(anomalies) == expected["anomalies"]

    assert len(before) == expected["before"]

    assert len(after) == expected["after"]

    assert after, (
        f"No anomaly detected after injection "
        f"for {fault}"
    )

    first_detection = min(
        anomaly.timestamp
        for anomaly in after
    )

    delay_seconds = (
        first_detection - info.inject_time_ms
    ) / 1000.0

    assert delay_seconds == expected["delay_seconds"]

    return {
        "fault": fault,
        "case": case,
        "anomalies": len(anomalies),
        "before": len(before),
        "after": len(after),
        "delay_seconds": delay_seconds,
    }


def test_cpu_incident(
    loader,
    normalizer,
    detector,
):
    result = run_incident_test(
        loader,
        normalizer,
        detector,
        "CPU",
    )

    assert result["after"] > 0


def test_delay_incident(
    loader,
    normalizer,
    detector,
):
    result = run_incident_test(
        loader,
        normalizer,
        detector,
        "DELAY",
    )

    assert result["after"] > 0


def test_loss_incident(
    loader,
    normalizer,
    detector,
):
    result = run_incident_test(
        loader,
        normalizer,
        detector,
        "LOSS",
    )

    assert result["after"] > 0


def test_socket_incident(
    loader,
    normalizer,
    detector,
):
    result = run_incident_test(
        loader,
        normalizer,
        detector,
        "SOCKET",
    )

    assert result["after"] > 0


def test_all_four_incidents(
    loader,
    normalizer,
    detector,
):
    results = []

    for fault in CASES:
        result = run_incident_test(
            loader,
            normalizer,
            detector,
            fault,
        )

        results.append(result)

    assert len(results) == 4

    for result in results:
        assert result["before"] == 0
        assert result["after"] > 0
        assert result["delay_seconds"] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])