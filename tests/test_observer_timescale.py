from datetime import datetime, timedelta, timezone

import pytest

from src.agents.observer.models import (
    AnomalyInput,
)

from src.agents.observer.timescale_consumer import (
    TimescaleConsumer,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)


def make_anomaly() -> AnomalyInput:

    return AnomalyInput(
        event_id="event_001",
        case_id="re2ob_checkoutservice_cpu_1",

        # Unix epoch seconds
        timestamp=1705354580,

        resource_id="checkoutservice",

        signal_type="cpu",

        metric_name="checkoutservice_cpu",

        value=5.63,

        score=66.01,

        detection_method=DetectionMethod.Z_SCORE,

        incident_type=(
            IncidentType.CPU_SATURATION
        ),
    )


def test_timestamp_conversion():

    timestamp = 1705354580

    result = (
        TimescaleConsumer.timestamp_to_datetime(
            timestamp
        )
    )

    assert isinstance(
        result,
        datetime,
    )

    assert result.tzinfo == timezone.utc


def test_timestamp_conversion_rejects_invalid():

    with pytest.raises(ValueError):

        TimescaleConsumer.timestamp_to_datetime(
            "invalid"
        )


def test_timestamp_conversion_accepts_float():

    result = (
        TimescaleConsumer.timestamp_to_datetime(
            1705354580.0
        )
    )

    assert result.tzinfo == timezone.utc


def test_consumer_requires_client():

    with pytest.raises(ValueError):

        TimescaleConsumer(None)


def test_negative_window_rejected():

    class DummyClient:
        pass

    consumer = TimescaleConsumer(
        DummyClient()
    )

    anomaly = make_anomaly()

    with pytest.raises(ValueError):

        consumer.get_observations_around_anomaly(
            anomaly,
            window_minutes=-1,
        )


def test_filter_metric_observations():

    observations = [
        {
            "metric_name": "checkoutservice_cpu",
            "value": 10.0,
        },
        {
            "metric_name": "checkoutservice_memory",
            "value": 20.0,
        },
        {
            "metric_name": "checkoutservice_cpu",
            "value": 30.0,
        },
    ]

    result = (
        TimescaleConsumer.filter_metric_observations(
            observations,
            "checkoutservice_cpu",
        )
    )

    assert len(result) == 2

    assert all(
        item["metric_name"]
        == "checkoutservice_cpu"
        for item in result
    )


def test_compute_statistics():

    observations = [
        {
            "value": 10.0,
        },
        {
            "value": 20.0,
        },
        {
            "value": 30.0,
        },
    ]

    statistics = (
        TimescaleConsumer.compute_statistics(
            observations
        )
    )

    assert statistics[
        "observation_count"
    ] == 3

    assert statistics[
        "minimum"
    ] == 10.0

    assert statistics[
        "maximum"
    ] == 30.0

    assert statistics[
        "mean"
    ] == 20.0

    assert statistics[
        "first_value"
    ] == 10.0

    assert statistics[
        "last_value"
    ] == 30.0

    assert statistics[
        "delta"
    ] == 20.0


def test_compute_statistics_empty():

    statistics = (
        TimescaleConsumer.compute_statistics(
            []
        )
    )

    assert statistics[
        "observation_count"
    ] == 0

    assert statistics[
        "mean"
    ] is None


def test_get_temporal_context_with_mocked_client():

    class DummyClient:
        pass

    consumer = TimescaleConsumer(
        DummyClient()
    )

    anomaly = make_anomaly()

    consumer.get_observations_around_anomaly = (
        lambda anomaly, window_minutes: [
            {
                "case_id": "re2ob_checkoutservice_cpu_1",
                "resource_id": "checkoutservice",
                "timestamp": datetime(
                    2024,
                    1,
                    15,
                    tzinfo=timezone.utc,
                ),
                "value": 2.0,
                "metric_name": (
                    "checkoutservice_cpu"
                ),
                "metric_type": "cpu",
            },
            {
                "case_id": "re2ob_checkoutservice_cpu_1",
                "resource_id": "checkoutservice",
                "timestamp": datetime(
                    2024,
                    1,
                    15,
                    0,
                    1,
                    tzinfo=timezone.utc,
                ),
                "value": 4.0,
                "metric_name": (
                    "checkoutservice_cpu"
                ),
                "metric_type": "cpu",
            },
        ]
    )

    context = (
        consumer.get_temporal_context(
            anomaly
        )
    )

    assert (
        context.resource_id
        == "checkoutservice"
    )

    assert (
        context.metric_name
        == "checkoutservice_cpu"
    )

    assert (
        context.signal_type
        == "cpu"
    )

    assert (
        len(context.observations)
        == 2
    )

    assert (
        context.statistics["mean"]
        == 3.0
    )

    assert (
        context.statistics["anomaly_value"]
        == 5.63
    )

    assert (
        context.statistics["anomaly_score"]
        == 66.01
    )


def test_temporal_context_excludes_other_cases():

    class DummyClient:
        pass

    consumer = TimescaleConsumer(DummyClient())
    anomaly = make_anomaly()

    consumer.get_observations_around_anomaly = (
        lambda anomaly, window_minutes: [
            {
                "case_id": anomaly.case_id,
                "metric_name": anomaly.metric_name,
                "value": 10.0,
            },
            {
                "case_id": "another_case",
                "metric_name": anomaly.metric_name,
                "value": 99.0,
            },
        ]
    )

    context = consumer.get_temporal_context(anomaly)

    assert len(context.observations) == 1
    assert context.statistics["mean"] == 10.0


def test_temporal_completeness_regular_window():
    """A missing timestamp must reduce temporal completeness."""

    start = datetime(
        2024,
        1,
        1,
        tzinfo=timezone.utc,
    )

    observations = [
        {
            "timestamp": start + timedelta(seconds=offset),
            "value": float(offset),
        }
        for offset in (0, 1, 2, 4)
    ]

    result = (
        TimescaleConsumer.compute_temporal_completeness(
            observations,
            requested_start_timestamp=start.timestamp(),
            requested_end_timestamp=(
                start + timedelta(seconds=4)
            ).timestamp(),
        )
    )

    assert result["first_observation_timestamp"] == start.timestamp()
    assert result["last_observation_timestamp"] == (
        start + timedelta(seconds=4)
    ).timestamp()
    assert result["actual_coverage_duration_seconds"] == 4.0
    assert result["estimated_sampling_interval_seconds"] == 1.0
    assert result["expected_observation_count"] == 5
    assert result["observed_unique_timestamp_count"] == 4
    assert result["missing_observation_count"] == 1
    assert result["temporal_data_completeness_ratio"] == pytest.approx(
        0.8
    )


def test_temporal_completeness_irregular_frequency():
    """Irregular timestamps use the median positive interval."""

    start = datetime(
        2024,
        1,
        1,
        tzinfo=timezone.utc,
    )

    observations = [
        {
            "timestamp": start + timedelta(seconds=offset),
            "value": float(offset),
        }
        for offset in (0, 2, 5, 9)
    ]

    result = (
        TimescaleConsumer.compute_temporal_completeness(
            observations,
            requested_start_timestamp=start.timestamp(),
            requested_end_timestamp=(
                start + timedelta(seconds=9)
            ).timestamp(),
        )
    )

    assert result["estimated_sampling_interval_seconds"] == 3.0
    assert result["expected_observation_count"] == 4
    assert result["missing_observation_count"] == 0
    assert result["temporal_data_completeness_ratio"] == 1.0


def test_temporal_completeness_empty_window():
    """Completeness is explicitly unavailable without observations."""

    result = (
        TimescaleConsumer.compute_temporal_completeness(
            [],
            requested_start_timestamp=100.0,
            requested_end_timestamp=200.0,
        )
    )

    assert result["first_observation_timestamp"] is None
    assert result["last_observation_timestamp"] is None
    assert result["actual_coverage_duration_seconds"] is None
    assert result["estimated_sampling_interval_seconds"] is None
    assert result["expected_observation_count"] is None
    assert result["observed_unique_timestamp_count"] == 0
    assert result["missing_observation_count"] is None
    assert result["temporal_data_completeness_ratio"] is None
def test_get_temporal_context_reports_completeness():
    """The returned context must expose effective window completeness."""

    class DummyClient:
        pass

    consumer = TimescaleConsumer(
        DummyClient()
    )

    anomaly = make_anomaly()

    anomaly_datetime = (
        TimescaleConsumer.timestamp_to_datetime(
            anomaly.timestamp
        )
    )

    consumer.get_observations_around_anomaly = (
        lambda anomaly, window_minutes: [
            {
                "case_id": anomaly.case_id,
                "resource_id": anomaly.resource_id,
                "metric_name": anomaly.metric_name,
                "metric_type": anomaly.signal_type,
                "timestamp": (
                    anomaly_datetime
                    + timedelta(seconds=offset)
                ),
                "value": float(index),
            }
            for index, offset in enumerate(
                (-60, 0, 60),
                start=1,
            )
        ]
    )

    context = consumer.get_temporal_context(
        anomaly,
        window_minutes=1,
    )

    assert context.requested_start_timestamp == (
        anomaly.timestamp - 60
    )
    assert context.requested_end_timestamp == (
        anomaly.timestamp + 60
    )
    assert context.requested_duration_seconds == 120.0

    assert context.first_observation_timestamp == (
        anomaly.timestamp - 60
    )
    assert context.last_observation_timestamp == (
        anomaly.timestamp + 60
    )
    assert context.actual_coverage_duration_seconds == 120.0

    assert context.estimated_sampling_interval_seconds == 60.0
    assert context.expected_observation_count == 3
    assert context.observed_unique_timestamp_count == 3
    assert context.missing_observation_count == 0
    assert context.temporal_data_completeness_ratio == 1.0