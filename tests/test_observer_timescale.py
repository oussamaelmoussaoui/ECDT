from datetime import datetime, timezone

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
