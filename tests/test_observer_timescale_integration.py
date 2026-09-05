"""
Integration tests for the Observer -> TimescaleDB pipeline.

These tests require a running ECDT TimescaleDB instance.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from src.agents.observer.models import (
    AnomalyInput,
)

from src.agents.observer.timescale_consumer import (
    TimescaleConsumer,
)

from src.digital_twin.timescale_client import (
    TimescaleClient,
)

from src.digital_twin.timeseries_queries import (
    audit_metric_observation_duplicates,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)


CASE_ID = "re2ob_checkoutservice_cpu_1"

RESOURCE_ID = "checkoutservice"

METRIC_NAME = "checkoutservice_cpu"

# 2024-01-15 21:24:06 UTC
ANOMALY_TIMESTAMP = 1705353846

# Injection-centered ten-minute window: 601 expected one-second samples.
AUDIT_CENTER_TIMESTAMP = 1705354566


def make_real_anomaly() -> AnomalyInput:
    """
    Build an AnomalyInput corresponding to an actual
    observation currently stored in TimescaleDB.
    """

    return AnomalyInput(
        event_id="integration_event_001",

        case_id=CASE_ID,

        timestamp=ANOMALY_TIMESTAMP,

        resource_id=RESOURCE_ID,

        signal_type="cpu",

        metric_name=METRIC_NAME,

        value=0.21588648332356936,

        score=10.0,

        detection_method=DetectionMethod.Z_SCORE,

        incident_type=IncidentType.CPU_SATURATION,
    )


@pytest.fixture(scope="module")
def timescale_client():
    """
    Create a real TimescaleDB client.

    The test expects the ECDT TimescaleDB service to be
    running through Docker Compose.
    """

    if not os.getenv("TIMESCALE_URI"):
        pytest.skip(
            "TIMESCALE_URI is not configured."
        )

    return TimescaleClient()


def test_real_timescaledb_connection(
    timescale_client,
):
    """
    Verify that the Observer can connect to the real
    TimescaleDB instance.
    """

    result = timescale_client.execute(
        "SELECT 1 AS value;",
        fetch=True,
    )

    assert result

    assert result[0]["value"] == 1


def test_real_anomaly_observation_exists(
    timescale_client,
):
    """
    Verify that the exact Phase 5 anomaly observation
    exists in TimescaleDB.
    """

    result = timescale_client.execute(
        """
        SELECT
            resource_id,
            metric_name,
            metric_type,
            value,
            case_id
        FROM metric_observations
        WHERE case_id = %s
          AND resource_id = %s
          AND metric_name = %s
          AND timestamp = to_timestamp(%s)
        """,
        (
            CASE_ID,
            RESOURCE_ID,
            METRIC_NAME,
            ANOMALY_TIMESTAMP,
        ),
        fetch=True,
    )

    assert len(result) == 1

    observation = result[0]

    assert (
        observation["resource_id"]
        == RESOURCE_ID
    )

    assert (
        observation["metric_name"]
        == METRIC_NAME
    )

    assert (
        observation["metric_type"]
        == "cpu"
    )

    assert (
        observation["case_id"]
        == CASE_ID
    )

    assert observation["value"] is not None


def test_observer_reads_real_timescaledb(
    timescale_client,
):
    """
    End-to-end test of:

        AnomalyInput
            |
            v
        TimescaleConsumer
            |
            v
        TimescaleDB
    """

    anomaly = make_real_anomaly()

    consumer = TimescaleConsumer(
        timescale_client
    )

    observations = (
        consumer.get_observations_around_anomaly(
            anomaly,
            window_minutes=5,
        )
    )

    assert isinstance(
        observations,
        list,
    )

    assert len(observations) >= 1


def test_observer_filters_real_metric(
    timescale_client,
):
    """
    Verify that the Observer keeps the expected
    metric from the real TimescaleDB response.
    """

    anomaly = make_real_anomaly()

    consumer = TimescaleConsumer(
        timescale_client
    )

    observations = (
        consumer.get_observations_around_anomaly(
            anomaly,
            window_minutes=5,
        )
    )

    metric_observations = (
        consumer.filter_metric_observations(
            observations,
            METRIC_NAME,
        )
    )

    assert len(metric_observations) >= 1

    assert all(
        observation["metric_name"]
        == METRIC_NAME
        for observation
        in metric_observations
    )


def test_observer_builds_real_temporal_context(
    timescale_client,
):
    """
    Full Phase 5.3 validation.

        Real TimescaleDB
              |
              v
        TimescaleConsumer
              |
              v
        TemporalContext
    """

    anomaly = make_real_anomaly()

    consumer = TimescaleConsumer(
        timescale_client
    )

    context = (
        consumer.get_temporal_context(
            anomaly,
            window_minutes=5,
        )
    )

    assert (
        context.resource_id
        == RESOURCE_ID
    )

    assert (
        context.metric_name
        == METRIC_NAME
    )

    assert (
        context.signal_type
        == "cpu"
    )

    assert (
        context.anomaly_timestamp
        == ANOMALY_TIMESTAMP
    )

    assert (
        context.window_before_seconds
        == 300
    )

    assert (
        context.window_after_seconds
        == 300
    )

    assert (
        len(context.observations)
        >= 1
    )

    statistics = context.statistics

    assert (
        statistics["observation_count"]
        >= 1
    )

    assert (
        statistics["minimum"]
        is not None
    )

    assert (
        statistics["maximum"]
        is not None
    )

    assert (
        statistics["mean"]
        is not None
    )

    assert (
        statistics["anomaly_value"]
        == anomaly.value
    )

    assert (
        statistics["anomaly_score"]
        == anomaly.score
    )

    assert context.rows_retrieved == len(context.observations)
    assert context.numeric_observation_count == (
        statistics["observation_count"]
    )
    assert context.observed_unique_timestamp_count <= (
        context.rows_retrieved
    )
    assert context.requested_duration_seconds == 600.0
    assert context.temporal_data_completeness_ratio is not None


def test_real_metric_window_contains_no_duplicate_timestamps(
    timescale_client,
):
    """Audit the verified ten-minute TimescaleDB window without mutation."""

    center = datetime.fromtimestamp(
        AUDIT_CENTER_TIMESTAMP,
        tz=timezone.utc,
    )
    result = audit_metric_observation_duplicates(
        timescale_client,
        case_id=CASE_ID,
        resource_id=RESOURCE_ID,
        metric_name=METRIC_NAME,
        start_time=center - timedelta(minutes=5),
        end_time=center + timedelta(minutes=5),
    )

    assert result["total_rows"] == 601
    assert result["distinct_timestamps"] == 601
    assert result["possible_duplicate_rows"] == 0


def test_timescaledb_contains_no_ground_truth_labels(
    timescale_client,
):
    """Operational time-series data must contain no RCAEval label."""

    result = timescale_client.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (
                WHERE fault IS NOT NULL
            ) AS labeled_rows
        FROM metric_observations;
        """,
        fetch=True,
    )

    assert result
    assert result[0]["total_rows"] > 0
    assert result[0]["labeled_rows"] == 0
