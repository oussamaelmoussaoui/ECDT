"""
ECDT - Phase 5.5
Unit tests for ObserverAgent.

These tests isolate the Observer orchestration layer.
TimescaleDB and Neo4j are mocked.

Real integration tests belong to the Phase 5.5
integration test suite.
"""

from unittest.mock import MagicMock

import pytest

from src.agents.observer.models import (
    TemporalContext,
)

from src.agents.observer.observer_agent import (
    IncidentContext,
    ObserverAgent,
)

from src.ingestion.models import (
    AnomalyEvent,
    DetectionMethod,
    IncidentType,
)


CASE_ID = "re2ob_checkoutservice_cpu_1"

RESOURCE_ID = "checkoutservice"

SIGNAL_TYPE = "cpu"

METRIC_NAME = "checkoutservice_cpu"

TIMESTAMP = 1705353846

VALUE = 0.21588648332356936

SCORE = 10.0


def make_anomaly() -> AnomalyEvent:
    """
    Build a deterministic Phase 2 AnomalyEvent.
    """

    return AnomalyEvent(
        event_id="event_001",

        case_id=CASE_ID,

        timestamp=TIMESTAMP,

        service=RESOURCE_ID,

        signal_type=SIGNAL_TYPE,

        value=VALUE,

        detection_method=DetectionMethod.Z_SCORE,

        score=SCORE,

        threshold=3.0,

        incident_type=IncidentType.CPU_SATURATION,

    )

def make_low_confidence_anomaly() -> AnomalyEvent:

    return AnomalyEvent(
        event_id="event_low_confidence",

        case_id=CASE_ID,

        timestamp=TIMESTAMP,

        service=RESOURCE_ID,

        signal_type=SIGNAL_TYPE,

        value=VALUE,

        detection_method=DetectionMethod.Z_SCORE,

        score=2.0,

        threshold=3.0,

        incident_type=(
            IncidentType.CPU_SATURATION
        ),
    )

def make_temporal_context() -> TemporalContext:
    """
    Build a deterministic temporal context returned
    by the mocked TimescaleDB consumer.
    """

    return TemporalContext(
        resource_id=RESOURCE_ID,

        metric_name=METRIC_NAME,

        signal_type=SIGNAL_TYPE,

        anomaly_timestamp=TIMESTAMP,

        window_before_seconds=300,

        window_after_seconds=300,

        observations=[
            {
                "timestamp": TIMESTAMP,
                "resource_id": RESOURCE_ID,
                "metric_name": METRIC_NAME,
                "metric_type": SIGNAL_TYPE,
                "value": VALUE,
            }
        ],

        statistics={
            "observation_count": 1,
            "minimum": VALUE,
            "maximum": VALUE,
            "mean": VALUE,
            "anomaly_value": VALUE,
            "anomaly_score": SCORE,
        },
    )


@pytest.fixture
def timescale_consumer():
    """
    Mock TimescaleDB consumer.
    """

    consumer = MagicMock()

    consumer.get_temporal_context.return_value = (
        make_temporal_context()
    )

    return consumer


@pytest.fixture
def incident_persistence():
    """
    Mock Neo4j incident persistence layer.
    """

    persistence = MagicMock()

    persistence.persist_incident.return_value = {
        "incident": {
            "incident_id": "inc_test"
        },
        "relationship": {
            "incident_id": "inc_test",
            "resource_id": RESOURCE_ID,
        },
    }

    return persistence


@pytest.fixture
def observer(
    timescale_consumer,
    incident_persistence,
):
    """
    Build an ObserverAgent using mocked dependencies.
    """

    return ObserverAgent(
        timescale_consumer=timescale_consumer,
        incident_persistence=incident_persistence,
        window_minutes=5,
    )


# =====================================================================
# Constructor
# =====================================================================


def test_observer_requires_timescale_consumer():

    persistence = MagicMock()

    with pytest.raises(ValueError):

        ObserverAgent(
            timescale_consumer=None,
            incident_persistence=persistence,
        )


def test_observer_requires_incident_persistence():

    consumer = MagicMock()

    with pytest.raises(ValueError):

        ObserverAgent(
            timescale_consumer=consumer,
            incident_persistence=None,
        )


def test_negative_window_rejected():

    consumer = MagicMock()

    persistence = MagicMock()

    with pytest.raises(ValueError):

        ObserverAgent(
            timescale_consumer=consumer,
            incident_persistence=persistence,
            window_minutes=-1,
        )


def test_invalid_confidence_threshold_rejected():

    consumer = MagicMock()

    persistence = MagicMock()

    with pytest.raises(ValueError):

        ObserverAgent(
            timescale_consumer=consumer,
            incident_persistence=persistence,
            minimum_confidence=1.5,
        )


# =====================================================================
# Anomaly conversion
# =====================================================================


def test_anomaly_event_is_converted_to_anomaly_input():

    anomaly = make_anomaly()

    result = ObserverAgent._to_anomaly_input(
        anomaly
    )

    assert result.event_id == "event_001"

    assert result.case_id == CASE_ID

    assert result.timestamp == TIMESTAMP

    assert result.resource_id == RESOURCE_ID

    assert result.signal_type == SIGNAL_TYPE

    assert result.metric_name == METRIC_NAME

    assert result.value == VALUE

    assert result.score == SCORE

    assert (
        result.detection_method
        == DetectionMethod.Z_SCORE
    )

    assert (
        result.incident_type
        == IncidentType.CPU_SATURATION
    )

    assert result.metadata["source_timestamp"] == TIMESTAMP


def test_anomaly_timestamp_milliseconds_are_normalized():

    anomaly = make_anomaly()
    anomaly.timestamp = TIMESTAMP * 1000

    result = ObserverAgent._to_anomaly_input(anomaly)

    assert result.timestamp == TIMESTAMP
    assert result.metadata["source_timestamp"] == TIMESTAMP * 1000
    assert result.metadata["source_timestamp_unit"] == "milliseconds"


# =====================================================================
# Qualification
# =====================================================================


def test_anomaly_is_qualified(
    observer,
):

    anomaly = make_anomaly()

    anomaly_input = (
        observer._to_anomaly_input(
            anomaly
        )
    )

    temporal_context = (
        make_temporal_context()
    )

    result = observer._qualify(
        anomaly_input,
        temporal_context,
    )

    assert result["is_qualified"] is True

    assert (
        result["confidence"]
        == 1.0
    )

    assert (
        result["observation_count"]
        == 1
    )

    assert (
        result["score"]
        == SCORE
    )

    assert (
        result["resource_id"]
        == RESOURCE_ID
    )

    assert (
        result["signal_type"]
        == SIGNAL_TYPE
    )

    assert (
        result["metric_name"]
        == METRIC_NAME
    )


def test_confidence_is_bounded():

    anomaly = make_anomaly()

    anomaly_input = (
        ObserverAgent._to_anomaly_input(
            anomaly
        )
    )

    temporal_context = (
        make_temporal_context()
    )

    result = ObserverAgent._extract_confidence(
        anomaly_input,
        temporal_context,
    )

    assert 0.0 <= result <= 1.0


# =====================================================================
# Full orchestration
# =====================================================================


def test_process_returns_incident_context(
    observer,
):

    anomaly = make_anomaly()

    result = observer.process(
        anomaly
    )

    assert isinstance(
        result,
        IncidentContext,
    )

    assert result.persisted is True

    assert (
        result.case_id
        == CASE_ID
    )

    assert (
        result.resource_id
        == RESOURCE_ID
    )

    assert result.incident_id

    assert result.incident is not None

    assert (
        result.temporal_context
        is not None
    )

    assert (
        result.qualification
        is not None
    )


def test_process_reads_temporal_context(
    observer,
    timescale_consumer,
):

    anomaly = make_anomaly()

    observer.process(
        anomaly
    )

    timescale_consumer.get_temporal_context.assert_called_once()

    call = (
        timescale_consumer
        .get_temporal_context
        .call_args
    )

    assert call.args[0].case_id == CASE_ID

    assert (
        call.args[0].resource_id
        == RESOURCE_ID
    )

    assert (
        call.kwargs["window_minutes"]
        == 5
    )


def test_process_persists_incident(
    observer,
    incident_persistence,
):

    anomaly = make_anomaly()

    result = observer.process(
        anomaly
    )

    incident_persistence.persist_incident.assert_called_once()

    persisted_incident = (
        incident_persistence
        .persist_incident
        .call_args.args[0]
    )

    assert (
        persisted_incident.incident_id
        == result.incident_id
    )

    assert (
        persisted_incident.case_id
        == CASE_ID
    )

    assert (
        persisted_incident.resource_id
        == RESOURCE_ID
    )


def test_process_preserves_temporal_context(
    observer,
    timescale_consumer,
):

    anomaly = make_anomaly()

    result = observer.process(
        anomaly
    )

    expected = (
        timescale_consumer
        .get_temporal_context
        .return_value
    )

    assert (
        result.temporal_context
        is expected
    )


def test_process_qualification_contains_signal_information(
    observer,
):

    anomaly = make_anomaly()

    result = observer.process(
        anomaly
    )

    qualification = (
        result.qualification
    )

    assert (
        qualification["resource_id"]
        == RESOURCE_ID
    )

    assert (
        qualification["signal_type"]
        == SIGNAL_TYPE
    )

    assert (
        qualification["metric_name"]
        == METRIC_NAME
    )

    assert (
        qualification["score"]
        == SCORE
    )


# =====================================================================
# Confidence threshold
# =====================================================================


def test_process_rejects_low_confidence():

    consumer = MagicMock()

    consumer.get_temporal_context.return_value = (
        make_temporal_context()
    )

    persistence = MagicMock()

    observer = ObserverAgent(
        timescale_consumer=consumer,
        incident_persistence=persistence,
        window_minutes=5,
        minimum_confidence=0.8,
    )

    anomaly = make_low_confidence_anomaly()

    with pytest.raises(ValueError):

        observer.process(
            anomaly
        )

    persistence.persist_incident.assert_not_called()


# =====================================================================
# Input validation
# =====================================================================


def test_process_rejects_none_anomaly(
    observer,
):

    with pytest.raises(ValueError):

        observer.process(
            None
        )


# =====================================================================
# Incident identity consistency
# =====================================================================


def test_incident_context_identity_is_consistent(
    observer,
):

    anomaly = make_anomaly()

    result = observer.process(
        anomaly
    )

    assert (
        result.incident_id
        == result.incident.incident_id
    )

    assert (
        result.case_id
        == result.incident.case_id
    )

    assert (
        result.resource_id
        == result.incident.resource_id
    )
