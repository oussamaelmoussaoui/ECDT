from src.agents.observer.incident_builder import (
    build_incident,
    build_incident_context,
    determine_confidence,
    determine_incident_type,
    determine_severity,
    generate_incident_id,
)

from src.agents.observer.models import (
    AnomalyInput,
    IncidentSeverity,
    IncidentStatus,
    IncidentSource,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)


def make_cpu_anomaly(
    score: float = 10.0,
) -> AnomalyInput:

    return AnomalyInput(
        event_id="event_001",
        case_id="re2ob_checkoutservice_cpu_1",
        timestamp=1705354580000,
        resource_id="checkoutservice",
        signal_type="cpu",
        metric_name="checkoutservice_cpu",
        value=5.63,
        score=score,
        detection_method=DetectionMethod.Z_SCORE,
        incident_type=IncidentType.CPU_SATURATION,
    )


def test_severity_low():
    assert (
        determine_severity(2.99)
        == IncidentSeverity.LOW
    )


def test_severity_medium():
    assert (
        determine_severity(3.0)
        == IncidentSeverity.MEDIUM
    )


def test_severity_high():
    assert (
        determine_severity(6.0)
        == IncidentSeverity.HIGH
    )


def test_severity_critical():
    assert (
        determine_severity(10.0)
        == IncidentSeverity.CRITICAL
    )


def test_cpu_incident_type():

    anomaly = make_cpu_anomaly()

    assert (
        determine_incident_type(anomaly)
        == IncidentType.CPU_SATURATION
    )


def test_incident_id_is_deterministic():

    anomaly = make_cpu_anomaly()

    first_id = generate_incident_id(anomaly)
    second_id = generate_incident_id(anomaly)

    assert first_id == second_id


def test_incident_id_changes_for_different_event():

    anomaly_1 = make_cpu_anomaly()

    anomaly_2 = make_cpu_anomaly()
    anomaly_2.event_id = "event_002"

    assert (
        generate_incident_id(anomaly_1)
        != generate_incident_id(anomaly_2)
    )


def test_build_incident():

    anomaly = make_cpu_anomaly(
        score=66.01
    )

    incident = build_incident(anomaly)

    assert (
        incident.incident_type
        == IncidentType.CPU_SATURATION
    )

    assert (
        incident.status
        == IncidentStatus.DETECTED
    )

    assert (
        incident.severity
        == IncidentSeverity.CRITICAL
    )

    assert (
        incident.resource_id
        == "checkoutservice"
    )

    assert (
        incident.observed_value
        == 5.63
    )

    assert (
        incident.anomaly_score
        == 66.01
    )

    assert (
        incident.source
        == IncidentSource.OBSERVER
    )


def test_build_incident_context():

    anomaly = make_cpu_anomaly(
        score=66.01
    )

    incident = build_incident(anomaly)

    context = build_incident_context(
        incident
    )

    assert context.incident is incident

    assert (
        context.resource_id
        == "checkoutservice"
    )

    assert (
        context.signal_type
        == "cpu"
    )

    assert (
        context.metric_name
        == "checkoutservice_cpu"
    )

    assert (
        context.observed_value
        == 5.63
    )

    assert (
        context.anomaly_score
        == 66.01
    )


def test_negative_score_rejected():

    anomaly = make_cpu_anomaly(
        score=-1.0
    )

    try:
        build_incident(anomaly)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_confidence_threshold():

    confidence = determine_confidence(
        score=100.0,
        detection_method=DetectionMethod.THRESHOLD,
    )

    assert confidence == 1.0


def test_zscore_confidence_is_bounded():

    confidence = determine_confidence(
        score=66.01,
        detection_method=DetectionMethod.Z_SCORE,
    )

    assert 0.0 <= confidence <= 1.0