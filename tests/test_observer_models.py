from src.agents.observer.models import (
    AnomalyInput,
    Incident,
    IncidentContext,
    IncidentSeverity,
    IncidentSource,
    IncidentStatus,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)


def test_anomaly_input_creation():

    anomaly = AnomalyInput(
        event_id="event_001",
        case_id="re2ob_checkoutservice_cpu_1",
        timestamp=1705354580000,
        resource_id="checkoutservice",
        signal_type="cpu",
        metric_name="checkoutservice_cpu",
        value=5.63,
        score=66.01,
        detection_method=DetectionMethod.Z_SCORE,
        incident_type=IncidentType.CPU_SATURATION,
    )

    assert anomaly.case_id == "re2ob_checkoutservice_cpu_1"
    assert anomaly.resource_id == "checkoutservice"
    assert anomaly.signal_type == "cpu"
    assert anomaly.score == 66.01
    assert anomaly.incident_type == IncidentType.CPU_SATURATION


def test_incident_creation():

    incident = Incident(
        incident_id="inc_001",
        case_id="re2ob_checkoutservice_cpu_1",
        incident_type=IncidentType.CPU_SATURATION,
        status=IncidentStatus.DETECTED,
        severity=IncidentSeverity.CRITICAL,
        resource_id="checkoutservice",
        detected_at=1705354580000,
        signal_type="cpu",
        metric_name="checkoutservice_cpu",
        observed_value=5.63,
        anomaly_score=66.01,
        detection_method=DetectionMethod.Z_SCORE,
        confidence=1.0,
    )

    assert incident.incident_id == "inc_001"
    assert incident.status == IncidentStatus.DETECTED
    assert incident.severity == IncidentSeverity.CRITICAL
    assert incident.source == IncidentSource.OBSERVER


def test_incident_context_creation():

    incident = Incident(
        incident_id="inc_001",
        case_id="re2ob_checkoutservice_cpu_1",
        incident_type=IncidentType.CPU_SATURATION,
        status=IncidentStatus.DETECTED,
        severity=IncidentSeverity.CRITICAL,
        resource_id="checkoutservice",
        detected_at=1705354580000,
        signal_type="cpu",
        metric_name="checkoutservice_cpu",
        observed_value=5.63,
        anomaly_score=66.01,
        detection_method=DetectionMethod.Z_SCORE,
        confidence=1.0,
    )

    context = IncidentContext(
        incident=incident,
        resource_id="checkoutservice",
        detection_timestamp=1705354580000,
        signal_type="cpu",
        metric_name="checkoutservice_cpu",
        observed_value=5.63,
        anomaly_score=66.01,
    )

    assert context.incident is incident
    assert context.resource_id == "checkoutservice"
    assert context.detection_timestamp == 1705354580000