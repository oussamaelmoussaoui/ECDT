from unittest.mock import MagicMock

import pytest

from src.agents.observer.incident_persistence import (
    IncidentPersistence,
)

from src.agents.observer.models import (
    AnomalyInput,
)

from src.agents.observer.incident_builder import (
    build_incident,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)


def make_incident():

    anomaly = AnomalyInput(
        event_id="event_001",

        case_id="re2ob_checkoutservice_cpu_1",

        timestamp=1705353846,

        resource_id="checkoutservice",

        signal_type="cpu",

        metric_name="checkoutservice_cpu",

        value=0.21588648332356936,

        score=10.0,

        detection_method=(
            DetectionMethod.Z_SCORE
        ),

        incident_type=(
            IncidentType.CPU_SATURATION
        ),
    )

    return build_incident(anomaly)


def test_requires_neo4j_client():

    with pytest.raises(ValueError):

        IncidentPersistence(None)


def test_create_incident():

    client = MagicMock()

    client.execute_write.return_value = [
        {
            "incident_id": "inc_123",
            "case_id": (
                "re2ob_checkoutservice_cpu_1"
            ),
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    result = persistence.create_incident(
        incident
    )

    assert (
        result["incident_id"]
        == "inc_123"
    )

    client.execute_write.assert_called_once()


def test_create_incident_uses_incident_id():

    client = MagicMock()

    client.execute_write.return_value = [
        {
            "incident_id": "inc_123",
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    persistence.create_incident(
        incident
    )

    parameters = (
        client.execute_write.call_args.args[1]
    )

    assert (
        parameters["incident_id"]
        == incident.incident_id
    )


def test_link_incident_to_resource():

    client = MagicMock()

    client.execute_write.return_value = [
        {
            "incident_id": "inc_123",
            "resource_id": (
                "checkoutservice"
            ),
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    result = (
        persistence.link_incident_to_resource(
            incident
        )
    )

    assert (
        result["resource_id"]
        == "checkoutservice"
    )


def test_link_requires_existing_graph_entities():

    client = MagicMock()

    client.execute_write.return_value = []

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    with pytest.raises(ValueError):

        persistence.link_incident_to_resource(
            incident
        )


def test_persist_incident():

    client = MagicMock()

    client.execute_write.return_value = [
        {
            "incident_id": "inc_123",
            "resource_id": "checkoutservice",
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    result = (
        persistence.persist_incident(
            incident
        )
    )

    assert "incident" in result

    assert "relationship" in result

    assert (
        result["relationship"][
            "resource_id"
        ]
        == "checkoutservice"
    )

    assert (
        client.execute_write.call_count
        == 1
    )


def test_get_incident():

    client = MagicMock()

    client.execute.return_value = [
        {
            "incident_id": "inc_123",
            "resource_id": (
                "checkoutservice"
            ),
            "affected_resource": (
                "checkoutservice"
            ),
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    result = persistence.get_incident(
        "inc_123"
    )

    assert result is not None

    assert (
        result["affected_resource"]
        == "checkoutservice"
    )


def test_get_incident_returns_none():

    client = MagicMock()

    client.execute.return_value = []

    persistence = IncidentPersistence(
        client
    )

    assert (
        persistence.get_incident(
            "does_not_exist"
        )
        is None
    )


def test_incident_affects_resource_true():

    client = MagicMock()

    client.execute.return_value = [
        {
            "count": 1,
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    assert (
        persistence.incident_affects_resource(
            "inc_123",
            "checkoutservice",
        )
        is True
    )


def test_incident_affects_resource_false():

    client = MagicMock()

    client.execute.return_value = [
        {
            "count": 0,
        }
    ]

    persistence = IncidentPersistence(
        client
    )

    assert (
        persistence.incident_affects_resource(
            "inc_123",
            "checkoutservice",
        )
        is False
    )


def test_parameter_serialization():

    client = MagicMock()

    persistence = IncidentPersistence(
        client
    )

    incident = make_incident()

    parameters = (
        persistence._incident_parameters(
            incident
        )
    )

    assert (
        parameters["case_id"]
        == "re2ob_checkoutservice_cpu_1"
    )

    assert (
        parameters["resource_id"]
        == "checkoutservice"
    )

    assert (
        parameters["signal_type"]
        == "cpu"
    )

    assert (
        parameters["metric_name"]
        == "checkoutservice_cpu"
    )

    assert isinstance(
        parameters["observed_value"],
        float,
    )

    assert isinstance(
        parameters["anomaly_score"],
        float,
    )

    assert isinstance(
        parameters["confidence"],
        float,
    )
