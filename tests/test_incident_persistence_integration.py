"""
ECDT - Phase 5.4
Real Neo4j integration test for Observer incident persistence.

Requires:
    - Neo4j running through Docker Compose
    - ECDT Neo4j schema initialized
    - checkoutservice Service node already present
"""

import os

import pytest

from src.agents.observer.incident_builder import (
    build_incident,
)

from src.agents.observer.incident_persistence import (
    IncidentPersistence,
)

from src.agents.observer.models import (
    AnomalyInput,
)

from src.ingestion.models import (
    DetectionMethod,
    IncidentType,
)

from src.knowledge_graph.neo4j_client import (
    Neo4jClient,
)


CASE_ID = "re2ob_checkoutservice_cpu_1"

RESOURCE_ID = "checkoutservice"

METRIC_NAME = "checkoutservice_cpu"

ANOMALY_TIMESTAMP = 1705353846

ANOMALY_VALUE = 0.21588648332356936

ANOMALY_SCORE = 10.0


def make_real_anomaly() -> AnomalyInput:
    """
    Build an anomaly corresponding to the real
    TimescaleDB validation case.
    """

    return AnomalyInput(
        event_id="integration_event_001",

        case_id=CASE_ID,

        timestamp=ANOMALY_TIMESTAMP,

        resource_id=RESOURCE_ID,

        signal_type="cpu",

        metric_name=METRIC_NAME,

        value=ANOMALY_VALUE,

        score=ANOMALY_SCORE,

        detection_method=(
            DetectionMethod.Z_SCORE
        ),

        incident_type=(
            IncidentType.CPU_SATURATION
        ),
    )


@pytest.fixture(scope="module")
def neo4j_client():
    """
    Create a real Neo4j client.

    Neo4j must be running before executing this test.
    """

    if not os.getenv("NEO4J_URI"):
        pytest.skip(
            "NEO4J_URI is not configured."
        )

    client = Neo4jClient()

    try:
        client.verify_connectivity()

        yield client

    finally:
        client.close()


def test_service_exists(
    neo4j_client,
):
    """
    Verify that the resource targeted by the Observer
    already exists in the Knowledge Graph.
    """

    result = neo4j_client.execute(
        """
        MATCH (s:Service {
            id: $resource_id
        })
        RETURN s.id AS resource_id
        """,
        {
            "resource_id": RESOURCE_ID,
        },
    )

    assert result

    assert (
        result[0]["resource_id"]
        == RESOURCE_ID
    )


def test_create_real_incident(
    neo4j_client,
):
    """
    Create a real Incident node in Neo4j.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    result = persistence.create_incident(
        incident
    )

    assert result

    assert (
        result["incident_id"]
        == incident.incident_id
    )

    assert (
        result["case_id"]
        == CASE_ID
    )


def test_link_real_incident_to_service(
    neo4j_client,
):
    """
    Create the real:

        (:Incident)-[:AFFECTS]->(:Service)

    relationship.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    result = (
        persistence.link_incident_to_resource(
            incident
        )
    )

    assert result

    assert (
        result["incident_id"]
        == incident.incident_id
    )

    assert (
        result["resource_id"]
        == RESOURCE_ID
    )


def test_real_incident_can_be_retrieved(
    neo4j_client,
):
    """
    Verify that the Incident persisted in Neo4j
    can be retrieved.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    result = persistence.get_incident(
        incident.incident_id
    )

    assert result is not None

    assert (
        result["incident_id"]
        == incident.incident_id
    )

    assert (
        result["case_id"]
        == CASE_ID
    )

    assert (
        result["resource_id"]
        == RESOURCE_ID
    )


def test_real_incident_affects_service(
    neo4j_client,
):
    """
    Verify the actual graph relationship:

        Incident
            |
            | AFFECTS
            v
        checkoutservice
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    assert (
        persistence.incident_affects_resource(
            incident.incident_id,
            RESOURCE_ID,
        )
        is True
    )


def test_real_graph_structure(
    neo4j_client,
):
    """
    Directly inspect the Neo4j graph.

    This is the final 5.4 validation.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    result = neo4j_client.execute(
        """
        MATCH
            (i:Incident {
                id: $incident_id
            })
            -[:AFFECTS]->
            (s:Service {
                id: $resource_id
            })

        RETURN
            i.id AS incident_id,
            i.case_id AS case_id,
            i.severity AS severity,
            i.incident_type AS incident_type,
            s.id AS resource_id
        """,
        {
            "incident_id": incident.incident_id,
            "resource_id": RESOURCE_ID,
        },
    )

    assert len(result) == 1

    row = result[0]

    assert (
        row["incident_id"]
        == incident.incident_id
    )

    assert (
        row["case_id"]
        == CASE_ID
    )

    assert (
        row["resource_id"]
        == RESOURCE_ID
    )

    assert row["severity"] is not None

    assert (
        row["incident_type"]
        is not None
    )

def test_real_incident_persistence_is_idempotent(
    neo4j_client,
):
    """
    Persisting the same Incident twice must not create
    duplicate Incident nodes.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    persistence.create_incident(
        incident
    )

    persistence.create_incident(
        incident
    )

    result = neo4j_client.execute(
        """
        MATCH (i:Incident {
            id: $incident_id
        })
        RETURN count(i) AS count
        """,
        {
            "incident_id": incident.incident_id,
        },
    )

    assert len(result) == 1

    assert result[0]["count"] == 1