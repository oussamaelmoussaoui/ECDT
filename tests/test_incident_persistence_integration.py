"""
ECDT - Phase 5.4
Real Neo4j integration test for Observer incident persistence.

Requires:
    - Neo4j running through Docker Compose
    - ECDT Neo4j schema initialized
    - checkoutservice Service node already present
"""

import os
from dataclasses import replace

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
            id: $resource_id,
            source: 'topology'
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
    Repeated persistence must keep one node and one AFFECTS edge.
    """

    anomaly = make_real_anomaly()

    incident = build_incident(
        anomaly
    )

    persistence = IncidentPersistence(
        neo4j_client
    )

    ground_truth_before = neo4j_client.execute(
        """
        MATCH (i:Incident {
            source: 'RCAEVAL_GROUND_TRUTH'
        })
        RETURN count(i) AS count
        """
    )[0]["count"]

    persistence.persist_incident(incident)
    persistence.persist_incident(incident)

    result = neo4j_client.execute(
        """
        MATCH (i:Incident {
            id: $incident_id,
            source: 'ECDT_OBSERVER'
        })
        OPTIONAL MATCH
            (i)-[r:AFFECTS]->(s:Service {
                source: 'topology'
            })
        RETURN
            count(DISTINCT i) AS incident_count,
            count(r) AS affects_count,
            i.case_id AS case_id,
            i.created_at IS NOT NULL AS has_created_at,
            i.pipeline_version AS pipeline_version
        """,
        {
            "incident_id": incident.incident_id,
        },
    )

    assert len(result) == 1

    row = result[0]

    assert row["incident_count"] == 1
    assert row["affects_count"] == 1
    assert row["case_id"] == CASE_ID
    assert row["has_created_at"] is True
    assert row["pipeline_version"]

    ground_truth_after = neo4j_client.execute(
        """
        MATCH (i:Incident {
            source: 'RCAEVAL_GROUND_TRUTH'
        })
        RETURN count(i) AS count
        """
    )[0]["count"]

    assert ground_truth_after == ground_truth_before


def test_unknown_service_is_not_created_implicitly(
    neo4j_client,
):
    """Observer persistence must reject resources outside topology."""

    unknown_service = "service_outside_operational_topology"
    anomaly = replace(
        make_real_anomaly(),
        event_id="integration_event_unknown_service",
        resource_id=unknown_service,
    )
    incident = build_incident(anomaly)
    persistence = IncidentPersistence(neo4j_client)

    with pytest.raises(ValueError, match="operational topology"):
        persistence.persist_incident(incident)

    result = neo4j_client.execute(
        """
        OPTIONAL MATCH (s:Service {id: $service_id})
        OPTIONAL MATCH (i:Incident {id: $incident_id})
        RETURN count(s) AS services, count(i) AS incidents
        """,
        {
            "service_id": unknown_service,
            "incident_id": incident.incident_id,
        },
    )

    assert result == [
        {
            "services": 0,
            "incidents": 0,
        }
    ]


def test_ground_truth_only_service_is_not_operational(
    neo4j_client,
):
    """An evaluation-only Service must not become an Observer target."""

    rows = neo4j_client.execute(
        """
        MATCH (s:Service {source: 'ground_truth'})
        RETURN s.id AS service_id
        ORDER BY service_id
        LIMIT 1
        """
    )

    if not rows:
        pytest.skip("No ground-truth-only Service exists in this graph.")

    service_id = rows[0]["service_id"]
    anomaly = replace(
        make_real_anomaly(),
        event_id="integration_event_ground_truth_service",
        resource_id=service_id,
    )
    incident = build_incident(anomaly)

    with pytest.raises(ValueError, match="operational topology"):
        IncidentPersistence(neo4j_client).persist_incident(incident)

    incident_rows = neo4j_client.execute(
        """
        MATCH (i:Incident {id: $incident_id})
        RETURN count(i) AS count
        """,
        {
            "incident_id": incident.incident_id,
        },
    )

    assert incident_rows[0]["count"] == 0
