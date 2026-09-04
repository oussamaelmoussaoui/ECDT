"""
ECDT - Knowledge Graph Tests

Automated tests for the Neo4j Knowledge Graph.

Expected graph after Phase 3 population:

    Service nodes       : 39
    Incident nodes      : 60
    DEPENDS_ON          : 64
    CAUSED_BY           : 60

Run:

    python -m pytest tests/test_knowledge_graph.py -v

or:

    python -m tests.test_knowledge_graph
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# ECDT imports
# ---------------------------------------------------------------------------

from src.knowledge_graph.neo4j_client import Neo4jClient

from src.knowledge_graph.graph_queries import (
    get_service,
    get_direct_dependencies,
    get_direct_dependents,
    get_upstream_dependencies,
    get_downstream_impacts,
    get_incident_root_cause,
    get_service_incidents,
    get_incident,
    get_service_neighborhood,
    get_graph_statistics,
    get_graph_overview,
)


# ---------------------------------------------------------------------------
# Test configuration
# ---------------------------------------------------------------------------

EXPECTED_SERVICES = 39
EXPECTED_INCIDENTS = 60
EXPECTED_DEPENDENCIES = 64
EXPECTED_CAUSED_BY = 60

REFERENCE_SERVICE = "checkoutservice"
REFERENCE_INCIDENT = "re2ob_checkoutservice_cpu_1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def neo4j_client():
    """
    Create one Neo4j client for the complete test module.

    The connection is opened once and closed after all tests.
    """

    client = Neo4jClient()

    try:
        client.verify_connectivity()
    except Exception as exc:
        client.close()

        pytest.skip(
            f"Neo4j is not available: {exc}"
        )

    yield client

    client.close()


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------


def test_neo4j_connectivity(neo4j_client):
    """
    Test that the application can connect to Neo4j.
    """

    result = neo4j_client.execute(
        """
        RETURN 1 AS value
        """
    )

    assert result
    assert result[0]["value"] == 1


# ---------------------------------------------------------------------------
# Graph statistics
# ---------------------------------------------------------------------------


def test_graph_statistics(neo4j_client):
    """
    Verify the expected global graph structure.
    """

    stats = get_graph_statistics(
        neo4j_client
    )

    assert stats["services"] == EXPECTED_SERVICES

    assert stats["ground_truth_incidents"] == EXPECTED_INCIDENTS

    assert stats["incidents"] == (
        stats["ground_truth_incidents"]
        + stats["observer_incidents"]
    )

    assert stats["dependencies"] == EXPECTED_DEPENDENCIES

    assert stats["caused_by"] == EXPECTED_CAUSED_BY


# ---------------------------------------------------------------------------
# Service lookup
# ---------------------------------------------------------------------------


def test_get_service(neo4j_client):
    """
    Verify that a known service can be retrieved.
    """

    result = get_service(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    assert len(result) == 1

    assert result[0]["id"] == REFERENCE_SERVICE


def test_get_unknown_service(neo4j_client):
    """
    An unknown service should return no records.
    """

    result = get_service(
        neo4j_client,
        "service_that_does_not_exist",
    )

    assert result == []


# ---------------------------------------------------------------------------
# Direct dependencies
# ---------------------------------------------------------------------------


def test_direct_dependencies(neo4j_client):
    """
    Verify direct DEPENDS_ON traversal.
    """

    result = get_direct_dependencies(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    assert isinstance(result, list)

    services = {
        row["service"]
        for row in result
    }

    # The exact dependency set is validated against Neo4j.
    assert len(services) == len(result)


# ---------------------------------------------------------------------------
# Direct dependents
# ---------------------------------------------------------------------------


def test_direct_dependents(neo4j_client):
    """
    Verify reverse traversal of DEPENDS_ON.
    """

    result = get_direct_dependents(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    assert isinstance(result, list)

    services = {
        row["service"]
        for row in result
    }

    assert len(services) == len(result)


# ---------------------------------------------------------------------------
# Upstream dependencies
# ---------------------------------------------------------------------------


def test_upstream_dependencies(neo4j_client):
    """
    Verify recursive upstream traversal.
    """

    result = get_upstream_dependencies(
        neo4j_client,
        REFERENCE_SERVICE,
        max_depth=5,
    )

    assert isinstance(result, list)

    for row in result:
        assert "service" in row
        assert "depth" in row

        assert row["service"] != REFERENCE_SERVICE

        assert row["depth"] >= 1
        assert row["depth"] <= 5


def test_upstream_dependencies_depth_validation(
    neo4j_client,
):
    """
    max_depth must be >= 1.
    """

    with pytest.raises(ValueError):
        get_upstream_dependencies(
            neo4j_client,
            REFERENCE_SERVICE,
            max_depth=0,
        )


# ---------------------------------------------------------------------------
# Downstream impacts
# ---------------------------------------------------------------------------


def test_downstream_impacts(neo4j_client):
    """
    Verify recursive downstream traversal.
    """

    result = get_downstream_impacts(
        neo4j_client,
        REFERENCE_SERVICE,
        max_depth=5,
    )

    assert isinstance(result, list)

    for row in result:
        assert "service" in row
        assert "depth" in row

        assert row["service"] != REFERENCE_SERVICE

        assert row["depth"] >= 1
        assert row["depth"] <= 5


def test_downstream_impacts_depth_validation(
    neo4j_client,
):
    """
    max_depth must be >= 1.
    """

    with pytest.raises(ValueError):
        get_downstream_impacts(
            neo4j_client,
            REFERENCE_SERVICE,
            max_depth=0,
        )


# ---------------------------------------------------------------------------
# Incident root cause
# ---------------------------------------------------------------------------


def test_incident_root_cause(neo4j_client):
    """
    Verify the ground-truth root cause relation.

    Reference case:

        re2ob_checkoutservice_cpu_1
                    |
                CAUSED_BY
                    |
                    v
              checkoutservice
    """

    result = get_incident_root_cause(
        neo4j_client,
        REFERENCE_INCIDENT,
    )

    assert len(result) == 1

    assert (
        result[0]["incident"]
        == REFERENCE_INCIDENT
    )

    assert (
        result[0]["root_cause"]
        == REFERENCE_SERVICE
    )


def test_unknown_incident_root_cause(
    neo4j_client,
):
    """
    Unknown incidents should return no root cause.
    """

    result = get_incident_root_cause(
        neo4j_client,
        "incident_that_does_not_exist",
    )

    assert result == []


# ---------------------------------------------------------------------------
# Incident lookup
# ---------------------------------------------------------------------------


def test_get_incident(neo4j_client):
    """
    Verify complete incident retrieval.
    """

    result = get_incident(
        neo4j_client,
        REFERENCE_INCIDENT,
    )

    assert len(result) == 1

    incident = result[0]

    assert (
        incident["incident"]
        == REFERENCE_INCIDENT
    )

    assert (
        incident["root_cause"]
        == REFERENCE_SERVICE
    )

    assert incident["fault"]


# ---------------------------------------------------------------------------
# Service incidents
# ---------------------------------------------------------------------------


def test_service_incidents(neo4j_client):
    """
    Verify retrieval of incidents caused by a service.
    """

    result = get_service_incidents(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    assert isinstance(result, list)

    for row in result:
        assert "incident" in row
        assert "fault" in row

        assert row["incident"]


def test_service_without_incidents(
    neo4j_client,
):
    """
    A valid service may have no ground-truth incidents.
    """

    result = get_service_incidents(
        neo4j_client,
        "frontendservice",
    )

    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Neighborhood
# ---------------------------------------------------------------------------


def test_service_neighborhood(neo4j_client):
    """
    Verify direct topology neighborhood retrieval.
    """

    result = get_service_neighborhood(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    assert len(result) == 1

    neighborhood = result[0]

    assert (
        neighborhood["service"]
        == REFERENCE_SERVICE
    )

    assert isinstance(
        neighborhood["dependencies"],
        list,
    )

    assert isinstance(
        neighborhood["dependents"],
        list,
    )


# ---------------------------------------------------------------------------
# Graph overview
# ---------------------------------------------------------------------------


def test_graph_overview(neo4j_client):
    """
    Verify that the graph overview exposes
    the expected node and relationship types.
    """

    overview = get_graph_overview(
        neo4j_client
    )

    assert "statistics" in overview

    stats = overview["statistics"]

    assert (
        stats["services"]
        == EXPECTED_SERVICES
    )

    # Exactly 60 RCAEval evaluation incidents.
    assert (
        stats["ground_truth_incidents"]
        == EXPECTED_INCIDENTS
    )

    # The total includes both RCAEval and Observer incidents.
    assert stats["incidents"] == (
        stats["ground_truth_incidents"]
        + stats["observer_incidents"]
    )

    assert "Service" in overview["node_types"]
    assert "Incident" in overview["node_types"]

    assert (
        "DEPENDS_ON"
        in overview["relationship_types"]
    )

    assert (
        "CAUSED_BY"
        in overview["relationship_types"]
    )


# ---------------------------------------------------------------------------
# Graph consistency
# ---------------------------------------------------------------------------


def test_every_incident_has_root_cause(
    neo4j_client,
):
    """
    Every ground-truth incident should have
    exactly one CAUSED_BY relationship.
    """

    result = neo4j_client.execute(
        """
        MATCH (i:Incident)
        WHERE i.source = 'RCAEVAL_GROUND_TRUTH'
        OPTIONAL MATCH
            (i)-[r:CAUSED_BY]->(s:Service)

        WITH
            i,
            count(r) AS root_cause_count

        RETURN
            i.id AS incident,
            root_cause_count

        ORDER BY incident
        """
    )

    assert len(result) == EXPECTED_INCIDENTS

    for row in result:
        assert row["root_cause_count"] == 1


def test_no_broken_dependency_relationships(
    neo4j_client,
):
    """
    Every DEPENDS_ON relationship must connect
    two existing Service nodes.
    """

    result = neo4j_client.execute(
        """
        MATCH (source)-[r:DEPENDS_ON]->(target)

        RETURN
            count(r) AS total,
            count(
                CASE
                    WHEN source:Service
                     AND target:Service
                    THEN 1
                END
            ) AS valid
        """
    )

    assert len(result) == 1

    row = result[0]

    assert row["total"] == EXPECTED_DEPENDENCIES

    assert row["valid"] == EXPECTED_DEPENDENCIES


def test_no_broken_caused_by_relationships(
    neo4j_client,
):
    """
    Every CAUSED_BY relationship must connect
    an Incident to a Service.
    """

    result = neo4j_client.execute(
        """
        MATCH (source)-[r:CAUSED_BY]->(target)

        RETURN
            count(r) AS total,
            count(
                CASE
                    WHEN source:Incident
                     AND target:Service
                    THEN 1
                END
            ) AS valid
        """
    )

    assert len(result) == 1

    row = result[0]

    assert row["total"] == EXPECTED_CAUSED_BY

    assert row["valid"] == EXPECTED_CAUSED_BY


# ---------------------------------------------------------------------------
# Query read-only behavior
# ---------------------------------------------------------------------------


def test_queries_do_not_create_extra_nodes(
    neo4j_client,
):
    """
    Running read-only graph queries must not modify
    the number of nodes.
    """

    before = get_graph_statistics(
        neo4j_client
    )

    get_service(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_direct_dependencies(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_direct_dependents(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_upstream_dependencies(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_downstream_impacts(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_incident_root_cause(
        neo4j_client,
        REFERENCE_INCIDENT,
    )

    get_service_incidents(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    get_incident(
        neo4j_client,
        REFERENCE_INCIDENT,
    )

    get_service_neighborhood(
        neo4j_client,
        REFERENCE_SERVICE,
    )

    after = get_graph_statistics(
        neo4j_client
    )

    assert before == after


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import pytest

    raise SystemExit(
        pytest.main(
            [
                __file__,
                "-v",
            ]
        )
    )