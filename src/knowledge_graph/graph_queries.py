"""
ECDT - Knowledge Graph Queries

Provides reusable Cypher queries for exploring the ECDT Knowledge Graph.

Graph model:

    (:Service)-[:DEPENDS_ON]->(:Service)
    (:Incident)-[:CAUSED_BY]->(:Service)

Main capabilities:
    - Find upstream dependencies.
    - Find downstream impacts.
    - Find the root cause of an incident.
    - Find incidents caused by a service.
    - Explore the neighborhood of a service.
    - Retrieve basic graph statistics.

This module does not modify the graph.
It only executes read-only Cypher queries.
"""

from __future__ import annotations

from typing import Any

from src.knowledge_graph.neo4j_client import Neo4jClient


# ---------------------------------------------------------------------------
# Basic service queries
# ---------------------------------------------------------------------------


def get_service(
    client: Neo4jClient,
    service_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve a service by its identifier.

    Parameters:
        client:
            Active Neo4jClient instance.

        service_id:
            Service identifier.

    Returns:
        Matching service records.
    """

    query = """
    MATCH (s:Service {id: $service_id})
    RETURN
        s.id AS id,
        s.name AS name,
        s.source AS source
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Upstream dependencies
# ---------------------------------------------------------------------------


def get_upstream_dependencies(
    client: Neo4jClient,
    service_id: str,
    max_depth: int = 5,
) -> list[dict[str, Any]]:
    """
    Find services upstream of a given service.

    Example:

        frontend
            |
        DEPENDS_ON
            v
        checkout
            |
        DEPENDS_ON
            v
        payment

    For a target service, this query traverses the graph
    in the reverse direction of DEPENDS_ON.

    Parameters:
        client:
            Active Neo4jClient instance.

        service_id:
            Target service.

        max_depth:
            Maximum traversal depth.

    Returns:
        Upstream services with their traversal depth.
    """

    if max_depth < 1:
        raise ValueError(
            "max_depth must be >= 1"
        )

    query = f"""
    MATCH (target:Service {{id: $service_id}})

    MATCH path =
        (upstream:Service)
        <-[:DEPENDS_ON*1..{max_depth}]-
        (target)

    WITH
        upstream,
        min(length(path)) AS depth

    RETURN
        upstream.id AS service,
        depth

    ORDER BY depth, service
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Downstream impacts
# ---------------------------------------------------------------------------


def get_downstream_impacts(
    client: Neo4jClient,
    service_id: str,
    max_depth: int = 5,
) -> list[dict[str, Any]]:
    """
    Find services downstream of a given service.

    Because the graph stores:

        source -[:DEPENDS_ON]-> target

    downstream services are found by following
    DEPENDS_ON in the forward direction.

    Parameters:
        client:
            Active Neo4jClient instance.

        service_id:
            Source service.

        max_depth:
            Maximum traversal depth.

    Returns:
        Downstream services with their traversal depth.
    """

    if max_depth < 1:
        raise ValueError(
            "max_depth must be >= 1"
        )

    query = f"""
    MATCH (source:Service {{id: $service_id}})

    MATCH path =
        (source)
        -[:DEPENDS_ON*1..{max_depth}]->
        (downstream:Service)

    WITH
        downstream,
        min(length(path)) AS depth

    RETURN
        downstream.id AS service,
        depth

    ORDER BY depth, service
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Direct dependencies
# ---------------------------------------------------------------------------


def get_direct_dependencies(
    client: Neo4jClient,
    service_id: str,
) -> list[dict[str, Any]]:
    """
    Return direct dependencies of a service.

    Example:

        checkoutservice
              |
          DEPENDS_ON
              v
        paymentservice
    """

    query = """
    MATCH
        (source:Service {id: $service_id})
        -[:DEPENDS_ON]->
        (target:Service)

    RETURN
        target.id AS service

    ORDER BY service
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Direct dependents
# ---------------------------------------------------------------------------


def get_direct_dependents(
    client: Neo4jClient,
    service_id: str,
) -> list[dict[str, Any]]:
    """
    Return services that directly depend on a service.

    Example:

        frontendservice
              |
          DEPENDS_ON
              v
        checkoutservice

    For checkoutservice, frontendservice is a direct dependent.
    """

    query = """
    MATCH
        (dependent:Service)
        -[:DEPENDS_ON]->
        (target:Service {id: $service_id})

    RETURN
        dependent.id AS service

    ORDER BY service
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Incident root cause
# ---------------------------------------------------------------------------


def get_incident_root_cause(
    client: Neo4jClient,
    incident_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve the ground-truth root cause service of an incident.

    Graph pattern:

        (:Incident)-[:CAUSED_BY]->(:Service)
    """

    query = """
    MATCH
        (i:Incident {id: $incident_id})
        -[:CAUSED_BY]->
        (s:Service)

    RETURN
        i.id AS incident,
        s.id AS root_cause
    """

    return client.execute(
        query,
        {
            "incident_id": incident_id,
        },
    )


# ---------------------------------------------------------------------------
# Incidents associated with a service
# ---------------------------------------------------------------------------


def get_service_incidents(
    client: Neo4jClient,
    service_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve incidents whose ground-truth root cause is a given service.
    """

    query = """
    MATCH
        (i:Incident)
        -[:CAUSED_BY]->
        (s:Service {id: $service_id})

    RETURN
        i.id AS incident,
        i.dataset AS dataset,
        i.fault AS fault,
        i.fault_description AS fault_description,
        i.suite AS suite,
        i.system_name AS system_name

    ORDER BY incident
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Incident information
# ---------------------------------------------------------------------------


def get_incident(
    client: Neo4jClient,
    incident_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve complete information about an incident.
    """

    query = """
    MATCH (i:Incident {id: $incident_id})

    OPTIONAL MATCH
        (i)-[:CAUSED_BY]->(s:Service)

    RETURN
        i.id AS incident,
        i.dataset AS dataset,
        i.fault AS fault,
        i.fault_description AS fault_description,
        i.suite AS suite,
        i.system_name AS system_name,
        i.inject_time AS inject_time,
        i.time_start AS time_start,
        i.time_end AS time_end,
        i.duration_minutes AS duration_minutes,
        s.id AS root_cause
    """

    return client.execute(
        query,
        {
            "incident_id": incident_id,
        },
    )


# ---------------------------------------------------------------------------
# Service neighborhood
# ---------------------------------------------------------------------------


def get_service_neighborhood(
    client: Neo4jClient,
    service_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve the direct graph neighborhood of a service.

    Includes:
        - services it depends on
        - services depending on it
    """

    query = """
    MATCH (s:Service {id: $service_id})

    OPTIONAL MATCH
        (s)-[:DEPENDS_ON]->(dependency:Service)

    OPTIONAL MATCH
        (dependent:Service)-[:DEPENDS_ON]->(s)

    RETURN
        s.id AS service,
        collect(DISTINCT dependency.id) AS dependencies,
        collect(DISTINCT dependent.id) AS dependents
    """

    return client.execute(
        query,
        {
            "service_id": service_id,
        },
    )


# ---------------------------------------------------------------------------
# Graph statistics
# ---------------------------------------------------------------------------


def get_graph_statistics(
    client: Neo4jClient,
) -> dict[str, int]:
    """
    Retrieve basic ECDT Knowledge Graph statistics.
    """

    queries = {
        "services": """
            MATCH (s:Service)
            RETURN count(s) AS count
        """,
        "incidents": """
            MATCH (i:Incident)
            RETURN count(i) AS count
        """,
        "dependencies": """
            MATCH (:Service)-[r:DEPENDS_ON]->(:Service)
            RETURN count(r) AS count
        """,
        "caused_by": """
            MATCH (:Incident)-[r:CAUSED_BY]->(:Service)
            RETURN count(r) AS count
        """,
    }

    statistics: dict[str, int] = {}

    for name, query in queries.items():
        result = client.execute(query)

        statistics[name] = (
            int(result[0]["count"])
            if result
            else 0
        )

    return statistics


# ---------------------------------------------------------------------------
# Full graph overview
# ---------------------------------------------------------------------------


def get_graph_overview(
    client: Neo4jClient,
) -> dict[str, Any]:
    """
    Return a compact overview of the Knowledge Graph.
    """

    statistics = get_graph_statistics(client)

    return {
        "statistics": statistics,
        "node_types": [
            "Service",
            "Incident",
        ],
        "relationship_types": [
            "DEPENDS_ON",
            "CAUSED_BY",
        ],
    }


# ---------------------------------------------------------------------------
# Standalone execution / demonstration
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Demonstrate the main ECDT graph queries.

    Run:

        python -m src.knowledge_graph.graph_queries
    """

    print("=" * 70)
    print("ECDT - Knowledge Graph Query Test")
    print("=" * 70)

    with Neo4jClient() as client:

        # ---------------------------------------------------------------
        # 1. Connectivity
        # ---------------------------------------------------------------

        print("\n[1] Neo4j connectivity")

        client.verify_connectivity()

        print("  Connection: SUCCESS")

        # ---------------------------------------------------------------
        # 2. Graph statistics
        # ---------------------------------------------------------------

        print("\n[2] Graph statistics")

        statistics = get_graph_statistics(client)

        for name, count in statistics.items():
            print(
                f"  {name:<15}: {count}"
            )

        # ---------------------------------------------------------------
        # 3. Service lookup
        # ---------------------------------------------------------------

        service_id = "checkoutservice"

        print(
            f"\n[3] Service lookup: {service_id}"
        )

        service = get_service(
            client,
            service_id,
        )

        for row in service:
            print(f"  {row}")

        # ---------------------------------------------------------------
        # 4. Direct dependencies
        # ---------------------------------------------------------------

        print(
            f"\n[4] Direct dependencies of {service_id}"
        )

        dependencies = get_direct_dependencies(
            client,
            service_id,
        )

        for row in dependencies:
            print(
                f"  -> {row['service']}"
            )

        # ---------------------------------------------------------------
        # 5. Direct dependents
        # ---------------------------------------------------------------

        print(
            f"\n[5] Direct dependents of {service_id}"
        )

        dependents = get_direct_dependents(
            client,
            service_id,
        )

        for row in dependents:
            print(
                f"  <- {row['service']}"
            )

        # ---------------------------------------------------------------
        # 6. Upstream dependencies
        # ---------------------------------------------------------------

        print(
            f"\n[6] Upstream dependencies of {service_id}"
        )

        upstream = get_upstream_dependencies(
            client,
            service_id,
            max_depth=5,
        )

        for row in upstream:
            print(
                f"  depth={row['depth']} "
                f"service={row['service']}"
            )

        # ---------------------------------------------------------------
        # 7. Downstream impacts
        # ---------------------------------------------------------------

        print(
            f"\n[7] Downstream impacts of {service_id}"
        )

        downstream = get_downstream_impacts(
            client,
            service_id,
            max_depth=5,
        )

        for row in downstream:
            print(
                f"  depth={row['depth']} "
                f"service={row['service']}"
            )

        # ---------------------------------------------------------------
        # 8. Incident root cause
        # ---------------------------------------------------------------

        incident_id = "re2ob_checkoutservice_cpu_1"

        print(
            f"\n[8] Root cause of {incident_id}"
        )

        root_cause = get_incident_root_cause(
            client,
            incident_id,
        )

        for row in root_cause:
            print(
                f"  {row['incident']} "
                f"-> {row['root_cause']}"
            )

        # ---------------------------------------------------------------
        # 9. Incidents caused by a service
        # ---------------------------------------------------------------

        print(
            f"\n[9] Incidents caused by {service_id}"
        )

        incidents = get_service_incidents(
            client,
            service_id,
        )

        for row in incidents:
            print(
                f"  {row['incident']} "
                f"| fault={row['fault']}"
            )

        # ---------------------------------------------------------------
        # 10. Neighborhood
        # ---------------------------------------------------------------

        print(
            f"\n[10] Neighborhood of {service_id}"
        )

        neighborhood = get_service_neighborhood(
            client,
            service_id,
        )

        for row in neighborhood:
            print(f"  {row}")

    print("\n" + "=" * 70)
    print("ECDT Knowledge Graph query test: SUCCESS")
    print("=" * 70)


if __name__ == "__main__":
    main()