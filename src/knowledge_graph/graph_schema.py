"""
ECDT - Knowledge Graph Schema

Defines and initializes the Neo4j schema used by ECDT.

Graph entities:
    Service
    Pod
    Database
    Node
    Incident

Graph relationships:
    DEPENDS_ON
    RUNS_ON
    IMPACTS
    CAUSED_BY

The schema is intentionally separated from graph population.

This module is responsible for:
    - Creating uniqueness constraints.
    - Verifying the constraints.
    - Providing a reusable schema initialization function.

It does NOT:
    - Import CSV files.
    - Create the ECDT topology.
    - Populate incidents.
    - Execute RCA queries.

Those responsibilities belong to graph_builder.py and graph_queries.py.
"""

from __future__ import annotations

from typing import Any

from src.knowledge_graph.neo4j_client import Neo4jClient


# ---------------------------------------------------------------------------
# ECDT Knowledge Graph schema
# ---------------------------------------------------------------------------

NODE_LABELS = (
    "Service",
    "Pod",
    "Database",
    "Node",
    "Incident",
)

RELATIONSHIP_TYPES = (
    "DEPENDS_ON",
    "RUNS_ON",
    "IMPACTS",
    "CAUSED_BY",
)


# ---------------------------------------------------------------------------
# Constraints
# ---------------------------------------------------------------------------

CONSTRAINT_QUERIES = {
    "service_id_unique": """
        CREATE CONSTRAINT service_id_unique IF NOT EXISTS
        FOR (s:Service)
        REQUIRE s.id IS UNIQUE
    """,
    "pod_id_unique": """
        CREATE CONSTRAINT pod_id_unique IF NOT EXISTS
        FOR (p:Pod)
        REQUIRE p.id IS UNIQUE
    """,
    "database_id_unique": """
        CREATE CONSTRAINT database_id_unique IF NOT EXISTS
        FOR (d:Database)
        REQUIRE d.id IS UNIQUE
    """,
    "node_id_unique": """
        CREATE CONSTRAINT node_id_unique IF NOT EXISTS
        FOR (n:Node)
        REQUIRE n.id IS UNIQUE
    """,
    "incident_id_unique": """
        CREATE CONSTRAINT incident_id_unique IF NOT EXISTS
        FOR (i:Incident)
        REQUIRE i.id IS UNIQUE
    """,
}


# ---------------------------------------------------------------------------
# Schema initialization
# ---------------------------------------------------------------------------


def create_constraints(client: Neo4jClient) -> None:
    """
    Create all ECDT uniqueness constraints.

    Constraints guarantee that every graph entity has a unique `id`
    within its corresponding label.

    Parameters:
        client:
            Active Neo4jClient instance.
    """

    for constraint_name, query in CONSTRAINT_QUERIES.items():
        client.execute_write(query)

        print(
            f"[SCHEMA] Constraint ready: {constraint_name}"
        )


def get_constraints(client: Neo4jClient) -> list[dict[str, Any]]:
    """
    Retrieve the constraints currently registered in Neo4j.

    Parameters:
        client:
            Active Neo4jClient instance.

    Returns:
        List of Neo4j constraint records.
    """

    return client.execute(
        "SHOW CONSTRAINTS"
    )


def initialize_schema(client: Neo4jClient) -> None:
    """
    Initialize the complete ECDT Knowledge Graph schema.

    This function:
        1. Verifies Neo4j connectivity.
        2. Creates all uniqueness constraints.
        3. Displays the resulting constraints.

    Parameters:
        client:
            Active Neo4jClient instance.
    """

    print("=" * 60)
    print("ECDT - Knowledge Graph Schema Initialization")
    print("=" * 60)

    print("\n[1/3] Checking Neo4j connectivity...")

    client.verify_connectivity()

    print("[OK] Neo4j connection verified.")

    print("\n[2/3] Creating ECDT constraints...")

    create_constraints(client)

    print("\n[3/3] Verifying constraints...")

    constraints = get_constraints(client)

    for constraint in constraints:
        name = constraint.get("name", "unknown")
        print(f"[SCHEMA] {name}")

    print("\n" + "=" * 60)
    print("ECDT schema initialization: SUCCESS")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_schema(client: Neo4jClient) -> bool:
    """
    Validate that all required ECDT constraints exist.

    Parameters:
        client:
            Active Neo4jClient instance.

    Returns:
        True if all expected constraints exist.
        False otherwise.
    """

    constraints = get_constraints(client)

    existing_names = {
        constraint.get("name")
        for constraint in constraints
    }

    expected_names = set(CONSTRAINT_QUERIES.keys())

    missing = expected_names - existing_names

    if missing:
        print("[SCHEMA] Missing constraints:")

        for name in sorted(missing):
            print(f"  - {name}")

        return False

    return True


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    """
    Initialize the ECDT Neo4j schema.

    Run from the ECDT project root:

        python -m src.knowledge_graph.graph_schema
    """

    try:
        with Neo4jClient() as client:
            initialize_schema(client)

            print("\n[VALIDATION] Checking final schema...")

            if validate_schema(client):
                print("[VALIDATION] Schema validation: SUCCESS")
            else:
                print("[VALIDATION] Schema validation: FAILED")

                raise SystemExit(1)

    except Exception as exc:
        print("\n" + "=" * 60)
        print("ECDT schema initialization: FAILED")
        print("=" * 60)
        print(f"Error: {exc}")

        raise