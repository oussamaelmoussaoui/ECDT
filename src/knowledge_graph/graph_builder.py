"""
ECDT - Knowledge Graph Builder

Builds the ECDT Knowledge Graph in Neo4j from the validated Phase 1 data.

Input files:
    data/processed/topology/services.csv
    data/processed/topology/dependencies.csv
    data/ground_truth/target_incidents.csv

Current graph population:

    (:Service)
    (:Incident)

    (:Service)-[:DEPENDS_ON]->(:Service)
    (:Incident)-[:CAUSED_BY]->(:Service)

Expected Phase 1 topology:
    39 services
    64 dependencies

Expected ECDT ground truth:
    60 incidents

This module is intentionally responsible for graph population only.

Schema management belongs to:
    graph_schema.py

Neo4j connectivity belongs to:
    neo4j_client.py

Graph queries belong to:
    graph_queries.py
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.knowledge_graph.neo4j_client import Neo4jClient


GRAPH_BUILDER_VERSION = "phase3-structural-v1"
GROUND_TRUTH_SOURCE = "RCAEVAL_GROUND_TRUTH"
TOPOLOGY_SOURCE = "topology"


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SERVICES_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "topology"
    / "services.csv"
)

DEPENDENCIES_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "topology"
    / "dependencies.csv"
)

GROUND_TRUTH_PATH = (
    PROJECT_ROOT
    / "data"
    / "ground_truth"
    / "target_incidents.csv"
)


# ---------------------------------------------------------------------------
# Expected schema
# ---------------------------------------------------------------------------

REQUIRED_SERVICE_COLUMNS = {
    "service",
}

REQUIRED_DEPENDENCY_COLUMNS = {
    "source_service",
    "target_service",
}

REQUIRED_INCIDENT_COLUMNS = {
    "case",
    "dataset",
    "fault",
    "root_cause_service",
}


# ---------------------------------------------------------------------------
# CSV utilities
# ---------------------------------------------------------------------------


def read_csv(path: Path) -> list[dict[str, str]]:
    """
    Read a CSV file into a list of dictionaries.

    Parameters:
        path:
            CSV file path.

    Returns:
        List of rows.

    Raises:
        FileNotFoundError:
            If the CSV does not exist.

        ValueError:
            If the CSV is empty.
    """

    if not path.exists():
        raise FileNotFoundError(
            f"Required CSV file not found: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(
                f"CSV file has no header: {path}"
            )

        rows = list(reader)

    return rows


def validate_columns(
    rows: list[dict[str, str]],
    required_columns: set[str],
    source_name: str,
) -> None:
    """
    Validate required CSV columns.

    Parameters:
        rows:
            CSV rows.

        required_columns:
            Required column names.

        source_name:
            Human-readable source name.
    """

    if not rows:
        raise ValueError(
            f"{source_name} is empty."
        )

    actual_columns = set(rows[0].keys())

    missing = required_columns - actual_columns

    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: "
            f"{sorted(missing)}"
        )


def clean_value(value: Any) -> str:
    """
    Normalize a CSV value into a clean string.
    """

    if value is None:
        return ""

    return str(value).strip()


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------


def load_services() -> list[dict[str, str]]:
    """
    Load the validated service topology.

    Returns:
        Service rows.
    """

    rows = read_csv(SERVICES_PATH)

    validate_columns(
        rows,
        REQUIRED_SERVICE_COLUMNS,
        "services.csv",
    )

    cleaned = []

    for row in rows:
        service = clean_value(row.get("service"))

        if not service:
            continue

        cleaned.append(
            {
                "service": service,
            }
        )

    return cleaned


def load_dependencies() -> list[dict[str, str]]:
    """
    Load the validated service dependencies.

    Each dependency represents:

        source_service -> target_service
    """

    rows = read_csv(DEPENDENCIES_PATH)

    validate_columns(
        rows,
        REQUIRED_DEPENDENCY_COLUMNS,
        "dependencies.csv",
    )

    cleaned = []

    for row in rows:
        source = clean_value(
            row.get("source_service")
        )

        target = clean_value(
            row.get("target_service")
        )

        if not source or not target:
            continue

        cleaned.append(
            {
                "source_service": source,
                "target_service": target,
            }
        )

    return cleaned


def load_incidents() -> list[dict[str, str]]:
    """
    Load the ECDT ground truth incidents.
    """

    rows = read_csv(GROUND_TRUTH_PATH)

    validate_columns(
        rows,
        REQUIRED_INCIDENT_COLUMNS,
        "target_incidents.csv",
    )

    cleaned = []

    for row in rows:
        case = clean_value(row.get("case"))
        dataset = clean_value(row.get("dataset"))
        fault = clean_value(row.get("fault"))
        root_cause_service = clean_value(
            row.get("root_cause_service")
        )

        if not case:
            continue

        cleaned.append(
            {
                "case": case,
                "dataset": dataset,
                "fault": fault,
                "root_cause_service": root_cause_service,
                "fault_description": clean_value(
                    row.get("fault_description")
                ),
                "suite": clean_value(
                    row.get("suite")
                ),
                "system_name": clean_value(
                    row.get("system_name")
                ),
                "inject_time": clean_value(
                    row.get("inject_time")
                ),
                "time_start": clean_value(
                    row.get("time_start")
                ),
                "time_end": clean_value(
                    row.get("time_end")
                ),
                "duration_minutes": clean_value(
                    row.get("duration_minutes")
                ),
            }
        )

    return cleaned


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_input_data(
    services: list[dict[str, str]],
    dependencies: list[dict[str, str]],
    incidents: list[dict[str, str]],
) -> None:
    """
    Validate the Phase 1 graph inputs before writing to Neo4j.
    """

    print("\n[VALIDATION] Input data")

    print(
        f"  Services      : {len(services)}"
    )

    print(
        f"  Dependencies  : {len(dependencies)}"
    )

    print(
        f"  Incidents     : {len(incidents)}"
    )

    if not services:
        raise ValueError(
            "No services found."
        )

    if not dependencies:
        raise ValueError(
            "No dependencies found."
        )

    if not incidents:
        raise ValueError(
            "No incidents found."
        )

    service_ids = {
        row["service"]
        for row in services
    }

    dependency_services = set()

    for dependency in dependencies:
        dependency_services.add(
            dependency["source_service"]
        )

        dependency_services.add(
            dependency["target_service"]
        )

    missing_services = (
        dependency_services - service_ids
    )

    if missing_services:
        raise ValueError(
            "Dependencies reference services "
            "that are absent from services.csv: "
            f"{sorted(missing_services)}"
        )

    incident_root_causes = {
        row["root_cause_service"]
        for row in incidents
        if row["root_cause_service"]
    }

    missing_root_causes = (
        incident_root_causes - service_ids
    )

    if missing_root_causes:
        print(
            "[WARNING] Ground truth contains root-cause "
            "services absent from topology:"
        )

        for service in sorted(missing_root_causes):
            print(f"  - {service}")

        print(
            "[INFO] These services will be created as "
            "Service nodes without inferred dependencies."
        )

    print("  Input validation: SUCCESS")


# ---------------------------------------------------------------------------
# Neo4j population
# ---------------------------------------------------------------------------


def create_services(
    client: Neo4jClient,
    services: list[dict[str, str]],
    incidents: list[dict[str, str]],
) -> int:
    """
    Create all Service nodes required by the Knowledge Graph.

    Services come from two sources:

        1. services.csv
           -> services observed in the extracted topology

        2. target_incidents.csv
           -> root-cause services from ground truth

    A service coming only from ground truth is created as a Service node,
    but no topology relationship is inferred for it.
    """

    service_ids = {
        row["service"]
        for row in services
        if row["service"]
    }

    topology_services = set(service_ids)

    for incident in incidents:
        root_cause = incident.get(
            "root_cause_service",
            "",
        ).strip()

        if root_cause:
            service_ids.add(root_cause)

    service_rows = [
        {
            "service": service,
        }
        for service in sorted(service_ids)
    ]

    query = """
    MERGE (s:Service {id: $service})
    SET
        s.name = $service,
        s.source =
            CASE
                WHEN $service IN $topology_services
                THEN $topology_source
                ELSE "ground_truth"
            END,
        s.created_at = coalesce(s.created_at, datetime()),
        s.pipeline_version = $pipeline_version,
        s.updated_at = datetime()
    RETURN s.id AS id
    """

    # The query needs the topology membership for every row.
    parameters = [
        {
            "service": row["service"],
            "topology_services": list(topology_services),
            "topology_source": TOPOLOGY_SOURCE,
            "pipeline_version": GRAPH_BUILDER_VERSION,
        }
        for row in service_rows
    ]

    count = client.execute_many(
        query,
        parameters,
    )

    print(
        f"[GRAPH] Services created/verified: {count}"
    )

    print(
        f"[GRAPH] Services from topology: "
        f"{len(topology_services)}"
    )

    ground_truth_only = (
        service_ids - topology_services
    )

    print(
        f"[GRAPH] Ground-truth-only services: "
        f"{len(ground_truth_only)}"
    )

    if ground_truth_only:
        print(
            "  " +
            ", ".join(sorted(ground_truth_only))
        )

    return count

def create_dependencies(
    client: Neo4jClient,
    dependencies: list[dict[str, str]],
) -> int:
    """
    Create DEPENDS_ON relationships.

    Direction:

        source_service
            |
            | DEPENDS_ON
            v
        target_service
    """

    query = """
    MATCH (source:Service {
        id: $source_service,
        source: $topology_source
    })
    MATCH (target:Service {
        id: $target_service,
        source: $topology_source
    })
    MERGE (source)-[r:DEPENDS_ON]->(target)
    SET
        r.created_at = coalesce(r.created_at, datetime()),
        r.source = $relationship_source,
        r.pipeline_version = $pipeline_version,
        r.updated_at = datetime()
    """

    count = client.execute_many(
        query,
        [
            {
                **dependency,
                "relationship_source": "TOPOLOGY_EXTRACTOR",
                "topology_source": TOPOLOGY_SOURCE,
                "pipeline_version": GRAPH_BUILDER_VERSION,
            }
            for dependency in dependencies
        ],
    )

    print(
        f"[GRAPH] Dependencies created/verified: {count}"
    )

    return count


def create_incidents(
    client: Neo4jClient,
    incidents: list[dict[str, str]],
) -> int:
    """
    Create Incident nodes.

    The incident case identifier is used as the
    unique Neo4j node id.
    """

    query = """
    MERGE (i:Incident {id: $case})
    SET
        i.source = $incident_source,
        i.case_id = $case,
        i.case = $case,
        i.dataset = $dataset,
        i.fault = $fault,
        i.fault_description = $fault_description,
        i.suite = $suite,
        i.system_name = $system_name,
        i.inject_time = $inject_time,
        i.time_start = $time_start,
        i.time_end = $time_end,
        i.duration_minutes = $duration_minutes,
        i.created_at = coalesce(i.created_at, datetime()),
        i.pipeline_version = $pipeline_version,
        i.updated_at = datetime()
    """

    count = client.execute_many(
        query,
        [
            {
                **incident,
                "incident_source": GROUND_TRUTH_SOURCE,
                "pipeline_version": GRAPH_BUILDER_VERSION,
            }
            for incident in incidents
        ],
    )

    print(
        f"[GRAPH] Incidents created/verified: {count}"
    )

    return count


def create_root_cause_relationships(
    client: Neo4jClient,
    incidents: list[dict[str, str]],
) -> int:
    """
    Create CAUSED_BY relationships.

    Direction:

        Incident
            |
            | CAUSED_BY
            v
        Service
    """

    relationships = [
        {
            "case": incident["case"],
            "root_cause_service": incident[
                "root_cause_service"
            ],
            "relationship_source": GROUND_TRUTH_SOURCE,
            "pipeline_version": GRAPH_BUILDER_VERSION,
        }
        for incident in incidents
        if incident["root_cause_service"]
    ]

    query = """
    MATCH (i:Incident {
        id: $case,
        source: $relationship_source
    })
    MATCH (s:Service {id: $root_cause_service})
    MERGE (i)-[r:CAUSED_BY]->(s)
    SET
        r.created_at = coalesce(r.created_at, datetime()),
        r.source = $relationship_source,
        r.pipeline_version = $pipeline_version,
        r.updated_at = datetime()
    """

    count = client.execute_many(
        query,
        relationships,
    )

    print(
        f"[GRAPH] CAUSED_BY relationships "
        f"created/verified: {count}"
    )

    return count


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def get_graph_statistics(
    client: Neo4jClient,
) -> dict[str, int]:
    """
    Retrieve basic Knowledge Graph statistics.
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
        "ground_truth_incidents": """
            MATCH (i:Incident {
                source: 'RCAEVAL_GROUND_TRUTH'
            })
            RETURN count(i) AS count
        """,

        "observer_incidents": """
            MATCH (i:Incident {
                source: 'ECDT_OBSERVER'
            })
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
        "affects": """
            MATCH (:Incident)-[r:AFFECTS]->(:Service)
            RETURN count(r) AS count
        """,
        "ground_truth_provenance_complete": """
            MATCH (i:Incident {
                source: 'RCAEVAL_GROUND_TRUTH'
            })
            WHERE i.case_id IS NOT NULL
              AND i.created_at IS NOT NULL
              AND i.pipeline_version IS NOT NULL
            RETURN count(i) AS count
        """,
        "duplicate_affects": """
            MATCH (i:Incident)-[r:AFFECTS]->(s:Service)
            WITH i, s, count(r) AS copies
            WHERE copies > 1
            RETURN coalesce(sum(copies - 1), 0) AS count
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


def verify_graph(
    client: Neo4jClient,
) -> bool:
    """
    Verify the populated Knowledge Graph.

    Returns:
        True if the basic graph structure is valid.
    """

    print("\n[VERIFICATION] Knowledge Graph")

    stats = get_graph_statistics(client)

    for name, count in stats.items():
        print(
            f"  {name:<15}: {count}"
        )

    expected = {
        "services": 39,
        "dependencies": 64,
        "ground_truth_incidents": 60,
        "ground_truth_provenance_complete": 60,
        "caused_by": 60,
        "duplicate_affects": 0,
    }

    success = True

    for key, expected_value in expected.items():
        actual_value = stats[key]

        if actual_value != expected_value:
            print(
                f"[WARNING] {key}: expected "
                f"{expected_value}, got {actual_value}"
            )
            success = False

    if success:
        print(
            "\n[VERIFICATION] Graph validation: SUCCESS"
        )
    else:
        print(
            "\n[VERIFICATION] Graph validation: FAILED"
        )

    return success


# ---------------------------------------------------------------------------
# Full build
# ---------------------------------------------------------------------------


def build_graph() -> bool:
    """
    Execute the complete ECDT Knowledge Graph build.

    Steps:
        1. Load services.
        2. Load dependencies.
        3. Load ground truth incidents.
        4. Validate inputs.
        5. Connect to Neo4j.
        6. Create services.
        7. Create dependencies.
        8. Create incidents.
        9. Create CAUSED_BY relationships.
        10. Verify final graph.
    """

    print("=" * 70)
    print("ECDT - Knowledge Graph Builder")
    print("=" * 70)

    print("\n[1/6] Loading Phase 1 data...")

    services = load_services()
    dependencies = load_dependencies()
    incidents = load_incidents()

    print(
        f"  Loaded services      : {len(services)}"
    )

    print(
        f"  Loaded dependencies  : {len(dependencies)}"
    )

    print(
        f"  Loaded incidents     : {len(incidents)}"
    )

    print("\n[2/6] Validating input data...")

    validate_input_data(
        services,
        dependencies,
        incidents,
    )

    print("\n[3/6] Connecting to Neo4j...")

    with Neo4jClient() as client:

        client.verify_connectivity()

        print(
            "  Neo4j connection: SUCCESS"
        )

        print(
            "\n[4/6] Creating Service nodes..."
        )

        create_services(
            client,
            services,
            incidents,
        )
        print(
            "\n[5/6] Creating topology and incidents..."
        )

        create_dependencies(
            client,
            dependencies,
        )

        create_incidents(
            client,
            incidents,
        )

        create_root_cause_relationships(
            client,
            incidents,
        )

        print(
            "\n[6/6] Verifying Knowledge Graph..."
        )

        return verify_graph(client)


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    try:
        success = build_graph()

        print("\n" + "=" * 70)

        if success:
            print(
                "ECDT Knowledge Graph build: SUCCESS"
            )
        else:
            print(
                "ECDT Knowledge Graph build: "
                "COMPLETED WITH VALIDATION WARNINGS"
            )

        print("=" * 70)

        if not success:
            raise SystemExit(1)

    except Exception as exc:
        print("\n" + "=" * 70)
        print(
            "ECDT Knowledge Graph build: FAILED"
        )
        print("=" * 70)
        print(f"Error: {exc}")

        raise
