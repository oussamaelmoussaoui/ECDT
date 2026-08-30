"""
ECDT - Phase 5.5
Real ObserverAgent integration tests.

Real pipeline:

    Phase 2 anomaly
          |
          v
    ObserverAgent
          |
          +--------------------+
          |                    |
          v                    v
    TimescaleDB              Neo4j
          |                    |
          v                    v
    TemporalContext         Incident
          |                    |
          +---------+----------+
                    |
                    v
              IncidentContext

Acceptance criterion:

    (:Incident)-[:AFFECTS]->(:Service)

Real RCAEval case:

    re2ob_checkoutservice_cpu_1
"""

from __future__ import annotations

import os

import pytest

from src.agents.observer.observer_agent import (
    IncidentContext,
    ObserverAgent,
)

from src.agents.observer.incident_persistence import (
    IncidentPersistence,
)

from src.agents.observer.timescale_consumer import (
    TimescaleConsumer,
)

from src.digital_twin.timescale_client import (
    TimescaleClient,
)

from src.ingestion.models import (
    AnomalyEvent,
    DetectionMethod,
    IncidentType,
)

from src.knowledge_graph.neo4j_client import (
    Neo4jClient,
)


# =====================================================================
# Real RCAEval case
# =====================================================================

CASE_ID = "re2ob_checkoutservice_cpu_1"

RESOURCE_ID = "checkoutservice"

SIGNAL_TYPE = "cpu"

METRIC_NAME = "checkoutservice_cpu"


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture(scope="module")
def timescale_client():
    """
    Real TimescaleDB client.

    No mock is used.
    """

    if not os.getenv("TIMESCALE_URI"):
        pytest.skip(
            "TIMESCALE_URI is not configured."
        )

    client = TimescaleClient()

    if not client.ping():
        pytest.fail(
            "TimescaleDB is not reachable. "
            "Start it with: "
            "docker compose up -d timescaledb"
        )

    yield client


@pytest.fixture(scope="module")
def neo4j_client():
    """
    Real Neo4j client.

    No mock is used.
    """

    if not os.getenv("NEO4J_URI"):
        pytest.skip(
            "NEO4J_URI is not configured."
        )

    client = Neo4jClient()

    try:
        client.verify_connectivity()

    except Exception as exc:
        pytest.fail(
            "Neo4j is not reachable. "
            "Start it with: "
            "docker compose up -d neo4j"
            f"\nOriginal error: {exc}"
        )

    yield client

    close = getattr(
        client,
        "close",
        None,
    )

    if callable(close):
        close()


@pytest.fixture(scope="module")
def observer(
    timescale_client,
    neo4j_client,
):
    """
    Build the REAL ObserverAgent.

    Both external dependencies are real.
    """

    timescale_consumer = (
        TimescaleConsumer(
            client=timescale_client
        )
    )

    incident_persistence = (
        IncidentPersistence(
            neo4j_client
        )
    )

    return ObserverAgent(
        timescale_consumer=(
            timescale_consumer
        ),
        incident_persistence=(
            incident_persistence
        ),
        window_minutes=5,
    )


# =====================================================================
# Helpers
# =====================================================================


def get_real_observation(
    timescale_client: TimescaleClient,
) -> dict:
    """
    Retrieve the real TimescaleDB observation.

    IMPORTANT:
    TimescaleClient uses execute(..., fetch=True).
    """

    rows = timescale_client.execute(
        """
        SELECT
            timestamp,
            resource_id,
            metric_name,
            metric_type,
            value,
            case_id
        FROM metric_observations
        WHERE case_id = %s
          AND resource_id = %s
          AND metric_name = %s
        ORDER BY timestamp
        LIMIT 1
        """,
        (
            CASE_ID,
            RESOURCE_ID,
            METRIC_NAME,
        ),
        fetch=True,
    )

    if not rows:
        pytest.fail(
            "No TimescaleDB observation found for "
            f"{CASE_ID}."
        )

    return rows[0]


def timestamp_to_unix(timestamp) -> int:
    """
    Convert a PostgreSQL timestamp into Unix seconds.
    """

    if hasattr(
        timestamp,
        "timestamp",
    ):
        return int(
            timestamp.timestamp()
        )

    return int(timestamp)


def build_real_anomaly(
    row: dict,
) -> AnomalyEvent:
    """
    Build an AnomalyEvent from the real TimescaleDB
    observation.

    This represents the Phase 2 anomaly entering
    the Observer.
    """

    return AnomalyEvent(
        event_id=(
            f"observer_"
            f"{CASE_ID}"
        ),

        case_id=CASE_ID,

        timestamp=timestamp_to_unix(
            row["timestamp"]
        ),

        service=RESOURCE_ID,

        signal_type=SIGNAL_TYPE,

        value=float(
            row["value"]
        ),

        detection_method=(
            DetectionMethod.Z_SCORE
        ),

        score=10.0,

        threshold=3.0,

        incident_type=(
            IncidentType.CPU_SATURATION
        ),
    )


# =====================================================================
# 1. REAL TimescaleDB validation
# =====================================================================


def test_real_phase2_observation_exists(
    timescale_client,
):
    """
    Verify that the real Digital Twin data exists
    in TimescaleDB.
    """

    row = get_real_observation(
        timescale_client
    )

    assert row["case_id"] == CASE_ID

    assert (
        row["resource_id"]
        == RESOURCE_ID
    )

    assert (
        row["metric_name"]
        == METRIC_NAME
    )

    assert (
        row["metric_type"]
        == SIGNAL_TYPE
    )

    assert row["timestamp"] is not None

    assert row["value"] is not None


# =====================================================================
# 2. REAL Neo4j resource validation
# =====================================================================


def test_real_service_exists(
    neo4j_client,
):
    """
    Verify that checkoutservice exists in Neo4j.
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

    assert len(result) == 1

    assert (
        result[0]["resource_id"]
        == RESOURCE_ID
    )


# =====================================================================
# 3. REAL Observer execution
# =====================================================================


def test_real_observer_processes_anomaly(
    observer,
    timescale_client,
):
    """
    Execute the complete ObserverAgent against
    real infrastructure.
    """

    observation = get_real_observation(
        timescale_client
    )

    anomaly = build_real_anomaly(
        observation
    )

    context = observer.process(
        anomaly
    )

    assert isinstance(
        context,
        IncidentContext,
    )

    assert context.persisted is True

    assert (
        context.case_id
        == CASE_ID
    )

    assert (
        context.resource_id
        == RESOURCE_ID
    )

    assert context.incident_id

    assert context.incident is not None

    assert (
        context.temporal_context
        is not None
    )

    assert (
        context.qualification
        is not None
    )


# =====================================================================
# 4. REAL Incident in Neo4j
# =====================================================================


def test_real_incident_exists_in_neo4j(
    observer,
    timescale_client,
    neo4j_client,
):
    """
    Verify that the Incident created by the real
    ObserverAgent exists in Neo4j.
    """

    observation = get_real_observation(
        timescale_client
    )

    anomaly = build_real_anomaly(
        observation
    )

    context = observer.process(
        anomaly
    )

    result = neo4j_client.execute(
        """
        MATCH (i:Incident {
            id: $incident_id
        })
        RETURN
            i.id AS incident_id,
            i.case_id AS case_id,
            i.resource_id AS resource_id,
            i.incident_type AS incident_type,
            i.severity AS severity
        """,
        {
            "incident_id": (
                context.incident_id
            )
        },
    )

    assert len(result) == 1

    incident = result[0]

    assert (
        incident["incident_id"]
        == context.incident_id
    )

    assert (
        incident["case_id"]
        == CASE_ID
    )

    assert (
        incident["resource_id"]
        == RESOURCE_ID
    )

    assert (
        incident["incident_type"]
        is not None
    )

    assert (
        incident["severity"]
        is not None
    )


# =====================================================================
# 5. REAL Incident -> Service relationship
# =====================================================================


def test_real_incident_affects_service(
    observer,
    timescale_client,
    neo4j_client,
):
    """
    Verify:

        (:Incident)-[:AFFECTS]->(:Service)
    """

    observation = get_real_observation(
        timescale_client
    )

    anomaly = build_real_anomaly(
        observation
    )

    context = observer.process(
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
            i.resource_id AS incident_resource,
            s.id AS service_id
        """,
        {
            "incident_id": (
                context.incident_id
            ),

            "resource_id": RESOURCE_ID,
        },
    )

    assert len(result) == 1

    relationship = result[0]

    assert (
        relationship["incident_id"]
        == context.incident_id
    )

    assert (
        relationship["case_id"]
        == CASE_ID
    )

    assert (
        relationship["incident_resource"]
        == RESOURCE_ID
    )

    assert (
        relationship["service_id"]
        == RESOURCE_ID
    )


# =====================================================================
# 6. FINAL ACCEPTANCE TEST
# =====================================================================


def test_observer_phase_5_5_acceptance(
    observer,
    timescale_client,
    neo4j_client,
):
    """
    FINAL ACCEPTANCE TEST FOR PHASE 5.5.

    Real pipeline:

        TimescaleDB
             |
             v
        AnomalyEvent
             |
             v
        ObserverAgent
             |
             v
        TemporalContext
             |
             v
          Incident
             |
             v
          Neo4j
             |
             v
    (:Incident)-[:AFFECTS]->(:Service)
    """

    # --------------------------------------------------------------
    # Real anomaly
    # --------------------------------------------------------------

    observation = get_real_observation(
        timescale_client
    )

    anomaly = build_real_anomaly(
        observation
    )

    # --------------------------------------------------------------
    # Observer
    # --------------------------------------------------------------

    context = observer.process(
        anomaly
    )

    # --------------------------------------------------------------
    # Observer output
    # --------------------------------------------------------------

    assert context.persisted is True

    assert (
        context.case_id
        == CASE_ID
    )

    assert (
        context.resource_id
        == RESOURCE_ID
    )

    assert context.incident_id

    # --------------------------------------------------------------
    # Final graph verification
    # --------------------------------------------------------------

    result = neo4j_client.execute(
        """
        MATCH
            (i:Incident)
            -[r:AFFECTS]->
            (s:Service)

        WHERE
            i.id = $incident_id
            AND i.case_id = $case_id
            AND i.resource_id = $resource_id
            AND s.id = $resource_id

        RETURN
            i.id AS incident_id,
            i.case_id AS case_id,
            i.resource_id AS resource_id,
            type(r) AS relationship,
            s.id AS service_id
        """,
        {
            "incident_id": (
                context.incident_id
            ),

            "case_id": CASE_ID,

            "resource_id": RESOURCE_ID,
        },
    )

    assert len(result) == 1

    graph = result[0]

    assert (
        graph["incident_id"]
        == context.incident_id
    )

    assert (
        graph["case_id"]
        == CASE_ID
    )

    assert (
        graph["resource_id"]
        == RESOURCE_ID
    )

    assert (
        graph["relationship"]
        == "AFFECTS"
    )

    assert (
        graph["service_id"]
        == RESOURCE_ID
    )