
"""
ECDT - Knowledge Graph
Dependency and diagnostic queries for the Diagnostic/Impact Agent.

Graph convention:
    (:Service)-[:DEPENDS_ON]->(:Service)

Therefore:
    - upstream/root-cause candidates are reached by following outgoing
      DEPENDS_ON edges from the incident resource;
    - downstream/dependent resources are reached by following incoming
      DEPENDS_ON edges.

Agent conclusions are persisted only as:
    SUSPECTED_ROOT_CAUSE
    IMPACTS

CAUSED_BY is ground truth and is never written by this module.
"""

from __future__ import annotations

import os
from typing import Iterable

from neo4j import Driver, GraphDatabase

from src.agents.diagnostic.diagnostic_models import ImpactedResource, RootCauseCandidate


FORBIDDEN_WRITE_RELATIONS = {"CAUSED_BY"}
ALLOWED_WRITE_RELATIONS = {"SUSPECTED_ROOT_CAUSE", "IMPACTS"}


def _assert_safe_relation_type(relation_type: str) -> None:
    """Reject every relationship type that an agent is not allowed to write."""
    if relation_type in FORBIDDEN_WRITE_RELATIONS:
        raise PermissionError(
            f"Writing relationship '{relation_type}' is forbidden: "
            "CAUSED_BY is reserved for RCAEval ground truth."
        )

    if relation_type not in ALLOWED_WRITE_RELATIONS:
        raise ValueError(
            f"Unsupported agent write relationship: '{relation_type}'"
        )


def get_driver() -> Driver:
    """Create a Neo4j driver from the project environment."""
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "")

    if not password:
        raise ValueError("NEO4J_PASSWORD is not configured.")

    return GraphDatabase.driver(uri, auth=(user, password))


def _validate_max_hops(max_hops: int) -> None:
    if not isinstance(max_hops, int) or max_hops < 1:
        raise ValueError("max_hops must be an integer >= 1.")


def get_upstream_candidates(
    driver: Driver,
    resource_id: str,
    max_hops: int = 3,
) -> list[dict]:
    """
    Return structural root-cause candidates.

    The incident resource itself is explicitly included at hop 0 so that
    a locally originating incident remains a valid fallback candidate.
    """
    if not resource_id:
        raise ValueError("resource_id must not be empty.")
    _validate_max_hops(max_hops)

    query = f"""
    MATCH (start:Service {{id: $resource_id}})
    OPTIONAL MATCH path =
        (start)-[:DEPENDS_ON*1..{max_hops}]->(candidate:Service)
    WITH candidate, min(length(path)) AS hop_distance
    WHERE candidate IS NOT NULL
    RETURN DISTINCT
        candidate.id AS resource_id,
        'Service' AS resource_type,
        hop_distance
    ORDER BY hop_distance ASC, resource_id ASC
    """

    with driver.session() as session:
        rows = session.run(query, resource_id=resource_id).data()

    return [
        {
            "resource_id": resource_id,
            "resource_type": "Service",
            "hop_distance": 0,
        },
        *rows,
    ]


def get_downstream_dependents(
    driver: Driver,
    resource_id: str,
    max_hops: int = 3,
) -> list[dict]:
    """Return services that depend directly or transitively on the incident resource."""
    if not resource_id:
        raise ValueError("resource_id must not be empty.")
    _validate_max_hops(max_hops)

    query = f"""
    MATCH (start:Service {{id: $resource_id}})
    OPTIONAL MATCH path =
        (dependent:Service)-[:DEPENDS_ON*1..{max_hops}]->(start)
    WITH dependent, min(length(path)) AS hop_distance
    WHERE dependent IS NOT NULL
    RETURN DISTINCT
        dependent.id AS resource_id,
        'Service' AS resource_type,
        hop_distance
    ORDER BY hop_distance ASC, resource_id ASC
    """

    with driver.session() as session:
        return session.run(query, resource_id=resource_id).data()


_WRITE_ROOT_CAUSE_QUERY = """
MATCH (i:Incident {id: $incident_id})
MATCH (r:Service {id: $resource_id})
MERGE (i)-[rel:SUSPECTED_ROOT_CAUSE]->(r)
SET
    rel.confidence = $confidence,
    rel.rank = $rank,
    rel.hop_distance = $hop_distance,
    rel.diagnosed_at = datetime()
"""

_WRITE_IMPACT_QUERY = """
MATCH (i:Incident {id: $incident_id})
MATCH (r:Service {id: $resource_id})
MERGE (i)-[rel:IMPACTS]->(r)
SET
    rel.impact_score = $impact_score,
    rel.hop_distance = $hop_distance,
    rel.confirmed_by_metrics = $confirmed_by_metrics,
    rel.diagnosed_at = datetime()
"""

_CLEAR_AGENT_RELATIONS_QUERY = """
MATCH (i:Incident {id: $incident_id})
OPTIONAL MATCH (i)-[r:SUSPECTED_ROOT_CAUSE|IMPACTS]->()
DELETE r
"""

_UPDATE_INCIDENT_SUMMARY_QUERY = """
MATCH (i:Incident {id: $incident_id})
SET
    i.probable_root_cause_resource_id = $resource_id,
    i.root_cause_confidence = $confidence,
    i.diagnosed_at = datetime()
"""


def write_diagnostic_relations(
    driver: Driver,
    incident_id: str,
    root_cause_candidates: Iterable[RootCauseCandidate],
    impacted_resources: Iterable[ImpactedResource],
) -> None:
    """
    Replace the agent-generated diagnostic relations for one incident.

    The operation is transactional and never writes CAUSED_BY.
    """
    if not incident_id:
        raise ValueError("incident_id must not be empty.")

    _assert_safe_relation_type("SUSPECTED_ROOT_CAUSE")
    _assert_safe_relation_type("IMPACTS")

    candidates = list(root_cause_candidates)
    impacts = list(impacted_resources)

    top_candidates = sorted(candidates, key=lambda c: (c.rank, c.resource_id))

    with driver.session() as session:
        tx = session.begin_transaction()
        try:
            # Prevent stale conclusions from previous diagnostic executions.
            tx.run(
                _CLEAR_AGENT_RELATIONS_QUERY,
                incident_id=incident_id,
            )

            for candidate in candidates:
                tx.run(
                    _WRITE_ROOT_CAUSE_QUERY,
                    incident_id=incident_id,
                    resource_id=candidate.resource_id,
                    confidence=candidate.confidence,
                    rank=candidate.rank,
                    hop_distance=candidate.hop_distance,
                )

            for impacted in impacts:
                tx.run(
                    _WRITE_IMPACT_QUERY,
                    incident_id=incident_id,
                    resource_id=impacted.resource_id,
                    impact_score=impacted.impact_score,
                    hop_distance=impacted.hop_distance,
                    confirmed_by_metrics=impacted.confirmed_by_metrics,
                )

            if top_candidates:
                top = top_candidates[0]
                tx.run(
                    _UPDATE_INCIDENT_SUMMARY_QUERY,
                    incident_id=incident_id,
                    resource_id=top.resource_id,
                    confidence=top.confidence,
                )
            else:
                tx.run(
                    """
                    MATCH (i:Incident {id: $incident_id})
                    REMOVE
                        i.probable_root_cause_resource_id,
                        i.root_cause_confidence,
                        i.diagnosed_at
                    """,
                    incident_id=incident_id,
                )

            tx.commit()
        except Exception:
            tx.rollback()
            raise
