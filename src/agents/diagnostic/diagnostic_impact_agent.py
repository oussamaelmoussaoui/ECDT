
"""
ECDT - Diagnostic / Impact Agent.

Consumes the Incident / IncidentContext produced by the Observer and combines:

    Neo4j
        structural dependency traversal

    TimescaleDB
        temporal deviation analysis

The agent writes only:
    SUSPECTED_ROOT_CAUSE
    IMPACTS

It never writes:
    CAUSED_BY

CAUSED_BY remains RCAEval ground truth.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any

from src.agents.diagnostic.diagnostic_models import (
    DiagnosticResult,
    ImpactedResource,
    MetricDeviation,
    RootCauseCandidate,
)
from src.agents.observer.models import Incident, IncidentContext
from src.digital_twin.deviation_analysis import (
    build_metric_deviation,
    epoch_to_datetime,
    resolve_metric_name,
)
from src.knowledge_graph.dependency_analysis import (
    get_downstream_dependents,
    get_driver,
    get_upstream_candidates,
    write_diagnostic_relations,
)

logger = logging.getLogger(__name__)


class DiagnosticImpactAgent:
    """
    Deterministic structural + temporal Diagnostic/Impact agent.

    The scoring layer is intentionally deterministic at this stage. An LLM
    can consume DiagnosticResult later, but it is not used to establish the
    root-cause hypothesis here.
    """

    def __init__(
        self,
        max_hops: int = 3,
        z_threshold: float = 3.0,
        top_k_causes: int = 3,
        structural_weight: float = 0.4,
        temporal_weight: float = 0.6,
        neo4j_driver=None,
    ) -> None:
        if max_hops < 1:
            raise ValueError("max_hops must be >= 1.")
        if z_threshold <= 0:
            raise ValueError("z_threshold must be > 0.")
        if top_k_causes < 1:
            raise ValueError("top_k_causes must be >= 1.")
        if structural_weight < 0 or temporal_weight < 0:
            raise ValueError("Scoring weights must be >= 0.")

        total_weight = structural_weight + temporal_weight
        if total_weight <= 0:
            raise ValueError(
                "At least one scoring weight must be greater than zero."
            )

        self.max_hops = max_hops
        self.z_threshold = z_threshold
        self.top_k_causes = top_k_causes

        # Normalize weights so the final confidence remains in [0, 1].
        self.structural_weight = structural_weight / total_weight
        self.temporal_weight = temporal_weight / total_weight

        self._driver = neo4j_driver

    @property
    def driver(self):
        if self._driver is None:
            self._driver = get_driver()
        return self._driver

    def close(self) -> None:
        """Close the owned Neo4j driver, if one exists."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def __enter__(self) -> "DiagnosticImpactAgent":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @staticmethod
    def _normalize_context(
        incident_or_context: Incident | IncidentContext,
    ) -> Incident:
        if isinstance(incident_or_context, IncidentContext):
            return incident_or_context.incident

        if isinstance(incident_or_context, Incident):
            return incident_or_context

        raise TypeError(
            "diagnose() expects an Observer Incident or IncidentContext."
        )

    @staticmethod
    def _incident_datetime(incident: Incident) -> datetime:
        return epoch_to_datetime(incident.detected_at)

    @staticmethod
    def _metric_type(incident: Incident) -> str:
        """
        Map Observer signal_type to the TimescaleDB metric_type.

        Phase 2 stores NormalizedEvent.signal_type in metric_type.
        """
        signal_type = incident.signal_type

        if hasattr(signal_type, "value"):
            signal_type = signal_type.value

        return str(signal_type)

    def diagnose(
        self,
        incident_or_context: Incident | IncidentContext,
    ) -> DiagnosticResult:
        """
        Execute the complete structural + temporal diagnostic pipeline.
        """
        incident = self._normalize_context(incident_or_context)

        incident_id = incident.incident_id
        incident_time = self._incident_datetime(incident)
        metric_type = self._metric_type(incident)

        logger.info(
            "Diagnostic started for incident=%s resource=%s",
            incident_id,
            incident.resource_id,
        )

        upstream_candidates = get_upstream_candidates(
            self.driver,
            incident.resource_id,
            self.max_hops,
        )

        downstream_dependents = get_downstream_dependents(
            self.driver,
            incident.resource_id,
            self.max_hops,
        )

        deviations = self._collect_deviations(
            upstream_candidates,
            incident,
            incident_time,
            metric_type,
        )

        root_causes = self._rank_root_causes(deviations)

        impacted = self._score_impacted_resources(
            downstream_dependents,
            incident,
            incident_time,
            metric_type,
        )

        result = DiagnosticResult(
            incident_id=incident_id,
            root_cause_candidates=root_causes,
            impacted_resources=impacted,
            deviation_timeline=deviations,
        )

        write_diagnostic_relations(
            self.driver,
            incident_id=incident_id,
            root_cause_candidates=result.root_cause_candidates,
            impacted_resources=result.impacted_resources,
        )

        top = result.suspected_root_cause

        logger.info(
            "Diagnostic completed for incident=%s root_cause=%s confidence=%.4f",
            incident_id,
            top.resource_id if top else None,
            top.confidence if top else 0.0,
        )

        return result

    def _collect_deviations(
        self,
        upstream_candidates: list[dict],
        incident: Incident,
        incident_time: datetime,
        metric_type: str,
    ) -> list[MetricDeviation]:
        """
        Analyze the same signal family on each structural candidate.

        The original implementation reused incident.metric_name literally,
        e.g. checkoutservice_cpu, for every upstream service. That is incorrect
        because metric names are resource-local. We therefore resolve:

            candidate_service + incident.signal_type

        e.g.:

            paymentservice + cpu -> paymentservice_cpu
        """
        deviations: list[MetricDeviation] = []

        for candidate in upstream_candidates:
            resource_id = candidate["resource_id"]
            candidate_metric_name = resolve_metric_name(
                resource_id,
                metric_type,
            )

            try:
                deviation = build_metric_deviation(
                    resource_id=resource_id,
                    metric_name=candidate_metric_name,
                    metric_type=metric_type,
                    hop_distance=int(candidate["hop_distance"]),
                    incident_time=incident_time,
                    z_threshold=self.z_threshold,
                    allow_post_incident=False,
                )
            except Exception:
                logger.exception(
                    "Temporal analysis failed for candidate=%s",
                    resource_id,
                )
                raise

            deviations.append(deviation)

        return deviations

    def _rank_root_causes(
        self,
        deviations: list[MetricDeviation],
    ) -> list[RootCauseCandidate]:
        """
        Rank candidates from temporal precedence + graph proximity.

        Temporal score:
            earliest deviation => 1.0

        Structural score:
            closer candidate => higher score

        If no upstream metric deviates before the incident, the incident
        resource itself is retained as a low-confidence local fallback.
        """
        deviated = [
            deviation
            for deviation in deviations
            if deviation.deviated
            and deviation.onset_timestamp is not None
        ]

        if not deviated:
            fallback = next(
                (
                    deviation
                    for deviation in deviations
                    if deviation.hop_distance == 0
                ),
                None,
            )

            if fallback is None:
                return []

            return [
                RootCauseCandidate(
                    resource_id=fallback.resource_id,
                    resource_type="Service",
                    hop_distance=0,
                    onset_timestamp=None,
                    temporal_score=0.0,
                    structural_score=1.0,
                    confidence=0.3,
                    rank=1,
                )
            ]

        earliest = min(
            deviation.onset_timestamp
            for deviation in deviated
            if deviation.onset_timestamp is not None
        )

        latest = max(
            deviation.onset_timestamp
            for deviation in deviated
            if deviation.onset_timestamp is not None
        )

        time_span = max(
            (latest - earliest).total_seconds(),
            1.0,
        )

        max_hop = max(
            deviation.hop_distance
            for deviation in deviated
        )

        scored: list[RootCauseCandidate] = []

        for deviation in deviated:
            assert deviation.onset_timestamp is not None

            elapsed = (
                deviation.onset_timestamp - earliest
            ).total_seconds()

            temporal_score = max(
                0.0,
                min(1.0, 1.0 - elapsed / time_span),
            )

            structural_score = max(
                0.0,
                min(
                    1.0,
                    1.0
                    - deviation.hop_distance
                    / (max_hop + 1),
                ),
            )

            confidence = (
                self.temporal_weight * temporal_score
                + self.structural_weight * structural_score
            )

            scored.append(
                RootCauseCandidate(
                    resource_id=deviation.resource_id,
                    resource_type="Service",
                    hop_distance=deviation.hop_distance,
                    onset_timestamp=deviation.onset_timestamp,
                    temporal_score=round(temporal_score, 4),
                    structural_score=round(structural_score, 4),
                    confidence=round(
                        max(0.0, min(1.0, confidence)),
                        4,
                    ),
                    rank=1,
                )
            )

        scored.sort(
            key=lambda candidate: (
                -candidate.confidence,
                candidate.hop_distance,
                candidate.onset_timestamp
                or datetime.max.replace(tzinfo=timezone.utc),
                candidate.resource_id,
            )
        )

        selected = scored[: self.top_k_causes]

        for rank, candidate in enumerate(
            selected,
            start=1,
        ):
            candidate.rank = rank

        return selected

    def _score_impacted_resources(
        self,
        downstream: list[dict],
        incident: Incident,
        incident_time: datetime,
        metric_type: str,
    ) -> list[ImpactedResource]:
        """
        Score downstream services by graph distance and metric confirmation.

        Unlike root-cause onset detection, impact confirmation may use the
        post-incident lookahead window because propagation can occur after
        the triggering anomaly.
        """
        impacted: list[ImpactedResource] = []

        for dependent in downstream:
            resource_id = dependent["resource_id"]
            hop_distance = int(dependent["hop_distance"])

            metric_name = resolve_metric_name(
                resource_id,
                metric_type,
            )

            deviation = build_metric_deviation(
                resource_id=resource_id,
                metric_name=metric_name,
                metric_type=metric_type,
                hop_distance=hop_distance,
                incident_time=incident_time,
                z_threshold=self.z_threshold,
                allow_post_incident=True,
            )

            base_score = 1.0 / (1.0 + hop_distance)

            confirmation_factor = (
                1.3 if deviation.deviated else 0.8
            )

            impact_score = min(
                1.0,
                base_score * confirmation_factor,
            )

            impacted.append(
                ImpactedResource(
                    resource_id=resource_id,
                    resource_type=dependent.get(
                        "resource_type",
                        "Service",
                    ),
                    hop_distance=hop_distance,
                    impact_score=round(
                        impact_score,
                        4,
                    ),
                    confirmed_by_metrics=deviation.deviated,
                )
            )

        impacted.sort(
            key=lambda resource: (
                -resource.impact_score,
                resource.hop_distance,
                resource.resource_id,
            )
        )

        return impacted


def diagnostic_impact_node(state: dict) -> dict:
    """
    Orchestrator node.

    Accepted state:
        state["incident"] = Incident or IncidentContext

    Output:
        state["diagnostic_result"] = DiagnosticResult
        state["diagnostic_context"] = compact LLM-ready dict
    """
    if "incident" not in state:
        raise KeyError("diagnostic_impact_node requires state['incident'].")

    agent = state.get("_diagnostic_impact_agent")

    if agent is None:
        agent = DiagnosticImpactAgent()

    result = agent.diagnose(state["incident"])

    state["diagnostic_result"] = result
    state["diagnostic_context"] = result.to_llm_context()
    state["_diagnostic_impact_agent"] = agent

    return state


def _load_incident_from_graph(
    driver,
    incident_id: str,
) -> Incident:
    """
    Reconstruct the Observer Incident contract from the persisted Neo4j node.

    This is CLI-only. The normal application flow should pass the Incident
    or IncidentContext directly from ObserverAgent.
    """
    query = """
    MATCH (i:Incident {id: $incident_id})
    RETURN
        i.id AS incident_id,
        i.case_id AS case_id,
        i.incident_type AS incident_type,
        i.status AS status,
        i.severity AS severity,
        i.resource_id AS resource_id,
        i.detected_at AS detected_at,
        i.signal_type AS signal_type,
        i.metric_name AS metric_name,
        i.observed_value AS observed_value,
        i.anomaly_score AS anomaly_score,
        i.detection_method AS detection_method,
        i.confidence AS confidence,
        i.source AS source,
        i.metadata AS metadata
    """

    with driver.session() as session:
        record = session.run(
            query,
            incident_id=incident_id,
        ).single()

    if record is None:
        raise ValueError(
            f"Incident not found in Neo4j: {incident_id}"
        )

    data = record.data()

    # Import locally to keep the main module dependencies explicit.
    from src.agents.observer.models import (
        IncidentSeverity,
        IncidentSource,
        IncidentStatus,
    )
    from src.ingestion.models import (
        DetectionMethod,
        IncidentType,
        SignalType,
    )

    return Incident(
        incident_id=data["incident_id"],
        case_id=data["case_id"],
        incident_type=IncidentType(data["incident_type"]),
        status=IncidentStatus(data["status"]),
        severity=IncidentSeverity(data["severity"]),
        resource_id=data["resource_id"],
        detected_at=int(data["detected_at"]),
        signal_type=SignalType(data["signal_type"]),
        metric_name=data["metric_name"],
        observed_value=float(data["observed_value"]),
        anomaly_score=float(data["anomaly_score"]),
        detection_method=DetectionMethod(data["detection_method"]),
        confidence=float(data["confidence"]),
        source=IncidentSource(data["source"]),
        metadata={},
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Run the ECDT Diagnostic/Impact Agent on a persisted incident."
        )
    )
    parser.add_argument(
        "--incident-id",
        required=True,
        help="Neo4j Incident.id",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=3.0,
    )
    parser.add_argument(
        "--top-k-causes",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    with DiagnosticImpactAgent(
        max_hops=args.max_hops,
        z_threshold=args.z_threshold,
        top_k_causes=args.top_k_causes,
    ) as agent:
        incident = _load_incident_from_graph(
            agent.driver,
            args.incident_id,
        )

        result = agent.diagnose(incident)

        print(
            json.dumps(
                result.to_llm_context(),
                indent=2,
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
