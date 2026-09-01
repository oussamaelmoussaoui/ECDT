
"""
ECDT - Diagnostic / Impact models.

These models are separate from the Observer models and represent:
    - temporal deviations,
    - suspected root-cause candidates,
    - impacted resources,
    - the final diagnostic result.

CAUSED_BY is deliberately absent from the agent write model because it is
reserved for RCAEval ground truth.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DiagnosticRelationType(str, Enum):
    SUSPECTED_ROOT_CAUSE = "SUSPECTED_ROOT_CAUSE"
    IMPACTS = "IMPACTS"


class MetricDeviation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    metric_name: str
    hop_distance: int = Field(ge=0)
    onset_timestamp: datetime | None = None
    max_z_score: float = Field(default=0.0, ge=0.0)
    deviated: bool = False


class RootCauseCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    resource_type: str | None = None
    hop_distance: int = Field(ge=0)
    onset_timestamp: datetime | None = None

    temporal_score: float = Field(ge=0.0, le=1.0)
    structural_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)

    rank: int = Field(ge=1)


class ImpactedResource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: str
    resource_type: str | None = None
    hop_distance: int = Field(ge=1)
    impact_score: float = Field(ge=0.0, le=1.0)
    confirmed_by_metrics: bool = False


class DiagnosticResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str
    analyzed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    root_cause_candidates: list[RootCauseCandidate] = Field(
        default_factory=list
    )
    impacted_resources: list[ImpactedResource] = Field(
        default_factory=list
    )
    deviation_timeline: list[MetricDeviation] = Field(
        default_factory=list
    )

    @property
    def suspected_root_cause(self) -> RootCauseCandidate | None:
        if not self.root_cause_candidates:
            return None
        return min(
            self.root_cause_candidates,
            key=lambda candidate: candidate.rank,
        )

    def to_llm_context(self) -> dict[str, Any]:
        """
        Produce a compact, JSON-friendly representation for downstream
        Memory/RAG and Recommendation agents.
        """
        top = self.suspected_root_cause

        return {
            "incident_id": self.incident_id,
            "analyzed_at": self.analyzed_at.isoformat(),
            "suspected_root_cause": (
                top.resource_id if top else None
            ),
            "confidence": (
                top.confidence if top else None
            ),
            "root_cause_candidates": [
                {
                    "resource_id": candidate.resource_id,
                    "rank": candidate.rank,
                    "confidence": candidate.confidence,
                    "temporal_score": candidate.temporal_score,
                    "structural_score": candidate.structural_score,
                    "hop_distance": candidate.hop_distance,
                    "onset": (
                        candidate.onset_timestamp.isoformat()
                        if candidate.onset_timestamp
                        else None
                    ),
                }
                for candidate in sorted(
                    self.root_cause_candidates,
                    key=lambda candidate: candidate.rank,
                )
            ],
            "impacted_resources": [
                {
                    "resource_id": resource.resource_id,
                    "impact_score": resource.impact_score,
                    "hop_distance": resource.hop_distance,
                    "confirmed_by_metrics": resource.confirmed_by_metrics,
                }
                for resource in self.impacted_resources
            ],
            "timeline": [
                {
                    "resource_id": deviation.resource_id,
                    "metric": deviation.metric_name,
                    "onset": (
                        deviation.onset_timestamp.isoformat()
                        if deviation.onset_timestamp
                        else None
                    ),
                    "max_z_score": deviation.max_z_score,
                }
                for deviation in sorted(
                    (
                        deviation
                        for deviation in self.deviation_timeline
                        if deviation.deviated
                    ),
                    key=lambda deviation: (
                        deviation.onset_timestamp or datetime.max.replace(
                            tzinfo=timezone.utc
                        )
                    ),
                )
            ],
        }
