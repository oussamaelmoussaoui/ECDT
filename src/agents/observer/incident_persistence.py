"""
ECDT - Phase 5
Observer Agent - Incident Persistence.

Responsible for persisting Observer-generated incidents
into the ECDT Knowledge Graph.

Graph semantics:

    (:Incident)-[:AFFECTS]->(:Service)

Important distinction:

    AFFECTS
        means that the Observer detected an anomaly
        affecting a resource.

    CAUSED_BY
        represents an established root-cause relationship
        and must not be created by the Observer.

The Observer therefore never invents a root cause.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from src.agents.observer.models import Incident

if TYPE_CHECKING:
    from src.knowledge_graph.neo4j_client import Neo4jClient


class IncidentPersistence:
    """
    Persistence layer for Observer-generated incidents.

    This class is intentionally separated from graph_queries.py
    because graph_queries.py is a read-only query layer.
    """

    def __init__(
        self,
        client: Neo4jClient,
    ) -> None:
        """
        Initialize the persistence layer.

        Parameters
        ----------
        client:
            Existing Neo4jClient instance.
        """

        if client is None:
            raise ValueError(
                "Neo4j client must not be None."
            )

        self.client = client

    # ------------------------------------------------------------------
    # Incident creation
    # ------------------------------------------------------------------

    def create_incident(
        self,
        incident: Incident,
    ) -> dict[str, Any]:
        """
        Create or update an Incident node.

        MERGE is used on Incident.id so that the operation
        is idempotent.

        Returns
        -------
        dict[str, Any]
            Persisted incident information.
        """

        if not incident.incident_id:
            raise ValueError(
                "incident_id must not be empty."
            )

        if not incident.case_id:
            raise ValueError(
                "case_id must not be empty."
            )

        query = """
        MERGE (i:Incident {
            id: $incident_id
        })

        SET
            i.case_id = $case_id,
            i.incident_type = $incident_type,
            i.status = $status,
            i.severity = $severity,
            i.resource_id = $resource_id,
            i.detected_at = $detected_at,
            i.signal_type = $signal_type,
            i.metric_name = $metric_name,
            i.observed_value = $observed_value,
            i.anomaly_score = $anomaly_score,
            i.detection_method = $detection_method,
            i.confidence = $confidence,
            i.source = $source,
            i.metadata = $metadata

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

        result = self.client.execute_write(
            query,
            self._incident_parameters(
                incident
            ),
        )

        if not result:
            raise RuntimeError(
                "Neo4j did not return the persisted incident."
            )

        return result[0]

    # ------------------------------------------------------------------
    # Incident -> Resource
    # ------------------------------------------------------------------

    def link_incident_to_resource(
        self,
        incident: Incident,
    ) -> dict[str, Any]:
        """
        Create the Observer relationship:

            (:Incident)-[:AFFECTS]->(:Service)

        The resource is expected to already exist in Neo4j.

        No Service node is silently created here because the Observer
        must not invent infrastructure topology.
        """

        if not incident.incident_id:
            raise ValueError(
                "incident_id must not be empty."
            )

        if not incident.resource_id:
            raise ValueError(
                "resource_id must not be empty."
            )

        query = """
        MATCH (i:Incident {
            id: $incident_id
        })

        MATCH (s:Service {
            id: $resource_id
        })

        MERGE (i)-[:AFFECTS]->(s)

        RETURN
            i.id AS incident_id,
            s.id AS resource_id
        """

        result = self.client.execute_write(
            query,
            {
                "incident_id": incident.incident_id,
                "resource_id": incident.resource_id,
            },
        )

        if not result:
            raise ValueError(
                "Unable to link incident to resource. "
                f"Incident '{incident.incident_id}' or "
                f"Service '{incident.resource_id}' "
                "does not exist."
            )

        return result[0]

    # ------------------------------------------------------------------
    # Complete persistence
    # ------------------------------------------------------------------

    def persist_incident(
        self,
        incident: Incident,
    ) -> dict[str, Any]:
        """
        Persist an incident and link it to its resource atomically.

        Pipeline:

            Incident
                |
                +--> Incident node
                |
                +--> AFFECTS
                         |
                         v
                      Service
        """

        # Match the known topology first. If the service does not exist, the
        # query returns no row and no orphan Incident node is created.
        query = """
        MATCH (s:Service {id: $resource_id})

        MERGE (i:Incident {id: $incident_id})

        SET
            i.case_id = $case_id,
            i.incident_type = $incident_type,
            i.status = $status,
            i.severity = $severity,
            i.resource_id = $resource_id,
            i.detected_at = $detected_at,
            i.signal_type = $signal_type,
            i.metric_name = $metric_name,
            i.observed_value = $observed_value,
            i.anomaly_score = $anomaly_score,
            i.detection_method = $detection_method,
            i.confidence = $confidence,
            i.source = $source,
            i.metadata = $metadata

        MERGE (i)-[:AFFECTS]->(s)

        RETURN
            i.id AS incident_id,
            i.case_id AS case_id,
            s.id AS resource_id
        """

        result = self.client.execute_write(
            query,
            self._incident_parameters(incident),
        )

        if not result:
            raise ValueError(
                "Unable to persist incident because Service "
                f"'{incident.resource_id}' does not exist."
            )

        persisted = result[0]

        return {
            "incident": persisted,
            "relationship": {
                "incident_id": persisted["incident_id"],
                "resource_id": persisted["resource_id"],
            },
        }

    # ------------------------------------------------------------------
    # Incident retrieval
    # ------------------------------------------------------------------

    def get_incident(
        self,
        incident_id: str,
    ) -> dict[str, Any] | None:
        """
        Retrieve an Observer incident by ID.

        This method is kept here because it is useful for
        verifying persistence after a write.
        """

        if not incident_id:
            raise ValueError(
                "incident_id must not be empty."
            )

        query = """
        MATCH (i:Incident {
            id: $incident_id
        })

        OPTIONAL MATCH
            (i)-[:AFFECTS]->(s:Service)

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
            i.metadata AS metadata,
            s.id AS affected_resource
        """

        result = self.client.execute(
            query,
            {
                "incident_id": incident_id,
            },
        )

        if not result:
            return None

        return result[0]

    # ------------------------------------------------------------------
    # Relationship verification
    # ------------------------------------------------------------------

    def incident_affects_resource(
        self,
        incident_id: str,
        resource_id: str,
    ) -> bool:
        """
        Verify that an Incident AFFECTS a given Service.
        """

        if not incident_id:
            raise ValueError(
                "incident_id must not be empty."
            )

        if not resource_id:
            raise ValueError(
                "resource_id must not be empty."
            )

        query = """
        MATCH
            (i:Incident {
                id: $incident_id
            })
            -[:AFFECTS]->
            (s:Service {
                id: $resource_id
            })

        RETURN count(*) AS count
        """

        result = self.client.execute(
            query,
            {
                "incident_id": incident_id,
                "resource_id": resource_id,
            },
        )

        return bool(
            result
            and result[0]["count"] > 0
        )

    # ------------------------------------------------------------------
    # Parameter serialization
    # ------------------------------------------------------------------

    @staticmethod
    def _incident_parameters(
        incident: Incident,
    ) -> dict[str, Any]:
        """
        Convert the Incident model into Neo4j-compatible
        parameters.

        Neo4j properties cannot directly contain Python
        dictionaries/maps, so metadata is serialized as JSON.
        """

        return {
            "incident_id": incident.incident_id,

            "case_id": incident.case_id,

            "incident_type": (
                incident.incident_type.value
                if hasattr(
                    incident.incident_type,
                    "value",
                )
                else str(
                    incident.incident_type
                )
            ),

            "status": (
                incident.status.value
                if hasattr(
                    incident.status,
                    "value",
                )
                else str(
                    incident.status
                )
            ),

            "severity": (
                incident.severity.value
                if hasattr(
                    incident.severity,
                    "value",
                )
                else str(
                    incident.severity
                )
            ),

            "resource_id": incident.resource_id,

            "detected_at": incident.detected_at,

            "signal_type": incident.signal_type,

            "metric_name": incident.metric_name,

            "observed_value": float(
                incident.observed_value
            ),

            "anomaly_score": float(
                incident.anomaly_score
            ),

            "detection_method": (
                incident.detection_method.value
                if hasattr(
                    incident.detection_method,
                    "value",
                )
                else str(
                    incident.detection_method
                )
            ),

            "confidence": float(
                incident.confidence
            ),

            "source": (
                incident.source.value
                if hasattr(
                    incident.source,
                    "value",
                )
                else str(
                    incident.source
                )
            ),

            # Neo4j does not accept Map{} as a property.
            # Store structured metadata as JSON.
            "metadata": json.dumps(
                dict(incident.metadata),
                default=str,
            ),
        }
