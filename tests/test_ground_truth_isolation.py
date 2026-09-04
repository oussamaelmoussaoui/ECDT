"""Contract tests for RCAEval ground-truth isolation.

These tests protect the boundary between:

* the operational path: telemetry -> detection -> Observer -> Neo4j;
* the evaluation path: RCAEval labels used only after inference.

They are intentionally independent from the RCAEval files, TimescaleDB,
and Neo4j so the contract can be checked as a fast unit-test suite.
"""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest
import polars as pl

from src.agents.observer.incident_builder import build_incident
from src.agents.observer.incident_persistence import IncidentPersistence
from src.agents.observer.models import AnomalyInput, TemporalContext
from src.agents.observer.observer_agent import ObserverAgent
from src.ingestion.anomaly_detector import AnomalyDetector, DetectorConfig
from src.ingestion.models import (
    AnomalyEvent,
    DetectionMethod,
    DetectionResult,
    IncidentType,
    NormalizedEvent,
    SignalType,
)
from src.ingestion.dataset_loader import DatasetLoader, DatasetPaths
from src.ingestion.schema_normalizer import (
    NormalizedEvent as SchemaNormalizedEvent,
    SchemaNormalizer,
)

from datetime import datetime, timezone

from src.digital_twin.timeseries_ingestion import (
    INSERT_METRIC_QUERY,
    _event_to_row,
)
from src.digital_twin.timeseries_queries import (
    get_metrics_around_timestamp,
)

CASE_ID = "re2ob_checkoutservice_cpu_1"
RESOURCE_ID = "checkoutservice"
METRIC_NAME = "checkoutservice_cpu"
TIMESTAMP = 1_705_354_580


# These labels belong to the RCAEval evaluation path. They must never be
# present in an operational object or in metadata persisted by the Observer.
FORBIDDEN_GROUND_TRUTH_FIELDS = {
    "expected_root_cause",
    "expected_service",
    "fault",
    "fault_description",
    "ground_truth",
    "inject_time",
    "injected_service",
    "injection_time",
    "root_cause_service",
    "time_end",
    "time_start",
}


def _forbidden_paths(value: Any, path: str = "root") -> list[str]:
    """Return every path containing a forbidden ground-truth field."""

    found: list[str] = []

    if is_dataclass(value) and not isinstance(value, type):
        for model_field in fields(value):
            field_path = f"{path}.{model_field.name}"
            if model_field.name.lower() in FORBIDDEN_GROUND_TRUTH_FIELDS:
                found.append(field_path)
            found.extend(
                _forbidden_paths(
                    getattr(value, model_field.name),
                    field_path,
                )
            )
        return found

    if isinstance(value, dict):
        for key, nested_value in value.items():
            normalized_key = str(key).lower()
            field_path = f"{path}.{key}"
            if normalized_key in FORBIDDEN_GROUND_TRUTH_FIELDS:
                found.append(field_path)
            found.extend(_forbidden_paths(nested_value, field_path))
        return found

    if isinstance(value, (list, tuple, set, frozenset)):
        for index, nested_value in enumerate(value):
            found.extend(
                _forbidden_paths(nested_value, f"{path}[{index}]")
            )
        return found

    # Enum values are operational scalar values, not objects to traverse.
    if isinstance(value, Enum):
        return found

    return found


def assert_no_ground_truth(value: Any) -> None:
    """Assert that an operational object contains no RCAEval label."""

    leaked_paths = _forbidden_paths(value)
    assert not leaked_paths, (
        "Ground-truth data leaked into the operational path: "
        + ", ".join(leaked_paths)
    )


def make_anomaly_event(
    *,
    metadata: dict[str, Any] | None = None,
) -> AnomalyEvent:
    return AnomalyEvent(
        event_id="event_001",
        case_id=CASE_ID,
        timestamp=TIMESTAMP,
        service=RESOURCE_ID,
        signal_type=SignalType.CPU,
        value=10.0,
        detection_method=DetectionMethod.Z_SCORE,
        score=12.0,
        threshold=3.0,
        incident_type=IncidentType.CPU_SATURATION,
        metadata=dict(metadata or {}),
    )


def make_anomaly_input(
    *,
    metadata: dict[str, Any] | None = None,
) -> AnomalyInput:
    return AnomalyInput(
        event_id="event_001",
        case_id=CASE_ID,
        timestamp=TIMESTAMP,
        resource_id=RESOURCE_ID,
        signal_type="cpu",
        metric_name=METRIC_NAME,
        value=10.0,
        score=12.0,
        detection_method=DetectionMethod.Z_SCORE,
        incident_type=IncidentType.CPU_SATURATION,
        metadata=dict(metadata or {}),
    )


def make_temporal_context() -> TemporalContext:
    return TemporalContext(
        resource_id=RESOURCE_ID,
        metric_name=METRIC_NAME,
        signal_type="cpu",
        anomaly_timestamp=TIMESTAMP,
        window_before_seconds=300,
        window_after_seconds=300,
        observations=[],
        statistics={"observation_count": 0},
    )


def make_dataset_loader(
    tmp_path,
    *,
    include_labels_in_telemetry: bool = True,
) -> DatasetLoader:
    """Create a loader backed by synthetic RCAEval-style CSV files."""

    telemetry_labels = (
        {
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
        }
        if include_labels_in_telemetry
        else {}
    )

    metrics_path = tmp_path / "metrics.csv"
    logs_path = tmp_path / "logs.csv"
    traces_path = tmp_path / "traces.csv"
    ground_truth_path = tmp_path / "ground_truth.csv"

    pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            **telemetry_labels,
            "time": [TIMESTAMP],
            METRIC_NAME: [10.0],
        }
    ).write_csv(metrics_path)

    pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            **telemetry_labels,
            "timestamp": [TIMESTAMP],
            "container_name": [RESOURCE_ID],
            "message": ["synthetic log"],
        }
    ).write_csv(logs_path)

    pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            **telemetry_labels,
            "time": [TIMESTAMP],
            "traceID": ["trace-001"],
            "spanID": ["span-001"],
            "serviceName": [RESOURCE_ID],
            "methodName": ["Checkout"],
            "operationName": ["PlaceOrder"],
            "parentSpanID": ["parent-001"],
            "startTimeMillis": [TIMESTAMP * 1000],
            "startTime": [TIMESTAMP],
            "duration": [25.0],
            "statusCode": [200],
        }
    ).write_csv(traces_path)

    pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
            "time_start": [TIMESTAMP - 300],
            "inject_time": [TIMESTAMP],
            "time_end": [TIMESTAMP + 300],
            "incident_type": ["cpu_saturation"],
        }
    ).write_csv(ground_truth_path)

    return DatasetLoader(
        DatasetPaths(
            metrics_path=metrics_path,
            logs_path=logs_path,
            traces_path=traces_path,
            ground_truth_path=ground_truth_path,
        )
    )


def test_ground_truth_remains_available_to_evaluation() -> None:
    """Isolation must not delete labels required for final evaluation."""

    result = DetectionResult(
        case_id=CASE_ID,
        fault="cpu",
        incident_type=IncidentType.CPU_SATURATION,
        detected=True,
        root_cause_service=RESOURCE_ID,
    )

    assert result.fault == "cpu"
    assert result.root_cause_service == RESOURCE_ID


def test_normalized_event_contract_excludes_ground_truth() -> None:
    """The normalized operational event schema must expose no label."""

    normalized_event_fields = {
        model_field.name.lower()
        for model_field in fields(NormalizedEvent)
    }

    leaked_fields = (
        normalized_event_fields & FORBIDDEN_GROUND_TRUTH_FIELDS
    )

    assert not leaked_fields, (
        "NormalizedEvent exposes ground-truth fields: "
        + ", ".join(sorted(leaked_fields))
    )


def test_detector_does_not_propagate_ground_truth() -> None:
    """Contaminated source columns must not enter series or anomalies."""

    events = [
        {
            "event_id": f"metric_{index}",
            "case_id": CASE_ID,
            "timestamp_ms": timestamp,
            "source": "metric",
            "service_name": RESOURCE_ID,
            "signal_type": "cpu",
            "metric_name": METRIC_NAME,
            "value": value,
            "fault": "cpu",
            "root_cause_service": RESOURCE_ID,
        }
        for index, (timestamp, value) in enumerate(
            ((1, 1.0), (2, 1.1), (3, 0.9), (4, 10.0)),
            start=1,
        )
    ]

    detector = AnomalyDetector(
        DetectorConfig(
            method=DetectionMethod.Z_SCORE,
            z_threshold=3.0,
            min_baseline_samples=3,
        )
    )

    series_map = detector.build_series(events)
    assert series_map
    assert_no_ground_truth(series_map)

    anomalies = []
    for series in series_map.values():
        anomalies.extend(
            detector.detect_series(
                series,
                baseline_end=4,
                incident_type=IncidentType.CPU_SATURATION,
            )
        )

    assert anomalies, "The synthetic CPU deviation was not detected."
    assert_no_ground_truth(anomalies)


def test_observer_rejects_ground_truth_metadata() -> None:
    """The Observer boundary must fail fast on contaminated input."""

    contaminated = make_anomaly_event(
        metadata={"root_cause_service": RESOURCE_ID}
    )

    with pytest.raises(ValueError):
        ObserverAgent._to_anomaly_input(contaminated)


def test_incident_builder_rejects_ground_truth_metadata() -> None:
    """The Incident Builder is a second defensive boundary."""

    with pytest.raises(ValueError):
        build_incident(
            make_anomaly_input(
                metadata={"expected_service": RESOURCE_ID}
            )
        )


def test_neo4j_serialization_rejects_ground_truth_metadata() -> None:
    """Persistence must refuse a contaminated incident."""

    incident = build_incident(make_anomaly_input())
    incident.metadata["ground_truth"] = {
        "root_cause_service": RESOURCE_ID
    }

    with pytest.raises(ValueError):
        IncidentPersistence._incident_parameters(incident)


def test_clean_observer_path_remains_operational() -> None:
    """Ground-truth isolation must preserve the valid Observer flow."""

    timescale_consumer = MagicMock()
    timescale_consumer.get_temporal_context.return_value = (
        make_temporal_context()
    )

    incident_persistence = MagicMock()
    incident_persistence.persist_incident.return_value = {
        "incident": {"incident_id": "inc_test"},
        "relationship": {
            "incident_id": "inc_test",
            "resource_id": RESOURCE_ID,
        },
    }

    observer = ObserverAgent(
        timescale_consumer=timescale_consumer,
        incident_persistence=incident_persistence,
        window_minutes=5,
    )

    context = observer.process(
        make_anomaly_event(
            metadata={"baseline_samples": 120}
        )
    )

    persisted_incident = (
        incident_persistence.persist_incident.call_args.args[0]
    )

    assert context.persisted is True
    assert_no_ground_truth(context)
    assert_no_ground_truth(persisted_incident)

    serialized = IncidentPersistence._incident_parameters(
        persisted_incident
    )
    serialized_metadata = json.loads(serialized["metadata"])
    assert_no_ground_truth(serialized_metadata)


def test_dataset_loader_keeps_ground_truth_for_evaluation(
    tmp_path,
) -> None:
    """Ground-truth labels remain available through evaluation APIs."""

    loader = make_dataset_loader(tmp_path)

    ground_truth = loader.load_ground_truth(case_id=CASE_ID)
    case_info = loader.get_case_info(CASE_ID)

    assert ground_truth["fault"].to_list() == ["cpu"]
    assert ground_truth["root_cause_service"].to_list() == [
        RESOURCE_ID
    ]
    assert case_info.fault == "cpu"
    assert case_info.root_cause_service == RESOURCE_ID


def test_dataset_loader_excludes_labels_from_telemetry(
    tmp_path,
) -> None:
    """Legacy labelled CSVs must produce label-free telemetry outputs."""

    loader = make_dataset_loader(tmp_path)

    outputs = {
        "metrics_wide": loader.load_metrics(case_id=CASE_ID),
        "metrics_long": loader.load_metrics(
            case_id=CASE_ID,
            long_format=True,
        ),
        "metrics_lazy_wide": loader.scan_metrics(
            case_id=CASE_ID,
        ).collect(),
        "metrics_lazy_long": loader.scan_metrics(
            case_id=CASE_ID,
            long_format=True,
        ).collect(),
        "logs": loader.load_logs(case_id=CASE_ID),
        "traces": loader.load_traces(case_id=CASE_ID),
    }

    for source_name, dataframe in outputs.items():
        leaked_columns = {
            column.lower()
            for column in dataframe.columns
        } & FORBIDDEN_GROUND_TRUTH_FIELDS

        assert not leaked_columns, (
            f"{source_name} exposes ground truth: "
            + ", ".join(sorted(leaked_columns))
        )

        assert_no_ground_truth(dataframe.to_dicts())

    metric_columns = loader.get_metric_columns()

    assert "fault" not in metric_columns
    assert "root_cause_service" not in metric_columns


def test_dataset_loader_accepts_label_free_telemetry(
    tmp_path,
) -> None:
    """Operational CSV schemas must not require evaluation labels."""

    loader = make_dataset_loader(
        tmp_path,
        include_labels_in_telemetry=False,
    )

    outputs = [
        loader.load_metrics(
            case_id=CASE_ID,
            long_format=True,
        ),
        loader.scan_metrics(
            case_id=CASE_ID,
            long_format=True,
        ).collect(),
        loader.load_logs(case_id=CASE_ID),
        loader.load_traces(case_id=CASE_ID),
    ]

    assert all(
        not dataframe.is_empty()
        for dataframe in outputs
    )

    for dataframe in outputs:
        assert_no_ground_truth(dataframe.to_dicts())

def test_schema_normalizer_event_contract_excludes_ground_truth() -> None:
    """The normalizer's Python event contract must expose no label."""

    event_fields = {
        model_field.name.lower()
        for model_field in fields(SchemaNormalizedEvent)
    }

    leaked_fields = (
        event_fields & FORBIDDEN_GROUND_TRUTH_FIELDS
    )

    assert not leaked_fields, (
        "SchemaNormalizer.NormalizedEvent exposes ground-truth fields: "
        + ", ".join(sorted(leaked_fields))
    )


def test_schema_normalizer_dataframes_exclude_ground_truth() -> None:
    """Normalized dataframes must contain no evaluation labels."""

    normalizer = SchemaNormalizer()

    metrics = pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
            "timestamp": [TIMESTAMP * 1000],
            "metric_name": [METRIC_NAME],
            "value": [10.0],
        }
    )

    logs = pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
            "timestamp_ms": [TIMESTAMP * 1000],
            "container_name": [RESOURCE_ID],
            "message": ["synthetic log"],
        }
    )

    traces = pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
            "timestamp_ms": [TIMESTAMP * 1000],
            "traceID": ["trace-001"],
            "spanID": ["span-001"],
            "serviceName": [RESOURCE_ID],
            "methodName": ["Checkout"],
            "operationName": ["PlaceOrder"],
            "parentSpanID": [None],
            "duration": [25.0],
            "statusCode": [200],
        }
    )

    outputs = {
        "metrics": normalizer.normalize_metrics(metrics),
        "logs": normalizer.normalize_logs(logs),
        "traces": normalizer.normalize_traces(traces),
        "combined": normalizer.normalize_all(
            metrics=metrics,
            logs=logs,
            traces=traces,
        ),
    }

    for source_name, dataframe in outputs.items():
        leaked_columns = {
            column.lower()
            for column in dataframe.columns
        } & FORBIDDEN_GROUND_TRUTH_FIELDS

        assert not leaked_columns, (
            f"{source_name} normalized dataframe exposes ground truth: "
            + ", ".join(sorted(leaked_columns))
        )

        assert_no_ground_truth(dataframe.to_dicts())


def test_schema_normalizer_to_events_excludes_ground_truth() -> None:
    """Python events produced by the normalizer must contain no labels."""

    normalizer = SchemaNormalizer()

    metrics = pl.DataFrame(
        {
            "case": [CASE_ID],
            "dataset": ["RE2-OB"],
            "fault": ["cpu"],
            "root_cause_service": [RESOURCE_ID],
            "timestamp": [TIMESTAMP * 1000],
            "metric_name": [METRIC_NAME],
            "value": [10.0],
        }
    )

    normalized = normalizer.normalize_metrics(metrics)
    events = normalizer.to_events(normalized)

    assert events
    assert_no_ground_truth(events)
    
    
def test_timescale_ingestion_excludes_ground_truth() -> None:
    """TimescaleDB ingestion must ignore evaluation labels."""

    contaminated_event = {
        "source": "metric",
        "case_id": CASE_ID,
        "timestamp_ms": TIMESTAMP * 1000,
        "service_name": RESOURCE_ID,
        "signal_type": "cpu",
        "metric_name": METRIC_NAME,
        "value": 10.0,
        "dataset": "RE2-OB",
        "fault": "cpu",
        "root_cause_service": RESOURCE_ID,
    }

    row = _event_to_row(contaminated_event)

    assert "fault" not in INSERT_METRIC_QUERY.lower()
    assert "root_cause_service" not in INSERT_METRIC_QUERY.lower()
    assert len(row) == 7


def test_timescale_observer_query_excludes_ground_truth() -> None:
    """The Observer's temporal query must expose no evaluation label."""

    client = MagicMock()
    client.execute.return_value = []

    get_metrics_around_timestamp(
        client,
        resource_id=RESOURCE_ID,
        timestamp=datetime.fromtimestamp(
            TIMESTAMP,
            tz=timezone.utc,
        ),
        window_minutes=5,
    )

    executed_query = client.execute.call_args.args[0].lower()

    assert "fault" not in executed_query
    assert "root_cause_service" not in executed_query

def test_detector_infers_incident_type_from_signal() -> None:
    """Incident type must come from telemetry, not RCAEval labels."""

    events = [
        {
            "event_id": f"cpu_{index}",
            "case_id": CASE_ID,
            "timestamp_ms": timestamp,
            "source": "metric",
            "service_name": RESOURCE_ID,
            "signal_type": "cpu",
            "metric_name": METRIC_NAME,
            "value": value,
        }
        for index, (timestamp, value) in enumerate(
            (
                (1, 1.0),
                (2, 1.1),
                (3, 0.9),
                (4, 10.0),
            ),
            start=1,
        )
    ]

    detector = AnomalyDetector(
        DetectorConfig(
            method=DetectionMethod.Z_SCORE,
            z_threshold=3.0,
            min_baseline_samples=3,
        )
    )

    anomalies = detector.detect_in_events(
        events,
        baseline_end=4,
    )

    assert anomalies
    assert {
        anomaly.incident_type
        for anomaly in anomalies
    } == {
        IncidentType.CPU_SATURATION
    }


def test_detector_excludes_unmapped_operational_signals() -> None:
    """Signals without a target incident type must not reach Observer."""

    events = [
        {
            "event_id": f"memory_{index}",
            "case_id": CASE_ID,
            "timestamp_ms": timestamp,
            "source": "metric",
            "service_name": RESOURCE_ID,
            "signal_type": "mem",
            "metric_name": "checkoutservice_mem",
            "value": value,
        }
        for index, (timestamp, value) in enumerate(
            (
                (1, 1.0),
                (2, 1.1),
                (3, 0.9),
                (4, 10.0),
            ),
            start=1,
        )
    ]

    detector = AnomalyDetector(
        DetectorConfig(
            method=DetectionMethod.Z_SCORE,
            z_threshold=3.0,
            min_baseline_samples=3,
        )
    )

    anomalies = detector.detect_in_events(
        events,
        baseline_end=4,
    )

    assert anomalies == []
