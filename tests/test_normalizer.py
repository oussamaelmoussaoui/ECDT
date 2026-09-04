from pathlib import Path

import pytest
import polars as pl

from src.ingestion.dataset_loader import create_default_loader
from src.ingestion.schema_normalizer import SchemaNormalizer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASE = "re2ob_checkoutservice_cpu_1"


@pytest.fixture(scope="module")
def loader():
    return create_default_loader(PROJECT_ROOT)


@pytest.fixture(scope="module")
def normalizer():
    return SchemaNormalizer()


def test_parse_metric_name(normalizer):
    test_cases = {
        "adservice_cpu": ("adservice", "cpu"),
        "checkoutservice_mem": ("checkoutservice", "mem"),
        "carts-db_diskio": ("carts-db", "diskio"),
        "frontend_latency-50": ("frontend", "latency-50"),
        "ts-route-service_latency-90": (
            "ts-route-service",
            "latency-90",
        ),
        "ts-auth-service_error": (
            "ts-auth-service",
            "error",
        ),
    }

    for metric_name, expected in test_cases.items():
        result = normalizer.parse_metric_name(metric_name)

        assert result == expected, (
            f"Invalid parsing for {metric_name}: "
            f"expected {expected}, got {result}"
        )


def test_normalize_metrics(loader, normalizer):
    metrics = loader.load_metrics(
        case_id=CASE,
        long_format=True,
    )

    events = normalizer.normalize_metrics(metrics)

    assert isinstance(events, pl.DataFrame)
    assert events.height > 0

    expected_columns = {
        "event_id",
        "case_id",
        "timestamp_ms",
        "dataset",
        "source",
        "service_name",
        "signal_type",
        "metric_name",
        "value",
    }

    assert expected_columns.issubset(set(events.columns))
    assert {
        "fault",
        "root_cause_service",
    }.isdisjoint(events.columns)


def test_normalized_metric_types(loader, normalizer):
    metrics = loader.load_metrics(
        case_id=CASE,
        long_format=True,
    )

    events = normalizer.normalize_metrics(metrics)

    assert events["source"].unique().to_list() == ["metric"]

    assert events["service_name"].null_count() < events.height

    assert events["signal_type"].null_count() < events.height

    assert events["value"].dtype == pl.Float64


def test_normalized_metric_semantics(loader, normalizer):
    metrics = loader.load_metrics(
        case_id=CASE,
        long_format=True,
    )

    events = normalizer.normalize_metrics(metrics)

    checkout_cpu = events.filter(
        (pl.col("service_name") == "checkoutservice")
        & (pl.col("signal_type") == "cpu")
    )

    assert checkout_cpu.height == 1441

    assert checkout_cpu["metric_name"].unique().to_list() == [
        "checkoutservice_cpu"
    ]


def test_normalize_logs(loader, normalizer):
    logs = loader.load_logs(case_id=CASE)

    events = normalizer.normalize_logs(logs)

    assert isinstance(events, pl.DataFrame)
    assert events.height > 0

    assert events["source"].unique().to_list() == ["log"]

    expected_columns = {
        "event_id",
        "case_id",
        "timestamp_ms",
        "dataset",
        "source",
        "message",
    }

    assert expected_columns.issubset(set(events.columns))
    assert {
        "fault",
        "root_cause_service",
    }.isdisjoint(events.columns)


def test_normalize_traces(loader, normalizer):
    traces = loader.load_traces(case_id=CASE)

    events = normalizer.normalize_traces(traces)

    assert isinstance(events, pl.DataFrame)
    assert events.height > 0

    assert events["source"].unique().to_list() == ["trace"]

    expected_columns = {
        "event_id",
        "case_id",
        "timestamp_ms",
        "dataset",
        "source",
        "trace_id",
        "span_id",
        "parent_span_id",
        "method_name",
        "operation_name",
        "duration_ms",
        "status_code",
    }

    assert expected_columns.issubset(set(events.columns))
    assert {
        "fault",
        "root_cause_service",
    }.isdisjoint(events.columns)


def test_normalize_all(loader, normalizer):
    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=True,
        include_traces=True,
        metrics_long_format=True,
    )

    events = normalizer.normalize_all(
        metrics=data["metrics"],
        logs=data["logs"],
        traces=data["traces"],
    )

    assert isinstance(events, pl.DataFrame)
    assert {
        "fault",
        "root_cause_service",
    }.isdisjoint(events.columns)

    assert events.height == 1_348_664

    sources = (
        events
        .group_by("source")
        .len()
        .sort("source")
    )

    source_counts = {
        row["source"]: row["len"]
        for row in sources.to_dicts()
    }

    assert source_counts["metric"] == 785345
    assert source_counts["log"] == 171322
    assert source_counts["trace"] == 391997


def test_normalized_timestamp_range(loader, normalizer):
    data = loader.load_case(
        CASE,
        include_metrics=True,
        include_logs=True,
        include_traces=True,
        metrics_long_format=True,
    )

    events = normalizer.normalize_all(
        metrics=data["metrics"],
        logs=data["logs"],
        traces=data["traces"],
    )

    assert events["timestamp_ms"].min() == 1705353846000
    assert events["timestamp_ms"].max() <= 1705355286000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])