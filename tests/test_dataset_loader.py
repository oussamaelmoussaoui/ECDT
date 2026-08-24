from pathlib import Path

import pytest

from src.ingestion.dataset_loader import create_default_loader


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def loader():
    return create_default_loader(PROJECT_ROOT)


def test_loader_lists_expected_cases(loader):
    cases = loader.list_cases()

    assert isinstance(cases, list)
    assert len(cases) == 60

    expected_case = "re2ob_checkoutservice_cpu_1"
    assert expected_case in cases


def test_loader_get_case_info(loader):
    case = "re2ob_checkoutservice_cpu_1"

    info = loader.get_case_info(case)

    assert info.case_id == case
    assert info.dataset == "RE2-OB"
    assert info.fault == "cpu"
    assert info.root_cause_service == "checkoutservice"

    assert info.time_start_ms < info.inject_time_ms
    assert info.inject_time_ms < info.time_end_ms


def test_loader_load_metrics(loader):
    case = "re2ob_checkoutservice_cpu_1"

    metrics = loader.load_metrics(
        case_id=case,
        long_format=True,
    )

    assert metrics.height > 0

    expected_columns = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "time",
        "timestamp",
        "metric_name",
        "value",
    }

    assert expected_columns.issubset(set(metrics.columns))

    assert metrics["value"].dtype.is_float()


def test_loader_load_logs(loader):
    case = "re2ob_checkoutservice_cpu_1"

    logs = loader.load_logs(case_id=case)

    assert logs.height > 0

    expected_columns = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "timestamp",
        "container_name",
        "message",
    }

    assert expected_columns.issubset(set(logs.columns))


def test_loader_load_traces(loader):
    case = "re2ob_checkoutservice_cpu_1"

    traces = loader.load_traces(case_id=case)

    assert traces.height > 0

    expected_columns = {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "traceID",
        "spanID",
        "serviceName",
        "startTimeMillis",
        "duration",
    }

    assert expected_columns.issubset(set(traces.columns))


def test_loader_load_complete_case(loader):
    case = "re2ob_checkoutservice_cpu_1"

    data = loader.load_case(
        case,
        include_metrics=True,
        include_logs=True,
        include_traces=True,
        metrics_long_format=True,
    )

    assert "metrics" in data
    assert "logs" in data
    assert "traces" in data

    assert data["metrics"].height > 0
    assert data["logs"].height > 0
    assert data["traces"].height > 0


def test_loader_time_ranges_are_consistent(loader):
    case = "re2ob_checkoutservice_cpu_1"

    info = loader.get_case_info(case)

    metrics = loader.load_metrics(
        case_id=case,
        long_format=True,
    )

    logs = loader.load_logs(case_id=case)
    traces = loader.load_traces(case_id=case)

    assert metrics["timestamp"].min() >= info.time_start_ms
    assert metrics["timestamp"].max() <= info.time_end_ms

    assert logs["timestamp_ms"].min() >= info.time_start_ms
    assert logs["timestamp_ms"].max() <= info.time_end_ms

    assert traces["timestamp_ms"].min() >= info.time_start_ms
    assert traces["timestamp_ms"].max() <= info.time_end_ms


if __name__ == "__main__":
    pytest.main([__file__, "-v"])