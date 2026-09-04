import math
import inspect

import pytest

from scripts import run_observer_pipeline as pipeline


def test_metric_value_rejection_reason():
    """Each rejected value must receive one explicit reason."""

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": None}
        )
        == "missing_value"
    )

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": "not-a-number"}
        )
        == "non_numeric"
    )

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": math.nan}
        )
        == "nan"
    )

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": math.inf}
        )
        == "infinite"
    )

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": -math.inf}
        )
        == "infinite"
    )

    assert (
        pipeline._metric_value_rejection_reason(
            {"value": "12.5"}
        )
        is None
    )


def test_summarize_metric_value_rejections():
    """The audit must explain every rejected metric row."""

    events = [
        {
            "source": "metric",
            "service_name": "checkoutservice",
            "signal_type": "cpu",
            "metric_name": "checkoutservice_cpu",
            "value": 12.5,
        },
        {
            "source": "metric",
            "service_name": "checkoutservice",
            "signal_type": "cpu",
            "metric_name": "checkoutservice_cpu",
            "value": None,
        },
        {
            "source": "metric",
            "service_name": "frontend",
            "signal_type": "latency-50",
            "metric_name": "frontend_latency-50",
            "value": "invalid",
        },
        {
            "source": "metric",
            "service_name": "frontend",
            "signal_type": "latency-50",
            "metric_name": "frontend_latency-50",
            "value": math.nan,
        },
        {
            "source": "metric",
            "service_name": "redis",
            "signal_type": "socket",
            "metric_name": "redis_socket",
            "value": math.inf,
        },
        {
            # Non-metric events must not enter this audit.
            "source": "log",
            "service_name": "frontend",
            "value": None,
        },
    ]

    summary = (
        pipeline._summarize_metric_value_rejections(
            events
        )
    )

    assert summary["metric_rows_total"] == 5
    assert summary["valid_metric_rows"] == 1
    assert summary["rejected_metric_rows"] == 4

    assert summary["by_reason"] == {
        "infinite": 1,
        "missing_value": 1,
        "nan": 1,
        "non_numeric": 1,
    }

    assert summary["by_service_name"] == {
        "checkoutservice": 1,
        "frontend": 2,
        "redis": 1,
    }

    assert summary["by_signal_type"] == {
        "cpu": 1,
        "latency-50": 2,
        "socket": 1,
    }

    assert summary["by_metric_name"] == {
        "checkoutservice_cpu": 1,
        "frontend_latency-50": 2,
        "redis_socket": 1,
    }

    assert sum(
        item["count"]
        for item in summary["details"]
    ) == 4


def test_rejection_summary_classifies_series_health():
    """The audit must distinguish useful and fully empty series."""

    events = [
        {
            "source": "metric",
            "service_name": "service-a",
            "signal_type": "cpu",
            "metric_name": "service-a_cpu",
            "value": 1.0,
        },
        {
            "source": "metric",
            "service_name": "service-a",
            "signal_type": "cpu",
            "metric_name": "service-a_cpu",
            "value": None,
        },
        {
            "source": "metric",
            "service_name": "service-b",
            "signal_type": "mem",
            "metric_name": "service-b_mem",
            "value": None,
        },
        {
            "source": "metric",
            "service_name": "service-b",
            "signal_type": "mem",
            "metric_name": "service-b_mem",
            "value": None,
        },
        {
            "source": "metric",
            "service_name": "service-c",
            "signal_type": "socket",
            "metric_name": "service-c_socket",
            "value": 2.0,
        },
        {
            "source": "metric",
            "service_name": "service-c",
            "signal_type": "socket",
            "metric_name": "service-c_socket",
            "value": 3.0,
        },
    ]

    summary = (
        pipeline._summarize_metric_value_rejections(
            events
        )
    )

    series = summary["series_summary"]

    assert series["total_series"] == 3
    assert series["fully_valid_series"] == 1
    assert series["partially_observed_series"] == 1
    assert series["fully_missing_series"] == 1

    details = {
        item["metric_name"]: item
        for item in series["details"]
    }

    assert details["service-a_cpu"]["status"] == (
        "partially_observed"
    )
    assert details["service-a_cpu"]["valid_rows"] == 1
    assert details["service-a_cpu"]["rejected_rows"] == 1

    assert details["service-b_mem"]["status"] == (
        "fully_missing"
    )
    assert details["service-b_mem"]["valid_rows"] == 0

    assert details["service-c_socket"]["status"] == (
        "fully_valid"
    )
    assert details["service-c_socket"]["rejected_rows"] == 0


def test_pipeline_does_not_reference_caused_by():
    """The operational pipeline must not consult RCAEval root causes."""

    run_source = inspect.getsource(
        pipeline.run
    )

    assert "CAUSED_BY" not in run_source.upper()


def test_topology_coverage_ratio():
    """Coverage is deterministic and explicit for empty input."""

    assert pipeline._topology_coverage_ratio(4, 3) == 0.75
    assert pipeline._topology_coverage_ratio(4, 4) == 1.0
    assert pipeline._topology_coverage_ratio(0, 0) is None

    with pytest.raises(ValueError):
        pipeline._topology_coverage_ratio(3, 4)

    with pytest.raises(ValueError):
        pipeline._topology_coverage_ratio(-1, 0)


def test_pipeline_filters_operational_topology_by_source():
    """Ground-truth-only services cannot become Observer resources."""

    run_source = inspect.getsource(pipeline.run)

    assert "source: $topology_source" in run_source
    assert '"topology_source": "topology"' in run_source
    assert '"topology_coverage_ratio"' in run_source
    assert '"outside_topology_services"' in run_source
