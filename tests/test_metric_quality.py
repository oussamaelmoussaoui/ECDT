"""Fast contract tests for explicit metric-value rejection auditing."""

import math

from src.ingestion.metric_quality import (
    has_finite_numeric_value,
    metric_value_rejection_reason,
    summarize_metric_value_rejections,
)


def _metric(value=1.0, **overrides):
    event = {
        "source": "metric",
        "service_name": "checkoutservice",
        "signal_type": "cpu",
        "metric_name": "checkoutservice_cpu",
        "value": value,
    }
    event.update(overrides)
    return event


def test_every_invalid_value_receives_an_explicit_reason():
    cases = [
        (_metric(None), "missing_value"),
        (_metric("bad"), "non_numeric"),
        (_metric(math.nan), "nan"),
        (_metric(math.inf), "infinite"),
        ({"source": "metric", "value": 1.0}, "schema_incompatible"),
    ]

    for event, expected in cases:
        assert metric_value_rejection_reason(event) == expected
        assert has_finite_numeric_value(event) is False

    assert metric_value_rejection_reason(_metric("12.5")) is None
    assert has_finite_numeric_value(_metric(-12.5)) is True


def test_rejection_summary_is_complete_and_multidimensional():
    events = [
        _metric(1.0),
        _metric(None),
        _metric(math.nan, signal_type="error", metric_name="svc_error"),
        _metric(math.inf, service_name="frontend"),
        _metric("bad", service_name="frontend"),
        {"source": "metric", "value": 1.0},
        {"source": "log", "value": None},
    ]

    summary = summarize_metric_value_rejections(events)

    assert summary["metric_rows_total"] == 6
    assert summary["valid_metric_rows"] == 1
    assert summary["rejected_metric_rows"] == 5
    assert summary["explained_rejected_rows"] == 5
    assert summary["all_rejections_explained"] is True
    assert summary["by_reason"] == {
        "infinite": 1,
        "missing_value": 1,
        "nan": 1,
        "non_numeric": 1,
        "schema_incompatible": 1,
    }
    assert sum(item["count"] for item in summary["details"]) == 5
    assert summary["by_service_name"]["frontend"] == 2
    assert summary["by_signal_type"]["error"] == 1
    assert summary["by_metric_name"]["svc_error"] == 1


def test_series_health_distinguishes_valid_partial_and_missing_series():
    events = [
        _metric(1.0, metric_name="a_cpu", service_name="a"),
        _metric(None, metric_name="a_cpu", service_name="a"),
        _metric(None, metric_name="b_cpu", service_name="b"),
        _metric(None, metric_name="b_cpu", service_name="b"),
        _metric(2.0, metric_name="c_cpu", service_name="c"),
        _metric(3.0, metric_name="c_cpu", service_name="c"),
    ]

    series = summarize_metric_value_rejections(events)["series_summary"]

    assert series["total_series"] == 3
    assert series["fully_valid_series"] == 1
    assert series["partially_observed_series"] == 1
    assert series["fully_missing_series"] == 1
