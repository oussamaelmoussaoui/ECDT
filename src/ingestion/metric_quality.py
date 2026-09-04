"""Metric-value quality audit used before detection and persistence.

The audit is deliberately independent from Polars so it can inspect normalized
records at the operational boundary and explain every rejected metric value.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


def metric_value_rejection_reason(
    event: Mapping[str, Any],
) -> str | None:
    """Return the explicit rejection reason for one metric record."""

    required_fields = {
        "service_name",
        "signal_type",
        "metric_name",
        "value",
    }
    if not required_fields.issubset(event):
        return "schema_incompatible"

    value = event["value"]
    if value is None:
        return "missing_value"

    if isinstance(value, bool):
        return "non_numeric"

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return "non_numeric"

    if math.isnan(numeric_value):
        return "nan"
    if math.isinf(numeric_value):
        return "infinite"
    return None


def has_finite_numeric_value(
    event: Mapping[str, Any],
) -> bool:
    """Return whether a metric record can safely enter numeric processing."""

    return metric_value_rejection_reason(event) is None


def summarize_metric_value_rejections(
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Explain rejected metric values by reason, signal, metric and service."""

    metric_rows_total = 0
    valid_metric_rows = 0
    by_reason: Counter[str] = Counter()
    by_service_name: Counter[str] = Counter()
    by_signal_type: Counter[str] = Counter()
    by_metric_name: Counter[str] = Counter()
    detail_counts: Counter[tuple[str, str, str, str]] = Counter()
    series_counts: dict[tuple[str, str, str], dict[str, Any]] = {}

    for event in events:
        if event.get("source") != "metric":
            continue

        metric_rows_total += 1
        service_name = str(event.get("service_name") or "unknown")
        signal_type = str(event.get("signal_type") or "unknown")
        metric_name = str(event.get("metric_name") or "unknown")
        series_key = (service_name, signal_type, metric_name)
        state = series_counts.setdefault(
            series_key,
            {
                "total_rows": 0,
                "valid_rows": 0,
                "rejected_rows": 0,
                "reasons": Counter(),
            },
        )
        state["total_rows"] += 1

        reason = metric_value_rejection_reason(event)
        if reason is None:
            valid_metric_rows += 1
            state["valid_rows"] += 1
            continue

        state["rejected_rows"] += 1
        state["reasons"][reason] += 1
        by_reason[reason] += 1
        by_service_name[service_name] += 1
        by_signal_type[signal_type] += 1
        by_metric_name[metric_name] += 1
        detail_counts[(service_name, signal_type, metric_name, reason)] += 1

    details = [
        {
            "service_name": service_name,
            "signal_type": signal_type,
            "metric_name": metric_name,
            "reason": reason,
            "count": count,
        }
        for (service_name, signal_type, metric_name, reason), count
        in sorted(detail_counts.items())
    ]

    series_status_counts: Counter[str] = Counter()
    series_details: list[dict[str, Any]] = []
    for (service_name, signal_type, metric_name), state in sorted(
        series_counts.items()
    ):
        total_rows = int(state["total_rows"])
        valid_rows = int(state["valid_rows"])
        rejected_rows = int(state["rejected_rows"])
        if valid_rows == 0:
            status = "fully_missing"
        elif rejected_rows == 0:
            status = "fully_valid"
        else:
            status = "partially_observed"
        series_status_counts[status] += 1
        series_details.append(
            {
                "service_name": service_name,
                "signal_type": signal_type,
                "metric_name": metric_name,
                "status": status,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "rejected_rows": rejected_rows,
                "rejection_ratio": (
                    rejected_rows / total_rows if total_rows else None
                ),
                "reasons": dict(sorted(state["reasons"].items())),
            }
        )

    rejected_metric_rows = metric_rows_total - valid_metric_rows
    explained_rejections = sum(by_reason.values())
    return {
        "metric_rows_total": metric_rows_total,
        "valid_metric_rows": valid_metric_rows,
        "rejected_metric_rows": rejected_metric_rows,
        "explained_rejected_rows": explained_rejections,
        "all_rejections_explained": (
            rejected_metric_rows == explained_rejections
        ),
        "by_reason": dict(sorted(by_reason.items())),
        "by_service_name": dict(sorted(by_service_name.items())),
        "by_signal_type": dict(sorted(by_signal_type.items())),
        "by_metric_name": dict(sorted(by_metric_name.items())),
        "details": details,
        "series_summary": {
            "total_series": len(series_details),
            "fully_valid_series": series_status_counts["fully_valid"],
            "partially_observed_series": (
                series_status_counts["partially_observed"]
            ),
            "fully_missing_series": series_status_counts["fully_missing"],
            "details": series_details,
        },
    }
