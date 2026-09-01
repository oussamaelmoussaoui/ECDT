
"""
ECDT - Digital Twin
Temporal deviation analysis for the Diagnostic/Impact Agent.

The implementation uses metric_observations.timestamp (TIMESTAMPTZ),
not a non-existent `time` column.

Root-cause onset is evaluated only at or before the incident timestamp;
post-incident observations may be used separately for impact confirmation.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl
import psycopg2

from src.agents.diagnostic.diagnostic_models import MetricDeviation


DEFAULT_BASELINE_WINDOW = timedelta(minutes=30)
DEFAULT_LOOKAHEAD_WINDOW = timedelta(minutes=10)
DEFAULT_ROLLING_POINTS = 12
DEFAULT_Z_THRESHOLD = 3.0


def _get_connection():
    dsn = os.environ.get(
        "TIMESCALE_URI",
        "postgresql://ecdt:ecdt@localhost:5432/ecdt",
    )
    return psycopg2.connect(dsn)


def _ensure_utc(timestamp: datetime) -> datetime:
    """Normalize naive/aware datetimes to UTC."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def epoch_to_datetime(epoch: int | float) -> datetime:
    """
    Convert the Observer timestamp to UTC.

    Phase 2 defines timestamps as Unix epoch seconds.
    """
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc)


def resolve_metric_name(resource_id: str, signal_type: str) -> str:
    """
    Build the resource-local metric name used by the Phase 2/Timescale schema.

    Example:
        checkoutservice + cpu -> checkoutservice_cpu
    """
    return f"{resource_id}_{signal_type}"


def fetch_metric_window(
    resource_id: str,
    metric_name: str | None,
    center_time: datetime,
    baseline_window: timedelta = DEFAULT_BASELINE_WINDOW,
    lookahead_window: timedelta = DEFAULT_LOOKAHEAD_WINDOW,
    metric_type: str | None = None,
) -> pl.DataFrame:
    """
    Retrieve a metric series from TimescaleDB.

    If metric_name is provided, it is preferred. metric_type can be used
    as an additional filter and is useful for validating a resource-local
    metric family.
    """
    if not resource_id:
        raise ValueError("resource_id must not be empty.")
    if baseline_window <= timedelta(0):
        raise ValueError("baseline_window must be positive.")
    if lookahead_window < timedelta(0):
        raise ValueError("lookahead_window must be >= 0.")

    center_time = _ensure_utc(center_time)
    start = center_time - baseline_window
    end = center_time + lookahead_window

    filters = [
        "resource_id = %s",
        "timestamp BETWEEN %s AND %s",
    ]
    params: list[Any] = [resource_id, start, end]

    if metric_name:
        filters.append("metric_name = %s")
        params.append(metric_name)

    if metric_type:
        filters.append("metric_type = %s")
        params.append(metric_type)

    query = f"""
        SELECT
            timestamp,
            value
        FROM metric_observations
        WHERE {" AND ".join(filters)}
        ORDER BY timestamp ASC
    """

    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    if not rows:
        return pl.DataFrame(
            {
                "timestamp": [],
                "value": [],
            },
            schema_overrides={"timestamp": pl.Datetime("us", "UTC")},
        )

    frame = pl.DataFrame(
        rows,
        schema=["timestamp", "value"],
        orient="row",
    )

    if frame["timestamp"].dtype.time_zone is None:
        frame = frame.with_columns(
            pl.col("timestamp").dt.replace_time_zone("UTC")
        )

    return frame.sort("timestamp")


def detect_onset(
    series: pl.DataFrame,
    incident_time: datetime,
    rolling_points: int = DEFAULT_ROLLING_POINTS,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    include_post_incident: bool = False,
) -> tuple[bool, datetime | None, float]:
    """
    Detect the first statistically significant deviation.

    Baseline uses only preceding observations via shift(1), preventing
    look-ahead leakage.

    For root-cause analysis, include_post_incident should remain False.
    """
    if rolling_points < 2:
        raise ValueError("rolling_points must be >= 2.")
    if z_threshold <= 0:
        raise ValueError("z_threshold must be > 0.")

    if series.is_empty() or series.height < rolling_points + 2:
        return False, None, 0.0

    incident_time = _ensure_utc(incident_time)
    series = series.sort("timestamp")

    enriched = (
        series.with_columns(
            [
                pl.col("value")
                .shift(1)
                .rolling_mean(window_size=rolling_points)
                .alias("baseline_mean"),
                pl.col("value")
                .shift(1)
                .rolling_std(window_size=rolling_points)
                .alias("baseline_std"),
            ]
        )
        .drop_nulls(subset=["baseline_mean", "baseline_std"])
        .with_columns(
            pl.when(pl.col("baseline_std") > 0)
            .then(
                (
                    (pl.col("value") - pl.col("baseline_mean"))
                    / pl.col("baseline_std")
                ).abs()
            )
            .otherwise(0.0)
            .alias("z_score")
        )
    )

    if not include_post_incident:
        enriched = enriched.filter(
            pl.col("timestamp") <= incident_time
        )

    if enriched.is_empty():
        return False, None, 0.0

    max_z = enriched["z_score"].max()
    max_z_value = float(max_z) if max_z is not None else 0.0

    deviating = (
        enriched
        .filter(pl.col("z_score") >= z_threshold)
        .sort("timestamp")
    )

    if deviating.is_empty():
        return False, None, max_z_value

    onset = deviating.row(0, named=True)["timestamp"]
    return True, onset, float(deviating["z_score"].max())


def build_metric_deviation(
    resource_id: str,
    metric_name: str | None,
    metric_type: str | None,
    hop_distance: int,
    incident_time: datetime,
    rolling_points: int = DEFAULT_ROLLING_POINTS,
    z_threshold: float = DEFAULT_Z_THRESHOLD,
    allow_post_incident: bool = False,
) -> MetricDeviation:
    """Fetch a series and convert its deviation into the diagnostic model."""
    if hop_distance < 0:
        raise ValueError("hop_distance must be >= 0.")

    series = fetch_metric_window(
        resource_id=resource_id,
        metric_name=metric_name,
        metric_type=metric_type,
        center_time=incident_time,
    )

    deviated, onset, max_z = detect_onset(
        series,
        incident_time=incident_time,
        rolling_points=rolling_points,
        z_threshold=z_threshold,
        include_post_incident=allow_post_incident,
    )

    effective_metric_name = metric_name or (
        f"{resource_id}_{metric_type}" if metric_type else "unknown"
    )

    return MetricDeviation(
        resource_id=resource_id,
        metric_name=effective_metric_name,
        hop_distance=hop_distance,
        onset_timestamp=onset,
        max_z_score=max_z,
        deviated=deviated,
    )
