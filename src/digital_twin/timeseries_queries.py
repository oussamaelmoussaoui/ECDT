"""
ECDT - Digital Twin
Time-series queries.

This module provides read-only analytical queries over the ECDT
TimescaleDB metric history.

Main capabilities
-----------------

1. Retrieve the history of a resource.
2. Retrieve one metric for a resource.
3. Retrieve metrics during a time period.
4. Retrieve metrics around an incident timestamp.
5. Aggregate a metric using TimescaleDB time_bucket.

These queries prepare the temporal correlation layer required by
future ECDT RCA components.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .timescale_client import TimescaleClient
from .timeseries_schema import METRIC_TABLE


# ---------------------------------------------------------------------------
# Resource history
# ---------------------------------------------------------------------------


def get_resource_history(
    client: TimescaleClient,
    resource_id: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Retrieve all metric observations for a resource.

    If start_time and end_time are provided, the query is restricted
    to that temporal window.
    """

    query = f"""
    SELECT
        resource_id,
        timestamp,
        value,
        metric_type,
        metric_name,
        case_id,
        dataset,
        fault
    FROM {METRIC_TABLE}
    WHERE resource_id = %s
    """

    parameters: list[Any] = [
        resource_id,
    ]

    if start_time is not None:
        query += """
        AND timestamp >= %s
        """

        parameters.append(
            start_time
        )

    if end_time is not None:
        query += """
        AND timestamp <= %s
        """

        parameters.append(
            end_time
        )

    query += """
    ORDER BY timestamp ASC;
    """

    return client.execute(
        query,
        tuple(parameters),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Metric history
# ---------------------------------------------------------------------------


def get_metric_history(
    client: TimescaleClient,
    resource_id: str,
    metric_name: str,
    start_time: datetime,
    end_time: datetime,
) -> list[dict[str, Any]]:
    """
    Retrieve one metric for one resource during a time window.
    """

    query = f"""
    SELECT
        resource_id,
        timestamp,
        value,
        metric_type,
        metric_name,
        case_id,
        dataset,
        fault
    FROM {METRIC_TABLE}
    WHERE resource_id = %s
      AND metric_name = %s
      AND timestamp >= %s
      AND timestamp <= %s
    ORDER BY timestamp ASC;
    """

    return client.execute(
        query,
        (
            resource_id,
            metric_name,
            start_time,
            end_time,
        ),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Metric type history
# ---------------------------------------------------------------------------


def get_metric_type_history(
    client: TimescaleClient,
    resource_id: str,
    metric_type: str,
    start_time: datetime,
    end_time: datetime,
) -> list[dict[str, Any]]:
    """
    Retrieve all metrics of a given type for a resource.

    Example:

        metric_type = "cpu"
    """

    query = f"""
    SELECT
        resource_id,
        timestamp,
        value,
        metric_type,
        metric_name,
        case_id,
        dataset,
        fault
    FROM {METRIC_TABLE}
    WHERE resource_id = %s
      AND metric_type = %s
      AND timestamp >= %s
      AND timestamp <= %s
    ORDER BY timestamp ASC;
    """

    return client.execute(
        query,
        (
            resource_id,
            metric_type,
            start_time,
            end_time,
        ),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Around timestamp
# ---------------------------------------------------------------------------


def get_metrics_around_timestamp(
    client: TimescaleClient,
    resource_id: str,
    timestamp: datetime,
    window_minutes: int = 5,
) -> list[dict[str, Any]]:
    """
    Retrieve metric observations around a timestamp.

    Default window:

        timestamp - 5 minutes
        timestamp + 5 minutes

    This function is intended for future incident/temporal correlation.
    """

    if window_minutes < 0:
        raise ValueError(
            "window_minutes must be >= 0."
        )

    query = f"""
    SELECT
        resource_id,
        timestamp,
        value,
        metric_type,
        metric_name,
        case_id,
        dataset,
        fault
    FROM {METRIC_TABLE}
    WHERE resource_id = %s
      AND timestamp BETWEEN
          (%s - (%s * INTERVAL '1 minute'))
          AND
          (%s + (%s * INTERVAL '1 minute'))
    ORDER BY timestamp ASC;
    """

    return client.execute(
        query,
        (
            resource_id,
            timestamp,
            window_minutes,
            timestamp,
            window_minutes,
        ),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Time bucket aggregation
# ---------------------------------------------------------------------------


def aggregate_metric_history(
    client: TimescaleClient,
    resource_id: str,
    metric_type: str,
    start_time: datetime,
    end_time: datetime,
    bucket: str = "1 minute",
) -> list[dict[str, Any]]:
    """
    Aggregate metric observations into TimescaleDB time buckets.

    Returns:

        bucket
        average
        minimum
        maximum
        sample_count
    """

    allowed_buckets = {
        "1 second",
        "10 seconds",
        "30 seconds",
        "1 minute",
        "5 minutes",
        "10 minutes",
        "15 minutes",
        "30 minutes",
        "1 hour",
        "1 day",
    }

    if bucket not in allowed_buckets:
        raise ValueError(
            f"Unsupported bucket: {bucket!r}. "
            f"Allowed values: {sorted(allowed_buckets)}"
        )

    query = f"""
    SELECT
        time_bucket(%s, timestamp) AS bucket,
        AVG(value) AS average,
        MIN(value) AS minimum,
        MAX(value) AS maximum,
        COUNT(*) AS sample_count
    FROM {METRIC_TABLE}
    WHERE resource_id = %s
      AND metric_type = %s
      AND timestamp >= %s
      AND timestamp <= %s
    GROUP BY bucket
    ORDER BY bucket ASC;
    """

    return client.execute(
        query,
        (
            bucket,
            resource_id,
            metric_type,
            start_time,
            end_time,
        ),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Case history
# ---------------------------------------------------------------------------


def get_case_metric_history(
    client: TimescaleClient,
    case_id: str,
) -> list[dict[str, Any]]:
    """
    Retrieve all metric observations belonging to one RCAEval case.
    """

    query = f"""
    SELECT
        resource_id,
        timestamp,
        value,
        metric_type,
        metric_name,
        case_id,
        dataset,
        fault
    FROM {METRIC_TABLE}
    WHERE case_id = %s
    ORDER BY timestamp ASC;
    """

    return client.execute(
        query,
        (case_id,),
        fetch=True,
    )


# ---------------------------------------------------------------------------
# Resource list
# ---------------------------------------------------------------------------


def list_resources(
    client: TimescaleClient,
) -> list[str]:
    """
    Return all resource identifiers currently represented in TimescaleDB.
    """

    query = f"""
    SELECT DISTINCT resource_id
    FROM {METRIC_TABLE}
    ORDER BY resource_id ASC;
    """

    rows = client.execute(
        query,
        fetch=True,
    )

    return [
        row["resource_id"]
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Count
# ---------------------------------------------------------------------------


def count_observations(
    client: TimescaleClient,
    resource_id: str | None = None,
) -> int:
    """
    Count metric observations.

    If resource_id is provided, count only observations belonging
    to that resource.
    """

    if resource_id is None:

        query = f"""
        SELECT COUNT(*) AS count
        FROM {METRIC_TABLE};
        """

        rows = client.execute(
            query,
            fetch=True,
        )

    else:

        query = f"""
        SELECT COUNT(*) AS count
        FROM {METRIC_TABLE}
        WHERE resource_id = %s;
        """

        rows = client.execute(
            query,
            (resource_id,),
            fetch=True,
        )

    if not rows:
        return 0

    return int(
        rows[0]["count"]
    )