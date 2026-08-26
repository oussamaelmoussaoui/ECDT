"""
ECDT - Digital Twin
TimescaleDB time-series schema.

This module defines the persistent schema used to store normalized
metric observations.

Main table:

    metric_observations

The table is converted into a TimescaleDB hypertable using ``timestamp``
as the time dimension.

Responsibilities
----------------
- Create the metric observation table.
- Create the TimescaleDB hypertable.
- Create indexes.
- Verify the schema.

This module does NOT:
- ingest metrics;
- execute analytical queries.
"""

from __future__ import annotations

from typing import Any

from .timescale_client import TimescaleClient


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

METRIC_TABLE = "metric_observations"

CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {METRIC_TABLE} (
    id BIGSERIAL,
    resource_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,

    value DOUBLE PRECISION NOT NULL,

    metric_type TEXT NOT NULL,
    metric_name TEXT NOT NULL,

    case_id TEXT,
    dataset TEXT,
    fault TEXT,

    PRIMARY KEY (id, timestamp)
);
"""


CREATE_HYPERTABLE_QUERY = f"""
SELECT create_hypertable(
    '{METRIC_TABLE}',
    'timestamp',
    if_not_exists => TRUE
);
"""


CREATE_RESOURCE_TIME_INDEX = f"""
CREATE INDEX IF NOT EXISTS
idx_metric_observations_resource_timestamp
ON {METRIC_TABLE} (
    resource_id,
    timestamp DESC
);
"""


CREATE_METRIC_TIME_INDEX = f"""
CREATE INDEX IF NOT EXISTS
idx_metric_observations_metric_timestamp
ON {METRIC_TABLE} (
    metric_name,
    timestamp DESC
);
"""


CREATE_CASE_TIME_INDEX = f"""
CREATE INDEX IF NOT EXISTS
idx_metric_observations_case_timestamp
ON {METRIC_TABLE} (
    case_id,
    timestamp DESC
);
"""


# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------


def create_metric_table(
    client: TimescaleClient,
) -> None:
    """
    Create the metric observation table.
    """

    client.execute(CREATE_TABLE_QUERY)


def create_hypertable(
    client: TimescaleClient,
) -> None:
    """
    Convert metric_observations into a TimescaleDB hypertable.

    ``if_not_exists`` makes the operation safe to run repeatedly.
    """

    client.execute(CREATE_HYPERTABLE_QUERY)


def create_indexes(
    client: TimescaleClient,
) -> None:
    """
    Create indexes required by the main Digital Twin queries.
    """

    client.execute(CREATE_RESOURCE_TIME_INDEX)
    client.execute(CREATE_METRIC_TIME_INDEX)
    client.execute(CREATE_CASE_TIME_INDEX)


def initialize_schema(
    client: TimescaleClient,
) -> None:
    """
    Initialize the complete ECDT time-series schema.

    The operation is intentionally idempotent.
    """

    create_metric_table(client)
    create_hypertable(client)
    create_indexes(client)


# ---------------------------------------------------------------------------
# Schema inspection
# ---------------------------------------------------------------------------


def get_table_columns(
    client: TimescaleClient,
) -> list[dict[str, Any]]:
    """
    Return the columns of metric_observations.
    """

    query = """
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = %s
    ORDER BY ordinal_position;
    """

    return client.execute(
        query,
        (METRIC_TABLE,),
        fetch=True,
    )


def is_hypertable(
    client: TimescaleClient,
) -> bool:
    """
    Check whether metric_observations is registered as a hypertable.
    """

    query = """
    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public'
          AND hypertable_name = %s
    ) AS is_hypertable;
    """

    rows = client.execute(
        query,
        (METRIC_TABLE,),
        fetch=True,
    )

    return bool(
        rows
        and rows[0].get("is_hypertable")
    )


def validate_schema(
    client: TimescaleClient,
) -> bool:
    """
    Validate the essential ECDT time-series schema.

    Returns:
        True when the table exists and is a hypertable.
    """

    columns = get_table_columns(client)

    required_columns = {
        "resource_id",
        "timestamp",
        "value",
        "metric_type",
        "metric_name",
        "case_id",
        "dataset",
        "fault",
    }

    existing_columns = {
        row["column_name"]
        for row in columns
    }

    missing = (
        required_columns
        - existing_columns
    )

    if missing:
        return False

    return is_hypertable(client)