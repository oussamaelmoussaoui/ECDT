"""
ECDT - Digital Twin
Normalized metric ingestion into TimescaleDB.

This module bridges Phase 2 normalization and the Digital Twin
time-series store.

Input
-----

Normalized metric events containing at least:

    case_id
    timestamp_ms
    service_name
    signal_type
    metric_name
    value

Only metric events are persisted.

Mapping
-------

NormalizedEvent.service_name
        ->
metric_observations.resource_id

NormalizedEvent.timestamp_ms
        ->
metric_observations.timestamp

NormalizedEvent.signal_type
        ->
metric_observations.metric_type

NormalizedEvent.metric_name
        ->
metric_observations.metric_name

NormalizedEvent.value
        ->
metric_observations.value
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .timescale_client import TimescaleClient
from .timeseries_schema import METRIC_TABLE


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INSERT_METRIC_QUERY = f"""
INSERT INTO {METRIC_TABLE} (
    resource_id,
    timestamp,
    value,
    metric_type,
    metric_name,
    case_id,
    dataset
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_attribute(
    event: Any,
    name: str,
    default: Any = None,
) -> Any:
    """
    Read a field from either an object or a dictionary.

    This keeps the ingestion layer compatible with the Phase 2
    NormalizedEvent dataclass as well as simple test dictionaries.
    """

    if isinstance(event, dict):
        return event.get(name, default)

    return getattr(
        event,
        name,
        default,
    )


def _timestamp_from_ms(
    timestamp_ms: int,
) -> datetime:
    """
    Convert Unix epoch milliseconds to a timezone-aware UTC datetime.
    """

    return datetime.fromtimestamp(
        int(timestamp_ms) / 1000.0,
        tz=timezone.utc,
    )


def _event_to_row(
    event: Any,
) -> tuple[Any, ...]:
    """
    Convert one normalized metric event into a TimescaleDB row.
    """

    source = _get_attribute(
        event,
        "source",
    )

    if source not in (None, "", "metric"):
        raise ValueError(
            "Only metric events can be ingested into "
            "metric_observations."
        )

    case_id = _get_attribute(
        event,
        "case_id",
    )

    timestamp_ms = _get_attribute(
        event,
        "timestamp_ms",
    )

    service_name = _get_attribute(
        event,
        "service_name",
    )

    signal_type = _get_attribute(
        event,
        "signal_type",
    )

    metric_name = _get_attribute(
        event,
        "metric_name",
    )

    value = _get_attribute(
        event,
        "value",
    )

    dataset = _get_attribute(
        event,
        "dataset",
    )

    if not case_id:
        raise ValueError(
            "Metric event is missing case_id."
        )

    if timestamp_ms is None:
        raise ValueError(
            "Metric event is missing timestamp_ms."
        )

    if not service_name:
        raise ValueError(
            "Metric event is missing service_name."
        )

    if not signal_type:
        raise ValueError(
            "Metric event is missing signal_type."
        )

    if not metric_name:
        raise ValueError(
            "Metric event is missing metric_name."
        )

    if value is None:
        raise ValueError(
            "Metric event is missing value."
        )

    return (
        str(service_name),
        _timestamp_from_ms(timestamp_ms),
        float(value),
        str(signal_type),
        str(metric_name),
        str(case_id),
        str(dataset) if dataset is not None else None,
    )


# ---------------------------------------------------------------------------
# Public ingestion API
# ---------------------------------------------------------------------------


def determine_case_ingestion_action(
    *,
    case_id: str,
    expected_valid_rows: int,
    existing_rows: int,
    unique_rows: int,
    skip_requested: bool = False,
) -> str:
    """Classify a case before ingestion without modifying TimescaleDB."""

    if not case_id:
        raise ValueError("case_id must not be empty.")

    counts = {
        "expected_valid_rows": expected_valid_rows,
        "existing_rows": existing_rows,
        "unique_rows": unique_rows,
    }

    for name, value in counts.items():
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{name} must be an integer.")
        if value < 0:
            raise ValueError(f"{name} must be >= 0.")

    if expected_valid_rows == 0:
        raise RuntimeError(
            f"No valid metric row is available for case {case_id!r}."
        )

    if unique_rows > existing_rows:
        raise RuntimeError(
            "TimescaleDB returned an incoherent metric state for case "
            f"{case_id!r}: {existing_rows} rows for {unique_rows} "
            "unique temporal keys."
        )

    if existing_rows != unique_rows:
        raise RuntimeError(
            f"TimescaleDB contains duplicates for case {case_id!r}: "
            f"{existing_rows} rows for {unique_rows} unique temporal keys."
        )

    if existing_rows == 0:
        if skip_requested:
            raise RuntimeError(
                "--skip-timescale-ingestion was requested, but no metric "
                f"exists for case {case_id!r}."
            )
        return "insert"

    if existing_rows == expected_valid_rows:
        return "skipped_by_flag" if skip_requested else "already_present"

    raise RuntimeError(
        "TimescaleDB contains a partial or incoherent state for case "
        f"{case_id!r}: {existing_rows} existing rows versus "
        f"{expected_valid_rows} expected rows. No automatic insertion "
        "was performed."
    )


def ingest_metric_events(
    client: TimescaleClient,
    events: Iterable[Any],
) -> int:
    """
    Ingest normalized metric events into TimescaleDB.

    Parameters:
        client:
            TimescaleDB client.

        events:
            Iterable of Phase 2 normalized metric events.

    Returns:
        Number of inserted rows.
    """

    rows = []

    for event in events:

        source = _get_attribute(
            event,
            "source",
        )

        if source not in (None, "", "metric"):
            continue

        rows.append(
            _event_to_row(event)
        )

    if not rows:
        return 0

    return client.execute_many(
        INSERT_METRIC_QUERY,
        rows,
    )


def ingest_normalized_events(
    client: TimescaleClient,
    events: Iterable[Any],
) -> int:
    """
    Ingest a mixed collection of normalized events.

    Logs and traces are ignored because TimescaleDB at this stage is
    dedicated to metric time series.

    Returns:
        Number of metric observations inserted.
    """

    return ingest_metric_events(
        client,
        events,
    )


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def initialize_and_ingest(
    client: TimescaleClient,
    events: Iterable[Any],
) -> int:
    """
    Initialize the TimescaleDB schema and ingest normalized metrics.

    Schema initialization is intentionally kept outside the low-level
    client.
    """

    from .timeseries_schema import initialize_schema

    initialize_schema(client)

    return ingest_normalized_events(
        client,
        events,
    )
