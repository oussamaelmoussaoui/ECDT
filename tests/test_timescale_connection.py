from pathlib import Path

from src.digital_twin.timescale_client import TimescaleClient
from src.digital_twin.timeseries_schema import initialize_schema
from src.digital_twin.timeseries_ingestion import ingest_normalized_events
from src.digital_twin.timeseries_queries import get_resource_history

from src.ingestion.dataset_loader import create_default_loader
from src.ingestion.schema_normalizer import SchemaNormalizer


CASE_ID = "re2ob_checkoutservice_cpu_1"


def main():
    print("=== FINAL TIMESCALEDB INTEGRATION VALIDATION ===")

    # ---------------------------------------------------------
    # 1. TimescaleDB
    # ---------------------------------------------------------

    print("\n[1] TimescaleDB connection")

    client = TimescaleClient()

    if not client.ping():
        raise RuntimeError("TimescaleDB connection failed")

    print("PASS: TimescaleDB connection")

    initialize_schema(client)

    print("PASS: TimescaleDB schema")

    # ---------------------------------------------------------
    # 2. Load real Phase 2 metrics
    # ---------------------------------------------------------

    print("\n[2] Loading real Phase 2 metrics")

    project_root = Path(".").resolve()

    loader = create_default_loader(project_root)

    metrics = loader.load_metrics(
        case_id=CASE_ID,
        long_format=True,
    )

    print(f"Metrics rows loaded: {metrics.height}")

    if metrics.height == 0:
        raise RuntimeError(
            f"No metrics found for case {CASE_ID}"
        )

    print("PASS: real Phase 2 metrics loaded")

    print("\nMetric columns:")
    print(metrics.columns)

    # ---------------------------------------------------------
    # 3. Normalize
    # ---------------------------------------------------------

    print("\n[3] Normalizing real metric data")

    normalizer = SchemaNormalizer()

    normalized = normalizer.normalize_metrics(metrics)

    print(f"Normalized rows: {normalized.height}")

    if normalized.height == 0:
        raise RuntimeError(
            "SchemaNormalizer returned no rows"
        )

    print("PASS: metric normalization")

    print("\nNormalized columns:")
    print(normalized.columns)

    # ---------------------------------------------------------
    # 4. Inspect normalized data
    # ---------------------------------------------------------

    print("\n[4] Inspecting normalized data")

    print(normalized.head(5))

    required_columns = {
        "case_id",
        "timestamp_ms",
        "service_name",
        "signal_type",
        "metric_name",
        "value",
    }

    missing = required_columns - set(normalized.columns)

    if missing:
        raise RuntimeError(
            f"Missing normalized columns: {sorted(missing)}"
        )

    print("PASS: normalized schema contains required fields")

    # ---------------------------------------------------------
    # 5. Select a real checkoutservice CPU observation
    # ---------------------------------------------------------

    print("\n[5] Selecting real checkoutservice CPU metric")

    selected = normalized.filter(
        (normalized["case_id"] == CASE_ID)
        & (normalized["service_name"] == "checkoutservice")
        & (normalized["signal_type"] == "cpu")
    )

    print(f"Matching normalized rows: {selected.height}")

    if selected.height == 0:
        raise RuntimeError(
            "No checkoutservice CPU metric found"
        )

    print(selected.head(5))

    print("PASS: real checkoutservice CPU metric found")

    # ---------------------------------------------------------
    # 6. Convert one normalized row to ingestion event
    # ---------------------------------------------------------

    print("\n[6] Preparing TimescaleDB ingestion event")

    row = selected.row(0, named=True)

    event = {
        "source": "metric",
        "case_id": row["case_id"],
        "timestamp_ms": row["timestamp_ms"],
        "service_name": row["service_name"],
        "signal_type": row["signal_type"],
        "metric_name": row["metric_name"],
        "value": row["value"],
    }

    print(event)

    print("PASS: ingestion event prepared")

    # ---------------------------------------------------------
    # 7. Ingest into TimescaleDB
    # ---------------------------------------------------------

    print("\n[7] Ingesting real Phase 2 metric")

    inserted = ingest_normalized_events(
        client,
        [event],
    )

    print(f"Inserted observations: {inserted}")

    if inserted != 1:
        raise RuntimeError(
            f"Expected 1 inserted observation, got {inserted}"
        )

    print("PASS: real metric inserted into TimescaleDB")

    # ---------------------------------------------------------
    # 8. Query by resource
    # ---------------------------------------------------------

    print("\n[8] Querying checkoutservice history")

    history = get_resource_history(
        client,
        resource_id="checkoutservice",
    )

    print(f"Rows returned: {len(history)}")

    if not history:
        raise RuntimeError(
            "No history found for checkoutservice"
        )

    print("PASS: resource history query")

    # ---------------------------------------------------------
    # 9. Verify persisted Phase 2 observation
    # ---------------------------------------------------------

    print("\n[9] Verifying persisted Phase 2 observation")

    matching = [
        row
        for row in history
        if row["case_id"] == CASE_ID
        and row["resource_id"] == "checkoutservice"
        and row["metric_type"] == "cpu"
    ]

    print(f"Matching rows: {len(matching)}")

    if not matching:
        raise RuntimeError(
            "Inserted Phase 2 metric was not found"
        )

    for row in matching[:5]:
        print(row)

    print("PASS: persisted Phase 2 metric")

    # ---------------------------------------------------------
    # Final
    # ---------------------------------------------------------

    print("\n==============================================")
    print("ALL FINAL TIMESCALEDB INTEGRATION VALIDATIONS PASSED")
    print("==============================================")


if __name__ == "__main__":
    main()