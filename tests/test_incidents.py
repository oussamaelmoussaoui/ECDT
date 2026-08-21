from pathlib import Path

from src.ingestion.dataset_loader import create_default_loader
from src.ingestion.schema_normalizer import SchemaNormalizer
from src.ingestion.anomaly_detector import AnomalyDetector


loader = create_default_loader(Path("."))
normalizer = SchemaNormalizer()
detector = AnomalyDetector()

cases = {
    "cpu": "re2ob_checkoutservice_cpu_1",
    "delay": "re2ob_checkoutservice_delay_1",
    "loss": "re2ob_checkoutservice_loss_1",
    "socket": "re2ob_checkoutservice_socket_1",
}

print("=" * 100)
print("FINAL CHECK — RCAEval Incident Detection")
print("=" * 100)

for expected_fault, case in cases.items():

    info = loader.get_case_info(case)

    data = loader.load_case(
        case,
        include_metrics=True,
        include_logs=False,
        include_traces=False,
        metrics_long_format=True,
    )

    events_df = normalizer.normalize_all(
        metrics=data["metrics"]
    )

    events = events_df.to_dicts()

    series = detector.build_series(events)

    # On utilise la série CPU comme dans la validation précédente.
    ts = series[("checkoutservice", "cpu")]

    anomalies = detector.detect_series(
        ts,
        baseline_end=info.inject_time_ms,
    )

    after = [
        a for a in anomalies
        if a.timestamp >= info.inject_time_ms
    ]

    incident_types = sorted(
        {
            a.incident_type.value
            for a in after
            if a.incident_type is not None
        }
    )

    print()
    print(f"CASE              : {case}")
    print(f"EXPECTED FAULT    : {expected_fault}")
    print(f"ANOMALIES         : {len(after)}")
    print(f"INCIDENT TYPES    : {incident_types}")

    if after:
        print(f"FIRST DETECTION   : {after[0].timestamp}")
        print(
            f"DETECTION DELAY   : "
            f"{(after[0].timestamp - info.inject_time_ms) / 1000:.2f}s"
        )

    print("STATUS            : ", end="")

    if after and incident_types:
        print("PASS")
    else:
        print("FAIL")


print()
print("=" * 100)
print("FINAL CHECK COMPLETED")
print("=" * 100)