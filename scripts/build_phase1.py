import pandas as pd
from pathlib import Path

# ============================================================
# ECDT - PHASE 1
# Construction des données processed à partir des 60 cas
# ============================================================

ROOT = Path(".")
RAW = ROOT / "data" / "raw" / "RCAEval"
PROCESSED = ROOT / "data" / "processed"
SUBSET_PATH = PROCESSED / "ecdt_final_subset.csv"

# ------------------------------------------------------------
# 1. Chargement
# ------------------------------------------------------------

print("=" * 70)
print("ECDT - PHASE 1 DATA BUILDER")
print("=" * 70)

if not SUBSET_PATH.exists():
    raise FileNotFoundError(
        f"Fichier introuvable : {SUBSET_PATH}"
    )

subset = pd.read_csv(SUBSET_PATH)

print(f"\nCas sélectionnés : {len(subset)}")

required_columns = {
    "case",
    "dataset",
    "fault",
    "root_cause_service"
}

missing = required_columns - set(subset.columns)

if missing:
    raise ValueError(
        f"Colonnes manquantes dans ecdt_final_subset.csv : {missing}"
    )

# ------------------------------------------------------------
# 2. Création des dossiers
# ------------------------------------------------------------

directories = [
    PROCESSED / "incidents" / "cpu_saturation",
    PROCESSED / "incidents" / "db_latency",
    PROCESSED / "incidents" / "network_failure",
    PROCESSED / "metrics",
    PROCESSED / "logs",
    PROCESSED / "traces",
    PROCESSED / "topology",
    ROOT / "data" / "ground_truth",
]

for directory in directories:
    directory.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# 3. Fichiers incidents
# ------------------------------------------------------------

print("\n[1/7] Génération des fichiers incidents...")

cpu = subset[subset["fault"] == "cpu"].copy()
delay = subset[subset["fault"] == "delay"].copy()
network = subset[subset["fault"].isin(["loss", "socket"])].copy()

cpu.to_csv(
    PROCESSED / "incidents" / "cpu_saturation" / "cases.csv",
    index=False
)

delay.to_csv(
    PROCESSED / "incidents" / "db_latency" / "cases.csv",
    index=False
)

network.to_csv(
    PROCESSED / "incidents" / "network_failure" / "cases.csv",
    index=False
)

print(f"  CPU      : {len(cpu)}")
print(f"  DELAY    : {len(delay)}")
print(f"  NETWORK  : {len(network)}")

# ------------------------------------------------------------
# 4. Ground truth
# ------------------------------------------------------------

print("\n[2/7] Génération du ground truth...")

ground_truth_columns = [
    "case",
    "dataset",
    "suite",
    "system_name",
    "fault",
    "fault_description",
    "root_cause_service",
    "inject_time",
    "time_start",
    "time_end",
    "duration_minutes",
    "normal_timesteps",
    "faulty_timesteps",
]

# Certaines colonnes peuvent ne pas être dans le subset.
# On les récupère depuis cases.parquet si nécessaire.

cases_path = RAW / "cases.parquet"

if cases_path.exists():
    cases = pd.read_parquet(cases_path)
else:
    cases = subset.copy()

available_gt = [
    c for c in ground_truth_columns
    if c in cases.columns
]

ground_truth = cases[
    cases["case"].isin(subset["case"])
][available_gt].copy()

ground_truth.to_csv(
    ROOT / "data" / "ground_truth" / "target_incidents.csv",
    index=False
)

print(f"  Ground truth : {len(ground_truth)} cas")

# ------------------------------------------------------------
# 5. Extraction Metrics / Logs / Traces
# ------------------------------------------------------------

print("\n[3/7] Extraction Metrics...")
print("[4/7] Extraction Logs...")
print("[5/7] Extraction Traces...")

metrics_frames = []
logs_frames = []
traces_frames = []

missing_metrics = []
missing_logs = []
missing_traces = []

for i, row in subset.iterrows():

    case = str(row["case"])
    case_dir = RAW / case

    if not case_dir.exists():
        print(f"  ATTENTION dossier absent : {case}")
        continue

    # --------------------------------------------------------
    # Metrics
    # --------------------------------------------------------

    metrics_path = case_dir / "metrics.parquet"

    if metrics_path.exists():

        try:
            df = pd.read_parquet(metrics_path)

            if not df.empty:
                df.insert(0, "case", case)
                df.insert(1, "dataset", row["dataset"])
                df.insert(2, "fault", row["fault"])
                df.insert(3, "root_cause_service",
                          row["root_cause_service"])

                metrics_frames.append(df)

        except Exception as e:
            print(f"  Erreur metrics {case}: {e}")

    else:
        missing_metrics.append(case)

    # --------------------------------------------------------
    # Logs
    # --------------------------------------------------------

    logs_path = case_dir / "logs.parquet"

    if logs_path.exists():

        try:
            df = pd.read_parquet(logs_path)

            if not df.empty:
                df.insert(0, "case", case)
                df.insert(1, "dataset", row["dataset"])
                df.insert(2, "fault", row["fault"])
                df.insert(3, "root_cause_service",
                          row["root_cause_service"])

                logs_frames.append(df)

        except Exception as e:
            print(f"  Erreur logs {case}: {e}")

    else:
        missing_logs.append(case)

    # --------------------------------------------------------
    # Traces
    # --------------------------------------------------------

    traces_path = case_dir / "traces.parquet"

    if traces_path.exists():

        try:
            df = pd.read_parquet(traces_path)

            if not df.empty:
                df.insert(0, "case", case)
                df.insert(1, "dataset", row["dataset"])
                df.insert(2, "fault", row["fault"])
                df.insert(3, "root_cause_service",
                          row["root_cause_service"])

                traces_frames.append(df)

        except Exception as e:
            print(f"  Erreur traces {case}: {e}")

    else:
        missing_traces.append(case)

# ------------------------------------------------------------
# 6. Sauvegarde Metrics / Logs / Traces
# ------------------------------------------------------------

print("\n[6/7] Sauvegarde des données observabilité...")

if metrics_frames:
    all_metrics = pd.concat(
        metrics_frames,
        ignore_index=True
    )
else:
    all_metrics = pd.DataFrame()

if logs_frames:
    all_logs = pd.concat(
        logs_frames,
        ignore_index=True
    )
else:
    all_logs = pd.DataFrame()

if traces_frames:
    all_traces = pd.concat(
        traces_frames,
        ignore_index=True
    )
else:
    all_traces = pd.DataFrame()

all_metrics.to_csv(
    PROCESSED / "metrics" / "target_metrics.csv",
    index=False
)

all_logs.to_csv(
    PROCESSED / "logs" / "target_logs.csv",
    index=False
)

all_traces.to_csv(
    PROCESSED / "traces" / "target_traces.csv",
    index=False
)

print(f"  Metrics : {len(all_metrics)} lignes")
print(f"  Logs    : {len(all_logs)} lignes")
print(f"  Traces  : {len(all_traces)} lignes")

# ------------------------------------------------------------
# 7. Construction de la topologie
# ------------------------------------------------------------

print("\n[7/7] Construction de la topologie...")

services = set()
dependencies = set()

for _, row in subset.iterrows():

    case = str(row["case"])
    traces_path = RAW / case / "traces.parquet"

    if not traces_path.exists():
        continue

    try:

        # Lire uniquement les colonnes nécessaires
        trace_df = pd.read_parquet(
            traces_path,
            columns=[
                "spanID",
                "serviceName",
                "parentSpanID"
            ]
        )

        trace_df = trace_df.dropna(
            subset=["serviceName"]
        )

        # Services
        case_services = (
            trace_df["serviceName"]
            .astype(str)
            .unique()
        )

        services.update(case_services)

        # Mapping local au cas uniquement
        span_to_service = dict(
            zip(
                trace_df["spanID"].astype(str),
                trace_df["serviceName"].astype(str)
            )
        )

        # Dépendances
        if "parentSpanID" in trace_df.columns:

            for parent_span, child_service in zip(
                trace_df["parentSpanID"],
                trace_df["serviceName"]
            ):

                if pd.isna(parent_span):
                    continue

                parent_service = span_to_service.get(
                    str(parent_span)
                )

                if (
                    parent_service
                    and parent_service != str(child_service)
                ):
                    dependencies.add(
                        (
                            parent_service,
                            str(child_service)
                        )
                    )

        del trace_df
        del span_to_service

    except Exception as e:

        print(
            f"  Erreur topology {case}: {e}"
        )


# Services CSV
services_df = pd.DataFrame(
    sorted(services),
    columns=["service"]
)

services_df.to_csv(
    PROCESSED / "topology" / "services.csv",
    index=False
)


# Dependencies CSV
dependencies_df = pd.DataFrame(
    sorted(dependencies),
    columns=[
        "source_service",
        "target_service"
    ]
)

dependencies_df.to_csv(
    PROCESSED / "topology" / "dependencies.csv",
    index=False
)

print(
    f"  Services      : {len(services_df)}"
)

print(
    f"  Dependencies  : {len(dependencies_df)}"
)
# ------------------------------------------------------------
# Rapport des fichiers absents
# ------------------------------------------------------------

print("\n" + "=" * 70)
print("RAPPORT")
print("=" * 70)

print(f"\nCas sélectionnés        : {len(subset)}")
print(f"CPU                     : {len(cpu)}")
print(f"DELAY                   : {len(delay)}")
print(f"LOSS                    : {len(subset[subset.fault == 'loss'])}")
print(f"SOCKET                  : {len(subset[subset.fault == 'socket'])}")

print(f"\nMetrics lignes          : {len(all_metrics)}")
print(f"Logs lignes             : {len(all_logs)}")
print(f"Traces lignes           : {len(all_traces)}")

print(f"\nServices                : {len(services_df)}")
print(f"Dépendances             : {len(dependencies_df)}")

print(f"\nCas sans metrics        : {len(missing_metrics)}")
print(f"Cas sans logs           : {len(missing_logs)}")
print(f"Cas sans traces         : {len(missing_traces)}")

print("\nFichiers générés :")

files = [
    PROCESSED / "incidents" / "cpu_saturation" / "cases.csv",
    PROCESSED / "incidents" / "db_latency" / "cases.csv",
    PROCESSED / "incidents" / "network_failure" / "cases.csv",
    PROCESSED / "metrics" / "target_metrics.csv",
    PROCESSED / "logs" / "target_logs.csv",
    PROCESSED / "traces" / "target_traces.csv",
    PROCESSED / "topology" / "services.csv",
    PROCESSED / "topology" / "dependencies.csv",
    ROOT / "data" / "ground_truth" / "target_incidents.csv",
]

for f in files:
    status = "OK" if f.exists() else "MISSING"
    print(f"  [{status}] {f}")

print("\n" + "=" * 70)
print("PHASE 1 DATA BUILD TERMINÉ")
print("=" * 70)
