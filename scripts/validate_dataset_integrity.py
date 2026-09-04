"""Aggregate RCAEval telemetry statistics and validate every manifest case."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.dataset_integrity import validate_case_integrity  # noqa: E402


REQUIRED_COLUMNS = {
    "metrics": {"case", "time"},
    "logs": {"case", "timestamp", "container_name"},
    "traces": {"case", "startTimeMillis", "serviceName"},
    "ground_truth": {
        "case",
        "dataset",
        "fault",
        "root_cause_service",
        "time_start",
        "inject_time",
        "time_end",
    },
}


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Manifest absent : {path}")

    manifest = json.loads(path.read_text(encoding="utf-8"))
    cases = manifest.get("cases")
    files = manifest.get("files")

    if not isinstance(cases, list) or not cases:
        raise ValueError("Le manifeste ne contient aucun cas.")
    if not isinstance(files, dict):
        raise ValueError("Le manifeste ne contient pas la section files.")

    return manifest


def _resolve_source_paths(
    project_root: Path,
    manifest: dict[str, Any],
) -> dict[str, Path]:
    paths: dict[str, Path] = {}

    for source in ("metrics", "logs", "traces", "ground_truth"):
        entry = manifest["files"].get(source)
        if not isinstance(entry, dict) or not entry.get("path"):
            raise ValueError(f"Fichier {source!r} absent du manifeste.")

        path = project_root / str(entry["path"])
        if not path.is_file():
            raise FileNotFoundError(f"Fichier {source!r} absent : {path}")
        paths[source] = path

    return paths


def _scan_csv(path: Path, source: str) -> tuple[pl.LazyFrame, list[str]]:
    frame = pl.scan_csv(
        path,
        try_parse_dates=False,
        infer_schema_length=1000,
    )
    columns = frame.collect_schema().names()
    missing = REQUIRED_COLUMNS.get(source, set()) - set(columns)
    if missing:
        raise ValueError(
            f"Schéma {source} invalide. Colonnes absentes : {sorted(missing)}"
        )
    return frame, columns


def _validate_ground_truth(
    path: Path,
    cases: list[dict[str, Any]],
) -> None:
    """Prove that the manifest still matches the current ground-truth CSV."""

    frame, _ = _scan_csv(path, "ground_truth")
    actual_rows = frame.select(
        sorted(REQUIRED_COLUMNS["ground_truth"])
    ).collect()

    actual_by_case = {
        str(row["case"]): row
        for row in actual_rows.to_dicts()
    }
    if len(actual_by_case) != actual_rows.height:
        raise ValueError("La vérité terrain contient des case_id dupliqués.")

    expected_ids = {str(case["case_id"]) for case in cases}
    actual_ids = set(actual_by_case)
    if actual_ids != expected_ids:
        raise ValueError(
            "Le manifeste et la vérité terrain ne contiennent pas les mêmes cas. "
            f"Absents du CSV : {sorted(expected_ids - actual_ids)}; "
            f"absents du manifeste : {sorted(actual_ids - expected_ids)}"
        )

    mismatches: list[str] = []
    for case in cases:
        case_id = str(case["case_id"])
        actual = actual_by_case[case_id]
        expected_values = {
            "dataset": str(case["dataset"]),
            "fault": str(case["fault"]),
            "root_cause_service": str(case["root_cause_service"]),
            "time_start_ms": int(case["time_start_ms"]),
            "inject_time_ms": int(case["inject_time_ms"]),
            "time_end_ms": int(case["time_end_ms"]),
        }
        actual_values = {
            "dataset": str(actual["dataset"]),
            "fault": str(actual["fault"]),
            "root_cause_service": str(actual["root_cause_service"]),
            "time_start_ms": int(float(actual["time_start"]) * 1000),
            "inject_time_ms": int(float(actual["inject_time"]) * 1000),
            "time_end_ms": int(float(actual["time_end"]) * 1000),
        }
        if actual_values != expected_values:
            mismatches.append(case_id)

    if mismatches:
        raise ValueError(
            "La vérité terrain diffère du manifeste pour : "
            + ", ".join(mismatches)
        )


def _collect_metrics(
    path: Path,
    cases: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    frame, columns = _scan_csv(path, "metrics")
    metadata = {"case", "dataset", "fault", "root_cause_service", "time"}
    metric_columns = [column for column in columns if column not in metadata]

    root_present = pl.lit(False)
    for case in cases:
        case_id = str(case["case_id"])
        root_service = str(case["root_cause_service"])
        service_columns = [
            column
            for column in metric_columns
            if column.startswith(f"{root_service}_")
        ]
        row_present = (
            pl.any_horizontal(
                [pl.col(column).is_not_null() for column in service_columns]
            )
            if service_columns
            else pl.lit(False)
        )
        root_present = (
            pl.when(pl.col("case") == case_id)
            .then(row_present)
            .otherwise(root_present)
        )

    summary = (
        frame.with_columns(root_present.alias("__root_present"))
        .group_by("case")
        .agg(
            pl.len().alias("row_count"),
            pl.col("time").min().alias("timestamp_min"),
            pl.col("time").max().alias("timestamp_max"),
            pl.col("__root_present").any().alias("root_service_present"),
        )
        .collect(engine="streaming")
    )
    return {
        str(row["case"]): {
            "row_count": row["row_count"],
            "timestamp_min": row["timestamp_min"],
            "timestamp_max": row["timestamp_max"],
            "timestamp_unit": "seconds",
            "root_service_present": row["root_service_present"],
        }
        for row in summary.to_dicts()
    }


def _root_lookup(cases: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "case": [str(case["case_id"]) for case in cases],
            "__expected_root": [
                str(case["root_cause_service"]) for case in cases
            ],
        }
    ).lazy()


def _collect_service_modality(
    path: Path,
    source: str,
    timestamp_column: str,
    service_column: str,
    timestamp_unit: str,
    cases: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    frame, _ = _scan_csv(path, source)
    summary = (
        frame.select("case", timestamp_column, service_column)
        .join(_root_lookup(cases), on="case", how="left")
        .with_columns(
            (pl.col(service_column) == pl.col("__expected_root")).alias(
                "__root_present"
            )
        )
        .group_by("case")
        .agg(
            pl.len().alias("row_count"),
            pl.col(timestamp_column).min().alias("timestamp_min"),
            pl.col(timestamp_column).max().alias("timestamp_max"),
            pl.col("__root_present").any().alias("root_service_present"),
        )
        .collect(engine="streaming")
    )
    return {
        str(row["case"]): {
            "row_count": row["row_count"],
            "timestamp_min": row["timestamp_min"],
            "timestamp_max": row["timestamp_max"],
            "timestamp_unit": timestamp_unit,
            "root_service_present": row["root_service_present"],
        }
        for row in summary.to_dicts()
    }


def _empty_statistics(timestamp_unit: str) -> dict[str, Any]:
    return {
        "row_count": 0,
        "timestamp_min": None,
        "timestamp_max": None,
        "timestamp_unit": timestamp_unit,
        "root_service_present": False,
    }


def build_integrity_report(
    project_root: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path)
    cases = sorted(manifest["cases"], key=lambda case: str(case["case_id"]))
    paths = _resolve_source_paths(project_root, manifest)

    _validate_ground_truth(paths["ground_truth"], cases)

    metrics = _collect_metrics(paths["metrics"], cases)
    logs = _collect_service_modality(
        paths["logs"], "logs", "timestamp", "container_name", "seconds", cases
    )
    traces = _collect_service_modality(
        paths["traces"],
        "traces",
        "startTimeMillis",
        "serviceName",
        "milliseconds",
        cases,
    )

    results: list[dict[str, Any]] = []
    for case in cases:
        case_id = str(case["case_id"])
        statistics = {
            "metrics": metrics.get(case_id, _empty_statistics("seconds")),
            "logs": logs.get(case_id, _empty_statistics("seconds")),
            "traces": traces.get(case_id, _empty_statistics("milliseconds")),
        }
        results.append(validate_case_integrity(case, statistics))

    error_counts = Counter(
        error
        for result in results
        for error in result["errors"]
    )
    warning_counts = Counter(
        warning
        for result in results
        for warning in result.get(
            "warnings",
            [],
        )
    )
    valid_count = sum(bool(result["valid"]) for result in results)

    return {
        "schema_version": "1.0",
        "manifest": str(manifest_path.relative_to(project_root)).replace("\\", "/"),
        "summary": {
            "total_cases": len(results),
            "valid_cases": valid_count,
            "invalid_cases": len(results) - valid_count,
            "error_counts": dict(sorted(error_counts.items())),
            "warning_counts": dict(sorted(warning_counts.items())),
        },
        "cases": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Valide l'intégrité des 60 cas RCAEval.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("data/manifest/rcaeval_subset_manifest.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/reports/dataset_integrity_report.json"),
    )
    args = parser.parse_args()

    project_root = args.project_root.resolve()
    manifest_path = (
        args.manifest if args.manifest.is_absolute() else project_root / args.manifest
    ).resolve()
    output_path = (
        args.output if args.output.is_absolute() else project_root / args.output
    ).resolve()

    report = build_integrity_report(project_root, manifest_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    summary = report["summary"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Rapport : {output_path}")
    return 0 if summary["invalid_cases"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
