"""
Generate a deterministic manifest for the selected RCAEval cases.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import csv
from collections import Counter
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.ingestion.dataset_loader import (  # noqa: E402
    create_default_loader,
)


FILE_ATTRIBUTES = {
    "metrics": "metrics_path",
    "logs": "logs_path",
    "traces": "traces_path",
    "ground_truth": "ground_truth_path",
}


def _sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    """Calculate a file SHA-256 without loading it entirely."""

    digest = hashlib.sha256()

    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def _resolve_source_path(
    path: Path,
    project_root: Path,
) -> Path:
    """Resolve and validate one dataset source path."""

    resolved_path = Path(path)

    if not resolved_path.is_absolute():
        resolved_path = (
            project_root
            / resolved_path
        )

    resolved_path = resolved_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"Dataset source file not found: {resolved_path}"
        )

    return resolved_path


def _manifest_path(
    path: Path,
    project_root: Path,
) -> str:
    """Return a portable project-relative path when possible."""

    try:
        return path.relative_to(
            project_root.resolve()
        ).as_posix()

    except ValueError:
        return str(path)


def _text(value: Any) -> str | None:
    """Return a stable string representation."""

    if value is None:
        return None

    return str(
        getattr(value, "value", value)
    )

def _parse_inventory_bool(
    value: Any,
    *,
    field_name: str,
    case_id: str,
) -> bool:
    """Parse a deterministic boolean from the RCAEval inventory."""

    normalized = str(value).strip().lower()

    if normalized in {"true", "1", "yes"}:
        return True

    if normalized in {"false", "0", "no"}:
        return False

    raise ValueError(
        f"Invalid {field_name} value for {case_id!r}: "
        f"{value!r}"
    )


def _load_modality_availability(
    project_root: Path,
    case_ids: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Load source modality availability for selected cases."""

    inventory_path = (
        project_root
        / "data"
        / "rcaeval_cases_inventory.csv"
    )

    if not inventory_path.is_file():
        raise FileNotFoundError(
            f"RCAEval inventory not found: {inventory_path}"
        )

    required_columns = {
        "case",
        "n_metrics",
        "n_timesteps",
        "has_logs",
        "n_logs",
        "has_traces",
        "n_traces",
    }

    selected_case_ids = set(case_ids)
    availability: dict[
        str,
        dict[str, dict[str, Any]],
    ] = {}

    with inventory_path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as source:
        reader = csv.DictReader(source)

        available_columns = set(
            reader.fieldnames or []
        )

        missing_columns = (
            required_columns
            - available_columns
        )

        if missing_columns:
            raise ValueError(
                "Invalid RCAEval inventory. "
                "Missing columns: "
                f"{sorted(missing_columns)}"
            )

        for row in reader:
            case_id = str(row["case"])

            if case_id not in selected_case_ids:
                continue

            if case_id in availability:
                raise ValueError(
                    "Duplicate case in RCAEval inventory: "
                    f"{case_id!r}"
                )

            metric_count = int(row["n_metrics"])
            timestep_count = int(
                row["n_timesteps"]
            )
            log_count = int(row["n_logs"])
            trace_count = int(row["n_traces"])

            has_logs = _parse_inventory_bool(
                row["has_logs"],
                field_name="has_logs",
                case_id=case_id,
            )

            has_traces = _parse_inventory_bool(
                row["has_traces"],
                field_name="has_traces",
                case_id=case_id,
            )

            if has_logs != (log_count > 0):
                raise ValueError(
                    "Inconsistent log availability for "
                    f"{case_id!r}"
                )

            if has_traces != (trace_count > 0):
                raise ValueError(
                    "Inconsistent trace availability for "
                    f"{case_id!r}"
                )

            availability[case_id] = {
                "metrics": {
                    "available_in_source": (
                        metric_count > 0
                        and timestep_count > 0
                    ),
                    "source_metric_count": (
                        metric_count
                    ),
                    "source_row_count": (
                        timestep_count
                    ),
                },
                "logs": {
                    "available_in_source": (
                        has_logs
                    ),
                    "source_row_count": (
                        log_count
                    ),
                },
                "traces": {
                    "available_in_source": (
                        has_traces
                    ),
                    "source_row_count": (
                        trace_count
                    ),
                },
            }

    missing_cases = (
        selected_case_ids
        - set(availability)
    )

    if missing_cases:
        raise ValueError(
            "Selected cases missing from RCAEval inventory: "
            f"{sorted(missing_cases)}"
        )

    return availability

def build_manifest(
    loader: Any,
    *,
    project_root: Path,
    source_repository_url: str | None = None,
    source_commit: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic manifest without writing it."""

    project_root = Path(
        project_root
    ).resolve()

    case_ids = sorted(
        set(loader.list_cases())
    )

    case_information = [
        loader.get_case_info(case_id)
        for case_id in case_ids
    ]
    modality_availability = (
        _load_modality_availability(
            project_root,
            case_ids,
        )
    )

    by_fault = Counter(
        _text(info.fault)
        for info in case_information
    )

    by_dataset = Counter(
        _text(info.dataset)
        for info in case_information
    )

    files: dict[str, dict[str, Any]] = {}

    for source_name, attribute_name in (
        FILE_ATTRIBUTES.items()
    ):
        source_path = _resolve_source_path(
            getattr(
                loader.paths,
                attribute_name,
            ),
            project_root,
        )

        files[source_name] = {
            "path": _manifest_path(
                source_path,
                project_root,
            ),
            "size_bytes": (
                source_path.stat().st_size
            ),
            "sha256": _sha256_file(
                source_path
            ),
        }

    if (
        source_repository_url is not None
        and source_commit is not None
    ):
        provenance_status = "recorded"

    elif (
        source_repository_url is not None
        or source_commit is not None
    ):
        provenance_status = "partial"

    else:
        provenance_status = (
            "unavailable_in_local_copy"
        )

    cases = []

    for info in sorted(
        case_information,
        key=lambda item: str(
            item.case_id
        ),
    ):
        case_id = str(info.case_id)

        modalities = (
            modality_availability[case_id]
        )

        associated_files = [
            source_name
            for source_name in (
                "metrics",
                "logs",
                "traces",
            )
            if modalities[source_name][
                "available_in_source"
            ]
        ]

        associated_files.append(
            "ground_truth"
        )
        cases.append(
            {
                "case_id": case_id,
                "dataset": _text(
                    info.dataset
                ),
                "fault": _text(
                    info.fault
                ),
                "root_cause_service": _text(
                    info.root_cause_service
                ),
                "time_start_ms": int(
                    info.time_start_ms
                ),
                "inject_time_ms": int(
                    info.inject_time_ms
                ),
                "time_end_ms": int(
                    info.time_end_ms
                ),
                "incident_type": _text(
                    info.incident_type
                ),
                "modalities": modalities,
                "associated_files": associated_files,
            }
        )

    return {
        "schema_version": "1.1",
        "source_provenance": {
            "dataset": "RCAEval",
            "repository_url": (
                source_repository_url
            ),
            "commit": source_commit,
            "status": provenance_status,
        },
        "selection": {
            "total_cases": len(
                case_information
            ),
            "by_fault": dict(
                sorted(by_fault.items())
            ),
            "by_dataset": dict(
                sorted(by_dataset.items())
            ),
        },
        "files": files,
        "cases": cases,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the deterministic RCAEval "
            "subset manifest."
        )
    )

    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/manifest/"
            "rcaeval_subset_manifest.json"
        ),
    )

    parser.add_argument(
        "--source-repository-url",
        default=None,
    )

    parser.add_argument(
        "--source-commit",
        default=None,
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _build_parser().parse_args(
        argv
    )

    project_root = (
        args.project_root
        .expanduser()
        .resolve()
    )

    loader = create_default_loader(
        project_root
    )

    manifest = build_manifest(
        loader,
        project_root=project_root,
        source_repository_url=(
            args.source_repository_url
        ),
        source_commit=(
            args.source_commit
        ),
    )

    output_path = args.output

    if not output_path.is_absolute():
        output_path = (
            project_root
            / output_path
        )

    output_path = (
        output_path
        .expanduser()
        .resolve()
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    rendered = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )

    output_path.write_text(
        rendered + "\n",
        encoding="utf-8",
    )

    print(
        f"Manifest written: {output_path}"
    )
    print(
        "Cases: "
        f"{manifest['selection']['total_cases']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())