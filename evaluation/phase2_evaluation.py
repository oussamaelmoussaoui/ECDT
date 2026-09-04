#!/usr/bin/env python3
"""Evaluate Phase 2 anomaly detection without leaking RCAEval labels.

The detector receives normalized metric telemetry and, in offline benchmark
mode only, the RCAEval injection timestamp as the baseline boundary. Fault and
root-cause labels are introduced only after predictions have been produced.

By default the CLI runs a deterministic pilot of three cases per fault
(CPU, delay, loss and socket). Use ``--all-cases`` for the 60-case campaign.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.anomaly_detector import (  # noqa: E402
    FAULT_TO_INCIDENT,
    AnomalyDetector,
    DetectorConfig,
)
from src.ingestion.dataset_loader import create_default_loader  # noqa: E402
from src.ingestion.metric_quality import (  # noqa: E402
    has_finite_numeric_value,
    summarize_metric_value_rejections,
)
from src.ingestion.models import DetectionMethod  # noqa: E402
from src.ingestion.schema_normalizer import SchemaNormalizer  # noqa: E402


LOGGER = logging.getLogger("phase2_evaluation")
SUPPORTED_FAULTS = ("cpu", "delay", "loss", "socket")

# Evaluation-only profiles. They never enter the detector.
FAULT_SIGNAL_PROFILES: dict[str, dict[str, tuple[str, ...]]] = {
    "cpu": {
        "detector_signals": ("cpu",),
        "context_evidence": ("cpu_metrics",),
    },
    "delay": {
        "detector_signals": ("latency-50", "latency-90"),
        "context_evidence": (
            "latency_metrics",
            "span_duration",
            "response_time",
        ),
    },
    "loss": {
        "detector_signals": ("error", "socket"),
        "context_evidence": (
            "error_metrics",
            "request_counters",
            "failed_or_incomplete_traces",
        ),
    },
    "socket": {
        "detector_signals": ("socket", "error"),
        "context_evidence": (
            "network_metrics",
            "connection_logs",
            "network_traces",
        ),
    },
}


def _text(value: Any) -> str:
    return str(getattr(value, "value", value))


def _case_field(case: Any, field: str) -> Any:
    if isinstance(case, Mapping):
        return case[field]
    return getattr(case, field)


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _stable_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def select_pilot_case_ids(
    cases: Iterable[Any],
    *,
    cases_per_fault: int = 3,
) -> list[str]:
    """Select a deterministic, balanced pilot from case metadata."""

    if cases_per_fault <= 0:
        raise ValueError("cases_per_fault must be greater than zero")

    grouped: dict[str, list[str]] = {
        fault: [] for fault in SUPPORTED_FAULTS
    }
    for case in cases:
        fault = _text(_case_field(case, "fault")).strip().lower()
        if fault in grouped:
            grouped[fault].append(_text(_case_field(case, "case_id")))

    selected: list[str] = []
    for fault in SUPPORTED_FAULTS:
        candidates = sorted(set(grouped[fault]))
        if len(candidates) < cases_per_fault:
            raise ValueError(
                f"Fault {fault!r} has {len(candidates)} cases; "
                f"{cases_per_fault} required"
            )
        selected.extend(candidates[:cases_per_fault])
    return selected


def summarize_semantic_signal_coverage(
    events: Iterable[Mapping[str, Any]],
    *,
    fault: str,
) -> dict[str, Any]:
    """Describe relevant operational signals for one evaluation label."""

    normalized_fault = str(fault).strip().lower()
    if normalized_fault not in FAULT_SIGNAL_PROFILES:
        raise ValueError(f"Unsupported RCAEval fault: {fault!r}")

    profile = FAULT_SIGNAL_PROFILES[normalized_fault]
    detector_signals = set(profile["detector_signals"])
    by_signal: Counter[str] = Counter()
    by_relevant_signal: Counter[str] = Counter()
    relevant_series: set[tuple[str, str, str]] = set()

    for event in events:
        if event.get("source") != "metric":
            continue
        if not has_finite_numeric_value(event):
            continue
        signal = str(event.get("signal_type") or "unknown")
        by_signal[signal] += 1
        if signal in detector_signals:
            by_relevant_signal[signal] += 1
            relevant_series.add(
                (
                    str(event.get("service_name") or "unknown"),
                    signal,
                    str(event.get("metric_name") or "unknown"),
                )
            )

    relevant_count = sum(by_relevant_signal.values())
    return {
        "fault": normalized_fault,
        "detector_signal_types": sorted(detector_signals),
        "context_evidence_to_review": list(profile["context_evidence"]),
        "all_valid_metric_rows_by_signal": dict(sorted(by_signal.items())),
        "relevant_metric_rows_by_signal": dict(
            sorted(by_relevant_signal.items())
        ),
        "relevant_metric_rows": relevant_count,
        "relevant_metric_series": len(relevant_series),
        "semantic_input_available": relevant_count > 0,
        "scope_note": (
            "Current statistical detection consumes metrics only; logs and "
            "traces are contextual evidence and are not silently presented "
            "as detector inputs."
        ),
    }


def _operational_anomaly_record(anomaly: Any) -> dict[str, Any]:
    score = float(anomaly.score)
    return {
        "event_id": str(anomaly.event_id),
        "case_id": str(anomaly.case_id),
        "timestamp_ms": float(anomaly.timestamp),
        "service": anomaly.service,
        "signal_type": _text(anomaly.signal_type),
        "value": float(anomaly.value),
        "detection_method": _text(anomaly.detection_method),
        "signed_detection_score": score,
        "observer_score": abs(score),
        "incident_type": (
            _text(anomaly.incident_type)
            if anomaly.incident_type is not None
            else None
        ),
    }


def _summarize_predictions(anomalies: Sequence[Any]) -> dict[str, Any]:
    records = [_operational_anomaly_record(item) for item in anomalies]
    score_directions = Counter(
        "negative"
        if item["signed_detection_score"] < 0
        else "positive"
        if item["signed_detection_score"] > 0
        else "zero"
        for item in records
    )
    by_signal = Counter(item["signal_type"] for item in records)
    by_service = Counter(str(item["service"] or "unknown") for item in records)
    return {
        "anomaly_count": len(records),
        "anomaly_fingerprint_sha256": _stable_hash(records),
        "by_signal_type": dict(sorted(by_signal.items())),
        "by_service": dict(sorted(by_service.items())),
        "signed_score_directions": dict(sorted(score_directions.items())),
        "observer_score_contract_violations": sum(
            1
            for item in records
            if (
                not math.isfinite(item["observer_score"])
                or item["observer_score"] < 0
            )
        ),
        "sample": records[:25],
    }


def _evaluate_after_prediction(
    case_info: Any,
    anomalies: Sequence[Any],
    semantic_signal_coverage: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply RCAEval labels only after operational predictions exist."""

    fault = _text(case_info.fault).strip().lower()
    relevant_signals = set(
        FAULT_SIGNAL_PROFILES[fault]["detector_signals"]
    )
    relevant = [
        anomaly
        for anomaly in anomalies
        if _text(anomaly.signal_type) in relevant_signals
    ]
    faulty_window = [
        anomaly
        for anomaly in relevant
        if (
            float(case_info.inject_time_ms)
            <= float(anomaly.timestamp)
            <= float(case_info.time_end_ms)
        )
    ]
    before_injection = [
        anomaly
        for anomaly in relevant
        if (
            float(case_info.time_start_ms)
            <= float(anomaly.timestamp)
            < float(case_info.inject_time_ms)
        )
    ]
    first_timestamp = (
        min(float(item.timestamp) for item in faulty_window)
        if faulty_window
        else None
    )
    delay_ms = (
        first_timestamp - float(case_info.inject_time_ms)
        if first_timestamp is not None
        else None
    )
    expected_incident = FAULT_TO_INCIDENT.get(fault)
    detected_services = sorted(
        {
            str(item.service)
            for item in faulty_window
            if item.service is not None
        }
    )
    return {
        "ground_truth": {
            "fault": fault,
            "root_cause_service": str(case_info.root_cause_service),
            "time_start_ms": int(case_info.time_start_ms),
            "inject_time_ms": int(case_info.inject_time_ms),
            "time_end_ms": int(case_info.time_end_ms),
            "expected_incident_type": (
                expected_incident.value if expected_incident else None
            ),
        },
        "relevant_signal_types": sorted(relevant_signals),
        "semantic_signal_coverage": dict(semantic_signal_coverage),
        "detected": bool(faulty_window),
        "relevant_anomalies_total": len(relevant),
        "relevant_anomalies_before_injection": len(before_injection),
        "relevant_anomalies_in_fault_window": len(faulty_window),
        "first_detection_timestamp_ms": first_timestamp,
        "detection_delay_ms": delay_ms,
        "detection_delay_seconds": (
            delay_ms / 1000 if delay_ms is not None else None
        ),
        "detected_services": detected_services,
        "root_cause_service_detected": (
            str(case_info.root_cause_service) in detected_services
        ),
        "interpretation_note": (
            "These labels are evaluation-only and were not supplied to the "
            "detector. Because the offline injection boundary defines the "
            "baseline split, this run does not estimate pre-injection false "
            "positives."
        ),
    }


def evaluate_one_case(
    loader: Any,
    normalizer: Any,
    detector: Any,
    case_id: str,
    *,
    verify_reproducibility: bool = True,
) -> dict[str, Any]:
    """Run normalization, detection and post-hoc evaluation for one case."""

    case_info = loader.get_case_info(case_id)
    metrics = loader.load_metrics(
        case_id=case_id,
        time_start_ms=case_info.time_start_ms,
        time_end_ms=case_info.time_end_ms,
        long_format=True,
    )
    normalized = normalizer.normalize_metrics(metrics)
    records = normalized.to_dicts()
    quality = summarize_metric_value_rejections(records)
    # No fault, expected incident or root-cause label enters this call.
    anomalies = detector.detect_in_events(
        records,
        baseline_end=case_info.inject_time_ms,
    )
    operational_predictions = _summarize_predictions(anomalies)

    if verify_reproducibility:
        repeated = detector.detect_in_events(
            records,
            baseline_end=case_info.inject_time_ms,
        )
        repeated_summary = _summarize_predictions(repeated)
        reproducibility = {
            "checked": True,
            "identical": (
                operational_predictions["anomaly_count"]
                == repeated_summary["anomaly_count"]
                and operational_predictions["anomaly_fingerprint_sha256"]
                == repeated_summary["anomaly_fingerprint_sha256"]
            ),
            "first_count": operational_predictions["anomaly_count"],
            "second_count": repeated_summary["anomaly_count"],
        }
    else:
        reproducibility = {"checked": False, "identical": None}

    operational = {
        "case_id": str(case_info.case_id),
        "dataset": str(case_info.dataset),
        "mode": "offline_benchmark",
        "baseline_boundary": {
            "source": "rcaeval_injection_timestamp",
            "production_available": False,
        },
        "normalized_metric_rows": len(records),
        "metric_quality": quality,
        "predictions": operational_predictions,
        "reproducibility": reproducibility,
    }
    semantic = summarize_semantic_signal_coverage(
        records,
        fault=case_info.fault,
    )
    return {
        "case_id": str(case_info.case_id),
        "status": "success",
        "operational": operational,
        "evaluation": _evaluate_after_prediction(
            case_info,
            anomalies,
            semantic,
        ),
    }


def _aggregate(case_reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successes = [item for item in case_reports if item["status"] == "success"]
    failures = [item for item in case_reports if item["status"] != "success"]
    by_fault: dict[str, dict[str, Any]] = {}
    for item in successes:
        evaluation = item["evaluation"]
        fault = evaluation["ground_truth"]["fault"]
        stats = by_fault.setdefault(
            fault,
            {
                "total": 0,
                "detected": 0,
                "semantic_input_available": 0,
                "reproducible": 0,
            },
        )
        stats["total"] += 1
        stats["detected"] += int(evaluation["detected"])
        stats["semantic_input_available"] += int(
            evaluation["semantic_signal_coverage"]
            ["semantic_input_available"]
        )
        stats["reproducible"] += int(
            item["operational"]["reproducibility"]["identical"] is True
        )

    for stats in by_fault.values():
        stats["detection_rate"] = stats["detected"] / stats["total"]

    return {
        "selected_cases": len(case_reports),
        "successful_cases": len(successes),
        "failed_cases": len(failures),
        "all_metric_rejections_explained": (
            bool(successes)
            and all(
                item["operational"]["metric_quality"]
                ["all_rejections_explained"]
                for item in successes
            )
        ),
        "observer_score_contract_violations": sum(
            item["operational"]["predictions"]
            ["observer_score_contract_violations"]
            for item in successes
        ),
        "by_fault": dict(sorted(by_fault.items())),
    }


def run_evaluation(
    *,
    project_root: Path = PROJECT_ROOT,
    method: DetectionMethod = DetectionMethod.Z_SCORE,
    z_threshold: float = 3.0,
    threshold_quantile: float = 0.95,
    min_baseline_samples: int = 10,
    cases_per_fault: int = 3,
    case_ids: Sequence[str] | None = None,
    all_cases: bool = False,
    verify_reproducibility: bool = True,
    loader: Any | None = None,
    normalizer: Any | None = None,
    detector: Any | None = None,
) -> dict[str, Any]:
    """Run the deterministic pilot or full Phase 2 evaluation campaign."""

    root = Path(project_root).resolve()
    loader = loader or create_default_loader(root)
    normalizer = normalizer or SchemaNormalizer()
    detector = detector or AnomalyDetector(
        DetectorConfig(
            method=method,
            z_threshold=z_threshold,
            threshold_quantile=threshold_quantile,
            min_baseline_samples=min_baseline_samples,
        )
    )

    available_case_ids = loader.list_cases()
    if case_ids:
        selected_ids = sorted(set(case_ids))
        missing = sorted(set(selected_ids) - set(available_case_ids))
        if missing:
            raise ValueError(f"Unknown case IDs: {missing}")
        selection_mode = "explicit"
    elif all_cases:
        selected_ids = sorted(available_case_ids)
        selection_mode = "all_cases"
    else:
        metadata = [loader.get_case_info(item) for item in available_case_ids]
        selected_ids = select_pilot_case_ids(
            metadata,
            cases_per_fault=cases_per_fault,
        )
        selection_mode = "balanced_pilot"

    case_reports: list[dict[str, Any]] = []
    for index, case_id in enumerate(selected_ids, start=1):
        LOGGER.info("[%d/%d] %s", index, len(selected_ids), case_id)
        try:
            case_reports.append(
                evaluate_one_case(
                    loader,
                    normalizer,
                    detector,
                    case_id,
                    verify_reproducibility=verify_reproducibility,
                )
            )
        except Exception as exc:
            LOGGER.exception("Case failed: %s", case_id)
            case_reports.append(
                {
                    "case_id": case_id,
                    "status": "failed",
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    },
                }
            )

    report: dict[str, Any] = {
        "schema_version": "2.0",
        "phase": "phase2_anomaly_detection_evaluation",
        "scope": {
            "mode": "offline_benchmark",
            "injection_timestamp_used_for_baseline": True,
            "production_assumption": False,
            "ground_truth_boundary": (
                "fault and root-cause labels are applied only after prediction"
            ),
            "detector_algorithm_changed": False,
        },
        "configuration": {
            "method": method.value,
            "z_threshold": z_threshold,
            "threshold_quantile": threshold_quantile,
            "min_baseline_samples": min_baseline_samples,
            "reproducibility_check": verify_reproducibility,
        },
        "selection": {
            "mode": selection_mode,
            "cases_per_fault": (
                cases_per_fault if selection_mode == "balanced_pilot" else None
            ),
            "case_ids": selected_ids,
        },
        "aggregate": _aggregate(case_reports),
        "cases": case_reports,
    }
    report["report_fingerprint_sha256"] = _stable_hash(report)
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate Phase 2 detection. Default: deterministic 12-case pilot."
        )
    )
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("evaluation/results/phase2_detection_results.json"),
    )
    parser.add_argument(
        "--method",
        choices=[item.value for item in DetectionMethod],
        default=DetectionMethod.Z_SCORE.value,
    )
    parser.add_argument("--z-threshold", type=float, default=3.0)
    parser.add_argument("--threshold-quantile", type=float, default=0.95)
    parser.add_argument("--min-baseline-samples", type=int, default=10)
    parser.add_argument("--cases-per-fault", type=int, default=3)
    parser.add_argument("--case-id", action="append", dest="case_ids")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--skip-reproducibility-check", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    report = run_evaluation(
        project_root=args.project_root,
        method=DetectionMethod(args.method),
        z_threshold=args.z_threshold,
        threshold_quantile=args.threshold_quantile,
        min_baseline_samples=args.min_baseline_samples,
        cases_per_fault=args.cases_per_fault,
        case_ids=args.case_ids,
        all_cases=args.all_cases,
        verify_reproducibility=not args.skip_reproducibility_check,
    )
    output = args.output
    if not output.is_absolute():
        output = Path(args.project_root) / output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=_json_default)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["aggregate"], indent=2, ensure_ascii=False))
    print(f"Report: {output.resolve()}")
    return 0 if report["aggregate"]["failed_cases"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
