"""
ECDT - Phase 2
Run anomaly detection evaluation against ground truth.

This script executes the full detection pipeline on all 60 ECDT cases
and produces a comprehensive evaluation report with recall, detection
rate, false positives, and detection delay metrics.

Usage:
    python -m evaluation.run_phase2_detection
    python -m evaluation.run_phase2_detection --method z_score --z-threshold 3.0
    python -m evaluation.run_phase2_detection --method threshold --quantile 0.95
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from ingestion.dataset_loader import create_loader
from ingestion.schema_normalizer import SchemaNormalizer
from ingestion.anomaly_detector import (
    AnomalyDetector,
    DetectorConfig,
    aggregate_metrics,
    evaluate_case,
    format_detection_report,
)
from ingestion.models import DetectionMethod, IncidentType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_evaluation(
    method: DetectionMethod = DetectionMethod.Z_SCORE,
    z_threshold: float = 3.0,
    threshold_quantile: float = 0.95,
    min_baseline_samples: int = 10,
    output_dir: Path = None,
) -> None:
    """
    Run the full Phase 2 detection evaluation.

    Parameters
    ----------
    method : DetectionMethod
        Detection method to use.
    z_threshold : float
        Z-score threshold (for z_score method).
    threshold_quantile : float
        Quantile threshold (for threshold method).
    min_baseline_samples : int
        Minimum baseline samples.
    output_dir : Path, optional
        Directory to save results.
    """
    logger.info("Starting Phase 2 detection evaluation")
    logger.info(f"  Method: {method.value}")
    logger.info(f"  Z-threshold: {z_threshold}")
    logger.info(f"  Threshold quantile: {threshold_quantile}")

    # Initialize components
    loader = create_loader(project_root)
    normalizer = SchemaNormalizer(loader)
    config = DetectorConfig(
        method=method,
        z_threshold=z_threshold,
        threshold_quantile=threshold_quantile,
        min_baseline_samples=min_baseline_samples,
    )
    detector = AnomalyDetector(config)

    # Get all cases
    cases = loader.get_available_cases()
    logger.info(f"Total cases to evaluate: {len(cases)}")

    # Evaluate each case
    all_case_metrics = []

    for i, case_id in enumerate(cases, 1):
        logger.info(f"[{i}/{len(cases)}] Processing case: {case_id}")

        try:
            # Get case info from ground truth
            case_info = loader.get_case_info(case_id)
            inject_time = float(case_info["inject_time"])
            time_start = float(case_info["time_start"])
            time_end = float(case_info["time_end"])
            fault = case_info["fault"]
            root_cause_service = case_info.get("root_cause_service")

            # Map fault to incident type
            from ingestion.schema_normalizer import map_fault_to_incident
            incident_type = map_fault_to_incident(fault)

            # Normalize events for this case
            logger.info(f"  Normalizing events...")
            events = list(normalizer.iter_case_events(case_id))
            logger.info(f"  Total normalized events: {len(events)}")

            # Detect anomalies
            logger.info(f"  Running anomaly detection...")
            anomalies = detector.detect_in_events(
                events,
                baseline_end=inject_time,
                incident_type=incident_type,
            )
            logger.info(f"  Detected anomalies: {len(anomalies)}")

            # Evaluate
            metrics = evaluate_case(
                case_id=case_id,
                fault=fault,
                incident_type=incident_type,
                anomalies=anomalies,
                inject_time=inject_time,
                time_start=time_start,
                time_end=time_end,
                root_cause_service=root_cause_service,
            )
            all_case_metrics.append(metrics)

            # Log summary for this case
            status = "DETECTED" if metrics.detected else "MISSED"
            delay_str = f"{metrics.detection_delay_seconds:.1f}s" if metrics.detection_delay_seconds else "N/A"
            logger.info(
                f"  Result: {status} | "
                f"Delay: {delay_str} | "
                f"TP: {metrics.true_positives} | "
                f"FP: {metrics.false_positives} | "
                f"RC detected: {metrics.root_cause_detected}"
            )

        except Exception as e:
            logger.error(f"  Error processing case {case_id}: {e}")
            continue

    # Aggregate metrics
    logger.info("Aggregating metrics...")
    agg = aggregate_metrics(all_case_metrics)

    # Format report
    report = format_detection_report(all_case_metrics, agg)
    print(report)

    # Save results
    if output_dir is None:
        output_dir = project_root / "evaluation" / "results"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save text report
    report_path = output_dir / "phase2_detection_report.txt"
    with open(report_path, "w") as f:
        f.write(report)
    logger.info(f"Report saved to: {report_path}")

    # Save JSON results
    json_results = {
        "config": {
            "method": method.value,
            "z_threshold": z_threshold,
            "threshold_quantile": threshold_quantile,
            "min_baseline_samples": min_baseline_samples,
        },
        "aggregate": {
            "total_cases": agg.total_cases,
            "detected_cases": agg.detected_cases,
            "detection_rate": agg.detection_rate,
            "avg_recall": agg.avg_recall,
            "avg_precision": agg.avg_precision,
            "avg_f1_score": agg.avg_f1_score,
            "total_true_positives": agg.total_true_positives,
            "total_false_positives": agg.total_false_positives,
            "global_precision": agg.global_precision,
            "global_recall": agg.global_recall,
            "global_f1_score": agg.global_f1_score,
            "avg_detection_delay_seconds": agg.avg_detection_delay_seconds,
            "median_detection_delay_seconds": agg.median_detection_delay_seconds,
            "max_detection_delay_seconds": agg.max_detection_delay_seconds,
            "root_cause_detection_rate": agg.root_cause_detection_rate,
            "per_incident_type": agg.per_incident_type,
            "per_fault": agg.per_fault,
        },
        "cases": [
            {
                "case_id": m.case_id,
                "fault": m.fault,
                "incident_type": m.incident_type.value if m.incident_type else None,
                "detected": m.detected,
                "detection_delay_seconds": m.detection_delay_seconds,
                "true_positives": m.true_positives,
                "false_positives": m.false_positives,
                "recall": m.recall,
                "precision": m.precision,
                "f1_score": m.f1_score,
                "root_cause_service": m.root_cause_service,
                "root_cause_detected": m.root_cause_detected,
                "detected_services": list(m.detected_services),
            }
            for m in all_case_metrics
        ],
    }

    json_path = output_dir / "phase2_detection_results.json"
    with open(json_path, "w") as f:
        json.dump(json_results, f, indent=2, default=str)
    logger.info(f"JSON results saved to: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="Run Phase 2 anomaly detection evaluation")
    parser.add_argument(
        "--method",
        type=str,
        default="z_score",
        choices=["z_score", "threshold"],
        help="Detection method (default: z_score)",
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=3.0,
        help="Z-score threshold (default: 3.0)",
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.95,
        help="Threshold quantile (default: 0.95)",
    )
    parser.add_argument(
        "--min-baseline",
        type=int,
        default=10,
        help="Minimum baseline samples (default: 10)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for results",
    )

    args = parser.parse_args()

    method = DetectionMethod.Z_SCORE if args.method == "z_score" else DetectionMethod.THRESHOLD

    run_evaluation(
        method=method,
        z_threshold=args.z_threshold,
        threshold_quantile=args.quantile,
        min_baseline_samples=args.min_baseline,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()