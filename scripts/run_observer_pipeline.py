#!/usr/bin/env python3
"""Execute the existing ECDT pipeline from RCAEval to the Observer Agent.

This module is intentionally limited to orchestration. It does not redefine the
project's normalization, anomaly detection, temporal context, incident model,
Neo4j persistence, or Cognitive Digital Twin logic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sys
from collections import defaultdict
from dataclasses import asdict, is_dataclass, replace
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Sequence
from collections import Counter



LOGGER = logging.getLogger("run_observer_pipeline")


def _discover_project_root(explicit_root: Path | None) -> Path:
    """Find an ECDT checkout containing the project's ``src`` package."""
    candidates = []
    if explicit_root is not None:
        candidates.append(explicit_root.expanduser().resolve())

    script_dir = Path(__file__).resolve().parent
    candidates.extend((Path.cwd().resolve(), script_dir, script_dir.parent))

    for candidate in candidates:
        if (candidate / "src" / "agents" / "observer").is_dir():
            return candidate

    searched = ", ".join(str(path) for path in candidates)
    raise RuntimeError(
        "Impossible de localiser la racine du projet ECDT. "
        f"Répertoires vérifiés : {searched}. Utilisez --project-root."
    )


def _configure_imports(project_root: Path) -> None:
    root = str(project_root)
    if root not in sys.path:
        sys.path.insert(0, root)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("la valeur doit être strictement positive")
    return parsed


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, Path)):
        return str(value)
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Type non sérialisable : {type(value).__name__}")


def _metric_value_rejection_reason(
    event: dict[str, Any],
) -> str | None:
    """Return the rejection reason, or None for a valid value."""

    value = event.get("value")

    if value is None:
        return "missing_value"

    try:
        numeric_value = float(value)

    except (TypeError, ValueError):
        return "non_numeric"

    if math.isnan(numeric_value):
        return "nan"

    if math.isinf(numeric_value):
        return "infinite"

    return None


def _has_finite_numeric_value(
    event: dict[str, Any],
) -> bool:
    """Return True when an event value can safely be stored."""

    return (
        _metric_value_rejection_reason(event)
        is None
    )


def _summarize_metric_value_rejections(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Explain rejected values and classify metric-series health."""

    metric_rows_total = 0
    valid_metric_rows = 0

    by_reason: Counter[str] = Counter()
    by_service_name: Counter[str] = Counter()
    by_signal_type: Counter[str] = Counter()
    by_metric_name: Counter[str] = Counter()

    detail_counts: Counter[
        tuple[str, str, str, str]
    ] = Counter()

    series_counts: dict[
        tuple[str, str, str],
        dict[str, Any],
    ] = {}

    for event in events:
        if event.get("source") != "metric":
            continue

        metric_rows_total += 1

        service_name = str(
            event.get("service_name")
            or "unknown"
        )
        signal_type = str(
            event.get("signal_type")
            or "unknown"
        )
        metric_name = str(
            event.get("metric_name")
            or "unknown"
        )

        series_key = (
            service_name,
            signal_type,
            metric_name,
        )

        if series_key not in series_counts:
            series_counts[series_key] = {
                "total_rows": 0,
                "valid_rows": 0,
                "rejected_rows": 0,
                "reasons": Counter(),
            }

        series_state = series_counts[
            series_key
        ]

        series_state["total_rows"] += 1

        reason = _metric_value_rejection_reason(
            event
        )

        if reason is None:
            valid_metric_rows += 1
            series_state["valid_rows"] += 1
            continue

        series_state["rejected_rows"] += 1
        series_state["reasons"][reason] += 1

        by_reason[reason] += 1
        by_service_name[service_name] += 1
        by_signal_type[signal_type] += 1
        by_metric_name[metric_name] += 1

        detail_counts[
            (
                service_name,
                signal_type,
                metric_name,
                reason,
            )
        ] += 1

    rejected_metric_rows = (
        metric_rows_total
        - valid_metric_rows
    )

    details = [
        {
            "service_name": service_name,
            "signal_type": signal_type,
            "metric_name": metric_name,
            "reason": reason,
            "count": count,
        }
        for (
            service_name,
            signal_type,
            metric_name,
            reason,
        ), count in sorted(
            detail_counts.items()
        )
    ]

    series_status_counts: Counter[str] = Counter()
    series_details: list[dict[str, Any]] = []

    for (
        service_name,
        signal_type,
        metric_name,
    ), state in sorted(
        series_counts.items()
    ):
        total_rows = int(
            state["total_rows"]
        )
        valid_rows = int(
            state["valid_rows"]
        )
        rejected_rows = int(
            state["rejected_rows"]
        )

        if valid_rows == 0:
            status = "fully_missing"
        elif rejected_rows == 0:
            status = "fully_valid"
        else:
            status = "partially_observed"

        series_status_counts[status] += 1

        series_details.append(
            {
                "service_name": service_name,
                "signal_type": signal_type,
                "metric_name": metric_name,
                "status": status,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "rejected_rows": rejected_rows,
                "rejection_ratio": (
                    rejected_rows / total_rows
                    if total_rows
                    else None
                ),
                "reasons": dict(
                    sorted(
                        state["reasons"].items()
                    )
                ),
            }
        )

    return {
        "metric_rows_total": metric_rows_total,
        "valid_metric_rows": valid_metric_rows,
        "rejected_metric_rows": rejected_metric_rows,
        "by_reason": dict(
            sorted(by_reason.items())
        ),
        "by_service_name": dict(
            sorted(by_service_name.items())
        ),
        "by_signal_type": dict(
            sorted(by_signal_type.items())
        ),
        "by_metric_name": dict(
            sorted(by_metric_name.items())
        ),
        "details": details,
        "series_summary": {
            "total_series": len(
                series_details
            ),
            "fully_valid_series": (
                series_status_counts[
                    "fully_valid"
                ]
            ),
            "partially_observed_series": (
                series_status_counts[
                    "partially_observed"
                ]
            ),
            "fully_missing_series": (
                series_status_counts[
                    "fully_missing"
                ]
            ),
            "details": series_details,
        },
    }

def _adapt_anomaly_for_observer(anomaly: Any) -> Any:
    """Adapt the Phase 2 signed z-score to the Observer magnitude contract."""
    score = float(anomaly.score)
    if score >= 0:
        return anomaly

    method = getattr(anomaly.detection_method, "value", anomaly.detection_method)
    if str(method) != "z_score":
        raise ValueError(
            "Un score négatif ne peut être adapté que pour la méthode z_score"
        )

    metadata = dict(anomaly.metadata)
    metadata["signed_detection_score"] = score
    metadata["observer_score_adaptation"] = "absolute_z_score"

    return replace(
        anomaly,
        score=abs(score),
        metadata=metadata,
    )


def _enum_text(value: Any) -> str:
    """Return a stable textual representation for grouping and reports."""
    return str(getattr(value, "value", value))


def _timestamp_to_seconds(timestamp: int | float) -> float:
    """Normalize Unix seconds or milliseconds for duration calculations."""
    numeric_timestamp = float(timestamp)
    if abs(numeric_timestamp) >= 10_000_000_000:
        numeric_timestamp /= 1000.0
    return numeric_timestamp


def _group_anomalies_into_episodes(
    anomalies: Sequence[Any],
    gap_seconds: int,
    context_window_seconds: int,
) -> list[dict[str, Any]]:
    """Group and describe consecutive anomalies without changing Observer."""
    if gap_seconds <= 0:
        raise ValueError("gap_seconds must be strictly positive")
    if context_window_seconds <= 0:
        raise ValueError("context_window_seconds must be strictly positive")

    grouped: dict[tuple[str, str, str, str], list[Any]] = defaultdict(list)
    for anomaly in anomalies:
        key = (
            str(anomaly.case_id),
            str(anomaly.service),
            _enum_text(anomaly.signal_type),
            _enum_text(anomaly.incident_type),
        )
        grouped[key].append(anomaly)

    episodes: list[dict[str, Any]] = []

    def append_episode(
        key: tuple[str, str, str, str],
        members: list[Any],
    ) -> None:
        ordered = sorted(
            members,
            key=lambda anomaly: _timestamp_to_seconds(anomaly.timestamp),
        )
        start_seconds = _timestamp_to_seconds(ordered[0].timestamp)
        end_seconds = _timestamp_to_seconds(ordered[-1].timestamp)
        duration_seconds = end_seconds - start_seconds
        inter_anomaly_gaps = [
            _timestamp_to_seconds(current.timestamp)
            - _timestamp_to_seconds(previous.timestamp)
            for previous, current in zip(ordered, ordered[1:])
        ]
        representative = max(
            enumerate(ordered),
            key=lambda item: (abs(float(item[1].score)), -item[0]),
        )[1]
        representative_seconds = _timestamp_to_seconds(
            representative.timestamp
        )
        context_start_seconds = (
            representative_seconds - context_window_seconds
        )
        context_end_seconds = (
            representative_seconds + context_window_seconds
        )
        missing_before_seconds = max(
            0.0,
            context_start_seconds - start_seconds,
        )
        missing_after_seconds = max(
            0.0,
            end_seconds - context_end_seconds,
        )
        if duration_seconds > 0:
            covered_seconds = max(
                0.0,
                min(end_seconds, context_end_seconds)
                - max(start_seconds, context_start_seconds),
            )
            context_coverage_ratio = min(
                1.0,
                covered_seconds / duration_seconds,
            )
            anomaly_rate_per_minute: float | None = (
                (len(ordered) - 1) * 60.0 / duration_seconds
            )
        else:
            covered_seconds = 0.0
            context_coverage_ratio = 1.0
            anomaly_rate_per_minute = None

        raw_episode_id = (
            f"{key[0]}:{key[1]}:{key[2]}:{key[3]}:"
            f"{start_seconds:.6f}:{end_seconds:.6f}"
        )
        episode_id = "ep_" + hashlib.sha256(
            raw_episode_id.encode("utf-8")
        ).hexdigest()[:16]

        episode_summary = {
            "episode_id": episode_id,
            "case_id": key[0],
            "service": key[1],
            "signal_type": key[2],
            "incident_type": key[3],
            "start_timestamp": ordered[0].timestamp,
            "end_timestamp": ordered[-1].timestamp,
            "duration_seconds": duration_seconds,
            "anomaly_count": len(ordered),
            "anomaly_rate_per_minute": anomaly_rate_per_minute,
            "mean_inter_anomaly_gap_seconds": (
                sum(inter_anomaly_gaps) / len(inter_anomaly_gaps)
                if inter_anomaly_gaps
                else None
            ),
            "max_inter_anomaly_gap_seconds": (
                max(inter_anomaly_gaps)
                if inter_anomaly_gaps
                else None
            ),
            "representative_event_id": representative.event_id,
            "representative_timestamp": representative.timestamp,
            "representative_signed_score": float(representative.score),
            "representative_absolute_score": abs(float(representative.score)),
            "configured_context_window_seconds": (
                context_window_seconds * 2
            ),
            "episode_seconds_covered_by_context_window": covered_seconds,
            "context_window_coverage_ratio": context_coverage_ratio,
            "context_window_fully_covers_episode": (
                missing_before_seconds == 0.0
                and missing_after_seconds == 0.0
            ),
            "episode_seconds_outside_context_before": (
                missing_before_seconds
            ),
            "episode_seconds_outside_context_after": (
                missing_after_seconds
            ),
        }

        metadata = dict(representative.metadata)
        metadata["anomaly_episode"] = dict(episode_summary)
        representative_with_episode = replace(
            representative,
            metadata=metadata,
        )

        episodes.append(
            {
                "representative": representative_with_episode,
                "summary": episode_summary,
            }
        )

    for key, bucket in grouped.items():
        ordered_bucket = sorted(
            bucket,
            key=lambda anomaly: _timestamp_to_seconds(anomaly.timestamp),
        )
        current_episode = [ordered_bucket[0]]

        for anomaly in ordered_bucket[1:]:
            previous = current_episode[-1]
            elapsed = (
                _timestamp_to_seconds(anomaly.timestamp)
                - _timestamp_to_seconds(previous.timestamp)
            )
            if elapsed > gap_seconds:
                append_episode(key, current_episode)
                current_episode = [anomaly]
            else:
                current_episode.append(anomaly)

        append_episode(key, current_episode)

    episodes.sort(
        key=lambda episode: (
            _timestamp_to_seconds(episode["summary"]["start_timestamp"]),
            episode["summary"]["service"],
            episode["summary"]["signal_type"],
        )
    )

    for chronological_rank, episode in enumerate(episodes, start=1):
        episode["summary"]["chronological_rank"] = chronological_rank

    episodes_by_salience = sorted(
        episodes,
        key=lambda episode: (
            -episode["summary"]["representative_absolute_score"],
            -episode["summary"]["anomaly_count"],
            -episode["summary"]["duration_seconds"],
            _timestamp_to_seconds(episode["summary"]["start_timestamp"]),
            episode["summary"]["service"],
        ),
    )
    for salience_rank, episode in enumerate(episodes_by_salience, start=1):
        episode["summary"]["salience_rank"] = salience_rank
        episode["summary"]["salience_ranking_basis"] = [
            "representative_absolute_score_desc",
            "anomaly_count_desc",
            "duration_seconds_desc",
            "start_timestamp_asc",
        ]

    # Les rangs et mesures calculés après le regroupement doivent également
    # voyager avec l'anomalie représentative transmise à l'Observer.
    for episode in episodes:
        representative = episode["representative"]
        metadata = dict(representative.metadata)
        metadata["anomaly_episode"] = dict(episode["summary"])
        episode["representative"] = replace(
            representative,
            metadata=metadata,
        )

    return episodes


def _summarize_temporal_context(temporal_context: Any) -> dict[str, Any]:
    """Build a compact report without duplicating raw TimescaleDB rows."""
    if isinstance(temporal_context, dict):
        get_value = temporal_context.get
    else:
        get_value = lambda name, default=None: getattr(  # noqa: E731
            temporal_context,
            name,
            default,
        )

    observations = get_value("observations", []) or []
    statistics = dict(get_value("statistics", {}) or {})

    return {
        "resource_id": get_value("resource_id"),
        "metric_name": get_value("metric_name"),
        "signal_type": get_value("signal_type"),
        "anomaly_timestamp": get_value(
            "anomaly_timestamp"
        ),
        "window_before_seconds": get_value(
            "window_before_seconds"
        ),
        "window_after_seconds": get_value(
            "window_after_seconds"
        ),
        "requested_start_timestamp": get_value(
            "requested_start_timestamp"
        ),
        "requested_end_timestamp": get_value(
            "requested_end_timestamp"
        ),
        "requested_duration_seconds": get_value(
            "requested_duration_seconds"
        ),
        "first_observation_timestamp": get_value(
            "first_observation_timestamp"
        ),
        "last_observation_timestamp": get_value(
            "last_observation_timestamp"
        ),
        "actual_coverage_duration_seconds": get_value(
            "actual_coverage_duration_seconds"
        ),
        "estimated_sampling_interval_seconds": get_value(
            "estimated_sampling_interval_seconds"
        ),
        "expected_observation_count": get_value(
            "expected_observation_count"
        ),
        "observed_unique_timestamp_count": get_value(
            "observed_unique_timestamp_count",
            0,
        ),
        "missing_observation_count": get_value(
            "missing_observation_count"
        ),
        "temporal_data_completeness_ratio": get_value(
            "temporal_data_completeness_ratio"
        ),
        "rows_retrieved": len(observations),
        "numeric_observation_count": statistics.get(
            "observation_count",
            0,
        ),
        "statistics": statistics,
    }


def _write_report(report: dict[str, Any], output_path: Path | None) -> None:
    rendered = json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
        default=_json_default,
    )
    print(rendered)

    if output_path is not None:
        output_path = output_path.expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        LOGGER.info("Rapport écrit dans %s", output_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exécute le pipeline ECDT existant : RCAEval -> normalisation -> "
            "TimescaleDB -> détection -> Observer -> Neo4j."
        )
    )
    parser.add_argument("--case-id", required=True, help="Identifiant du cas RCAEval")
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Racine du dépôt ECDT (détection automatique par défaut)",
    )
    parser.add_argument(
        "--method",
        choices=("z_score", "threshold"),
        default="z_score",
        help="Méthode de détection déjà implémentée (défaut : z_score)",
    )
    parser.add_argument("--z-threshold", type=float, default=3.0)
    parser.add_argument("--threshold-quantile", type=float, default=0.95)
    parser.add_argument("--min-baseline-samples", type=_positive_int, default=10)
    parser.add_argument("--window-minutes", type=_positive_int, default=5)
    parser.add_argument("--minimum-confidence", type=float, default=0.0)
    parser.add_argument(
        "--skip-timescale-ingestion",
        action="store_true",
        help="N'insère pas les métriques si ce cas est déjà présent dans TimescaleDB",
    )
    parser.add_argument(
        "--max-episodes",
        "--max-anomalies",
        dest="max_episodes",
        type=_positive_int,
        help=(
            "Limite d'épisodes traités; --max-anomalies reste un alias "
            "compatible"
        ),
    )
    parser.add_argument(
        "--episode-gap-seconds",
        type=_positive_int,
        default=60,
        help=(
            "Nouvel épisode après ce délai sans anomalie pour le même "
            "service/signal (défaut : 60)"
        ),
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue le traitement si la persistance d'une anomalie échoue",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Chemin optionnel du rapport JSON (le rapport est toujours affiché)",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    project_root = _discover_project_root(args.project_root)
    _configure_imports(project_root)

    # Imports différés : le script peut ainsi localiser explicitement le dépôt.
    from src.agents.observer.incident_persistence import IncidentPersistence
    from src.agents.observer.observer_agent import ObserverAgent
    from src.agents.observer.timescale_consumer import TimescaleConsumer
    from src.digital_twin.timescale_client import TimescaleClient
    from src.digital_twin.timeseries_ingestion import ingest_normalized_events
    from src.digital_twin.timeseries_schema import METRIC_TABLE, initialize_schema
    from src.ingestion.anomaly_detector import FAULT_TO_INCIDENT, create_detector
    from src.ingestion.dataset_loader import create_default_loader
    from src.ingestion.models import DetectionMethod
    from src.ingestion.schema_normalizer import SchemaNormalizer
    from src.knowledge_graph.neo4j_client import Neo4jClient

    LOGGER.info("Chargement du cas RCAEval %s", args.case_id)
    loader = create_default_loader(project_root)
    case_data = loader.load_case(
        args.case_id,
        include_metrics=True,
        include_logs=True,
        include_traces=True,
        metrics_long_format=True,
    )
    case_info = case_data["case_info"]

    LOGGER.info("Normalisation des métriques, logs et traces")
    normalizer = SchemaNormalizer()
    normalized = normalizer.normalize_all(
        metrics=case_data["metrics"],
        logs=case_data["logs"],
        traces=case_data["traces"],
    )
    if normalized.is_empty():
        raise RuntimeError(f"Aucun événement normalisé pour le cas {args.case_id}")

    normalized_records = normalized.to_dicts()

    timescale = TimescaleClient()
    neo4j: Neo4jClient | None = None

    try:
        if not timescale.ping():
            raise RuntimeError("TimescaleDB est inaccessible (vérifiez TIMESCALE_URI)")

        # Préparer le contrat d'ingestion indépendamment de la décision
        # d'insérer. Cela rend aussi le rapport exact avec --skip-timescale-ingestion.
        metric_value_rejection_summary = (
            _summarize_metric_value_rejections(
                normalized_records
            )
        )

        metric_rows_count = (
            metric_value_rejection_summary[
                "metric_rows_total"
            ]
        )

        valid_metric_rows_count = (
            metric_value_rejection_summary[
                "valid_metric_rows"
            ]
        )

        skipped_metric_values = (
            metric_value_rejection_summary[
                "rejected_metric_rows"
            ]
        )

        # La table doit exister avant le contrôle d'idempotence. Cette fonction
        # réutilise strictement le schéma existant et ses opérations IF NOT EXISTS.
        initialize_schema(timescale)

        state_rows = timescale.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT (resource_id, timestamp, metric_name))
                    AS unique_rows
            FROM {METRIC_TABLE}
            WHERE case_id = %s;
            """,
            (case_info.case_id,),
            fetch=True,
        )
        state = state_rows[0] if state_rows else {}
        existing_metric_rows = int(state.get("total_rows") or 0)
        existing_unique_metric_rows = int(state.get("unique_rows") or 0)

        if existing_metric_rows != existing_unique_metric_rows:
            raise RuntimeError(
                "TimescaleDB contient des doublons pour le cas "
                f"{case_info.case_id!r} : {existing_metric_rows} lignes pour "
                f"{existing_unique_metric_rows} clés temporelles uniques."
            )

        inserted_metrics = 0
        ingestion_status: str

        if existing_metric_rows == 0:
            if args.skip_timescale_ingestion:
                raise RuntimeError(
                    "--skip-timescale-ingestion a été demandé, mais aucune "
                    f"métrique n'existe pour le cas {case_info.case_id!r}."
                )

            LOGGER.info(
                "Cas absent de TimescaleDB : ingestion de %d métriques",
                valid_metric_rows_count,
            )
            valid_metric_rows = [
                event
                for event in normalized_records
                if event.get("source") == "metric"
                if _has_finite_numeric_value(event)
            ]
            inserted_metrics = ingest_normalized_events(
                timescale,
                valid_metric_rows,
            )
            ingestion_status = "inserted"

        elif existing_metric_rows == valid_metric_rows_count:
            ingestion_status = (
                "skipped_by_flag"
                if args.skip_timescale_ingestion
                else "already_present"
            )
            LOGGER.info(
                "Cas déjà complet dans TimescaleDB (%d métriques) : "
                "aucune nouvelle insertion",
                existing_metric_rows,
            )

        else:
            raise RuntimeError(
                "État TimescaleDB partiel ou incohérent pour le cas "
                f"{case_info.case_id!r} : {existing_metric_rows} lignes "
                f"existantes contre {valid_metric_rows_count} attendues. "
                "Aucune insertion automatique n'a été effectuée."
            )

        timescale_case_rows_after = existing_metric_rows + inserted_metrics

        

        detector = create_detector(
            method=DetectionMethod(args.method),
            z_threshold=args.z_threshold,
            threshold_quantile=args.threshold_quantile,
            min_baseline_samples=args.min_baseline_samples,
        )
        LOGGER.info("Détection des anomalies avant/après l'instant d'injection")
        anomalies = detector.detect_in_events(
            normalized_records,
            baseline_end=case_info.inject_time_ms,
        )
        if not anomalies:
            raise RuntimeError(
                "Aucune anomalie détectée; aucun IncidentContext ne peut être créé"
            )

        neo4j = Neo4jClient()
        neo4j.verify_connectivity()

        # Neo4j est la source de vérité de la topologie du Digital Twin.
        # Une anomalie portant sur une ressource extérieure à cette topologie
        # ne doit pas être transmise à l'Observer, qui refuse volontairement
        # de créer des nœuds Service implicites.
        service_rows = neo4j.execute(
            "MATCH (s:Service) RETURN s.id AS service_id"
        )
        known_service_ids = {
            str(row["service_id"])
            for row in service_rows
            if row.get("service_id") is not None
        }
        if not known_service_ids:
            raise RuntimeError(
                "Aucun nœud Service n'est disponible dans Neo4j"
            )

        eligible_anomalies = [
            anomaly
            for anomaly in anomalies
            if anomaly.service is not None
            and str(anomaly.service) in known_service_ids
        ]
        outside_topology_anomalies = [
            anomaly
            for anomaly in anomalies
            if anomaly.service is None
            or str(anomaly.service) not in known_service_ids
        ]
        outside_topology_services = sorted(
            {
                str(anomaly.service)
                for anomaly in outside_topology_anomalies
                if anomaly.service is not None
            }
        )

        if outside_topology_anomalies:
            LOGGER.warning(
                "%d anomalie(s) hors topologie Neo4j ignorée(s) : %s",
                len(outside_topology_anomalies),
                ", ".join(outside_topology_services) or "service absent",
            )

        if not eligible_anomalies:
            raise RuntimeError(
                "Aucune anomalie détectée ne correspond à un Service du "
                "Digital Twin Neo4j"
            )

        episodes = _group_anomalies_into_episodes(
            eligible_anomalies,
            gap_seconds=args.episode_gap_seconds,
            context_window_seconds=args.window_minutes * 60,
        )
        LOGGER.info(
            "Regroupement : %d anomalies admissibles -> %d épisodes "
            "(écart maximal=%ds)",
            len(eligible_anomalies),
            len(episodes),
            args.episode_gap_seconds,
        )

        episodes_by_salience = sorted(
            episodes,
            key=lambda episode: episode["summary"]["salience_rank"],
        )
        selected_episodes = episodes_by_salience
        if args.max_episodes is not None:
            selected_episodes = episodes_by_salience[: args.max_episodes]

        LOGGER.info(
            "Sélection de %d épisode(s) par rang de saillance",
            len(selected_episodes),
        )

        selected_anomalies = [
            episode["representative"]
            for episode in selected_episodes
        ]

        observer = ObserverAgent(
            timescale_consumer=TimescaleConsumer(timescale),
            incident_persistence=IncidentPersistence(neo4j),
            window_minutes=args.window_minutes,
            minimum_confidence=args.minimum_confidence,
        )

        incidents: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        for anomaly in selected_anomalies:
            LOGGER.info(
                "Observer : %s / %s / %s",
                anomaly.service,
                anomaly.signal_type,
                anomaly.timestamp,
            )
            try:
                observer_anomaly = _adapt_anomaly_for_observer(anomaly)
                if observer_anomaly is not anomaly:
                    LOGGER.info(
                        "Adaptation du z-score signé %.6f en magnitude %.6f",
                        anomaly.score,
                        observer_anomaly.score,
                    )

                context = observer.process(observer_anomaly)
                incident = context.incident
                temporal_summary = _summarize_temporal_context(
                    context.temporal_context
                )
                incidents.append(
                    {
                        "incident_id": context.incident_id,
                        "case_id": context.case_id,
                        "incident_type": incident.incident_type,
                        "status": incident.status,
                        "severity": incident.severity,
                        "confidence": incident.confidence,
                        "persisted": context.persisted,
                        "anomaly": {
                            "service": anomaly.service,
                            "signal_type": anomaly.signal_type,
                            "timestamp_ms": anomaly.timestamp,
                            "signed_detection_score": anomaly.score,
                            "observer_score": observer_anomaly.score,
                            "method": anomaly.detection_method,
                        },
                        "episode": anomaly.metadata.get("anomaly_episode", {}),
                        "temporal_context": temporal_summary,
                    }
                )
            except Exception as exc:
                failure = {
                    "service": str(anomaly.service),
                    "signal_type": str(anomaly.signal_type),
                    "error": str(exc),
                }
                failures.append(failure)
                LOGGER.exception("Échec du traitement Observer")
                if not args.continue_on_error:
                    raise
        
        evaluation_fault = str(
            case_info.fault
        ).strip().lower()

        expected_incident_type = (
            FAULT_TO_INCIDENT.get(
                evaluation_fault
            )
        )
        
        return {
            "case_id": case_info.case_id,
            "dataset": case_info.dataset,
            "evaluation": {
                "fault": case_info.fault,
                "expected_incident_type": (
                    expected_incident_type.value
                    if expected_incident_type is not None
                    else None
                ),
            },
            "normalized_events": normalized.height,
            "expected_valid_metric_rows": valid_metric_rows_count,
            "timescale_existing_rows_before": existing_metric_rows,
            "timescale_unique_rows_before": existing_unique_metric_rows,
            "timescale_metrics_inserted": inserted_metrics,
            "timescale_case_rows_after": timescale_case_rows_after,
            "timescale_ingestion_status": ingestion_status,
            "invalid_metric_values_skipped": skipped_metric_values,
            "metric_value_rejection_summary": (
                metric_value_rejection_summary
            ),
            "metric_value_rejection_summary": (
                metric_value_rejection_summary
            ),
            "anomalies_detected": len(anomalies),
            "anomalies_eligible_for_observer": len(eligible_anomalies),
            "anomalies_outside_topology": len(outside_topology_anomalies),
            "outside_topology_services": outside_topology_services,
            "episode_gap_seconds": args.episode_gap_seconds,
            "episodes_detected": len(episodes),
            "anomalies_collapsed_into_episodes": (
                len(eligible_anomalies) - len(episodes)
            ),
            "episodes_processed": len(selected_episodes),
            "episode_selection_order": "salience_rank",
            "episodes_fully_covered_by_context_window": sum(
                bool(
                    episode["summary"][
                        "context_window_fully_covers_episode"
                    ]
                )
                for episode in episodes
            ),
            "episodes_partially_covered_by_context_window": sum(
                not bool(
                    episode["summary"][
                        "context_window_fully_covers_episode"
                    ]
                )
                for episode in episodes
            ),
            "selected_episode_ids": [
                episode["summary"]["episode_id"]
                for episode in selected_episodes
            ],
            "episode_summaries": [
                episode["summary"]
                for episode in episodes
            ],
            "anomalies_processed": len(selected_anomalies),
            "incidents_persisted": len(incidents),
            "temporal_observations_retrieved": sum(
                incident["temporal_context"]["rows_retrieved"]
                for incident in incidents
            ),
            "incidents": incidents,
            "failures": failures,
        }
    finally:
        if neo4j is not None:
            neo4j.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        report = run(args)
        _write_report(report, args.output)
        return 0
    except Exception as exc:
        LOGGER.error("Pipeline interrompu : %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
