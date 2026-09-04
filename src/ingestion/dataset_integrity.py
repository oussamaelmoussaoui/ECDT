"""Validation d'intégrité des cas RCAEval avant traitement."""

from __future__ import annotations

import math
from typing import Any


MODALITIES = (
    "metrics",
    "logs",
    "traces",
)

TIMESTAMP_FACTORS_TO_MS = {
    "seconds": 1000.0,
    "second": 1000.0,
    "s": 1000.0,
    "milliseconds": 1.0,
    "millisecond": 1.0,
    "ms": 1.0,
}


def _get_value(
    value: Any,
    name: str,
    default: Any = None,
) -> Any:
    """Read a field from either a mapping or an object."""

    if isinstance(value, dict):
        return value.get(name, default)

    return getattr(value, name, default)


def _finite_float(value: Any) -> float | None:
    """Convert a finite numeric value to float."""

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(numeric_value):
        return None

    return numeric_value


def _append_error(
    errors: list[str],
    error_code: str,
) -> None:
    """Append an error code only once."""

    if error_code not in errors:
        errors.append(error_code)


def validate_case_integrity(
    case_info: Any,
    modality_statistics: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Validate one RCAEval case before operational processing.

    A modality explicitly unavailable in the RCAEval source is
    reported but does not invalidate the case.

    Metrics remain mandatory. Missing temporal coverage in optional
    logs or traces is reported as a warning.
    """

    errors: list[str] = []
    warnings: list[str] = []

    case_id = _get_value(
        case_info,
        "case_id",
    )

    dataset = _get_value(
        case_info,
        "dataset",
    )

    fault = _get_value(
        case_info,
        "fault",
    )

    root_cause_service = _get_value(
        case_info,
        "root_cause_service",
    )

    declared_modalities = (
        _get_value(
            case_info,
            "modalities",
            {},
        )
        or {}
    )

    ground_truth_start = _finite_float(
        _get_value(
            case_info,
            "time_start_ms",
        )
    )

    injection_timestamp = _finite_float(
        _get_value(
            case_info,
            "inject_time_ms",
        )
    )

    ground_truth_end = _finite_float(
        _get_value(
            case_info,
            "time_end_ms",
        )
    )

    ground_truth_window_valid = (
        ground_truth_start is not None
        and injection_timestamp is not None
        and ground_truth_end is not None
        and ground_truth_start
        < injection_timestamp
        < ground_truth_end
    )

    if not ground_truth_window_valid:
        _append_error(
            errors,
            "ground_truth_window_invalid",
        )

    modality_results: dict[
        str,
        dict[str, Any],
    ] = {}

    root_service_found = False

    for modality in MODALITIES:
        declaration = (
            _get_value(
                declared_modalities,
                modality,
                {},
            )
            or {}
        )

        available_in_source = bool(
            _get_value(
                declaration,
                "available_in_source",
                True,
            )
        )

        source_row_count = _get_value(
            declaration,
            "source_row_count",
        )

        statistics = modality_statistics.get(
            modality
        )

        if statistics is None:
            row_count = 0
            timestamp_unit = None
        else:
            raw_row_count = _get_value(
                statistics,
                "row_count",
                0,
            )

            try:
                row_count = int(
                    raw_row_count
                )
            except (TypeError, ValueError):
                row_count = 0

                _append_error(
                    errors,
                    f"{modality}_row_count_invalid",
                )

            raw_unit = _get_value(
                statistics,
                "timestamp_unit",
            )

            timestamp_unit = (
                str(raw_unit).strip().lower()
                if raw_unit is not None
                else None
            )

        modality_result = {
            "available_in_source": (
                available_in_source
            ),
            "source_row_count": (
                source_row_count
            ),
            "row_count": row_count,
            "timestamp_unit": timestamp_unit,
            "time_start_ms": None,
            "time_end_ms": None,
            "covers_injection": False,
            "root_cause_service_present": False,
            "status": "invalid",
        }

        if not available_in_source:
            if row_count > 0:
                _append_error(
                    errors,
                    (
                        f"{modality}"
                        "_unexpected_data_present"
                    ),
                )

                modality_result["status"] = (
                    "invalid"
                )
            else:
                _append_error(
                    warnings,
                    (
                        f"{modality}"
                        "_not_available_in_source"
                    ),
                )

                modality_result["status"] = (
                    "not_available_in_source"
                )

            modality_results[modality] = (
                modality_result
            )
            continue

        if statistics is None:
            _append_error(
                errors,
                f"{modality}_statistics_missing",
            )

            modality_results[modality] = (
                modality_result
            )
            continue

        if row_count <= 0:
            _append_error(
                errors,
                f"{modality}_empty",
            )

            modality_results[modality] = (
                modality_result
            )
            continue

        modality_root_service_present = bool(
            _get_value(
                statistics,
                "root_service_present",
                False,
            )
        )

        modality_result[
            "root_cause_service_present"
        ] = modality_root_service_present

        root_service_found = (
            root_service_found
            or modality_root_service_present
        )

        timestamp_factor = (
            TIMESTAMP_FACTORS_TO_MS.get(
                timestamp_unit
            )
        )

        if timestamp_factor is None:
            _append_error(
                errors,
                (
                    f"{modality}"
                    "_timestamp_unit_invalid"
                ),
            )

            modality_results[modality] = (
                modality_result
            )
            continue

        raw_start = _finite_float(
            _get_value(
                statistics,
                "timestamp_min",
            )
        )

        raw_end = _finite_float(
            _get_value(
                statistics,
                "timestamp_max",
            )
        )

        if (
            raw_start is None
            or raw_end is None
            or raw_end < raw_start
        ):
            _append_error(
                errors,
                (
                    f"{modality}"
                    "_timestamp_range_invalid"
                ),
            )

            modality_results[modality] = (
                modality_result
            )
            continue

        time_start_ms = (
            raw_start
            * timestamp_factor
        )

        time_end_ms = (
            raw_end
            * timestamp_factor
        )

        modality_result["time_start_ms"] = (
            time_start_ms
        )

        modality_result["time_end_ms"] = (
            time_end_ms
        )

        covers_injection = (
            injection_timestamp is not None
            and time_start_ms
            <= injection_timestamp
            <= time_end_ms
        )

        modality_result[
            "covers_injection"
        ] = covers_injection

        if covers_injection:
            modality_result["status"] = (
                "available"
            )

        elif modality == "metrics":
            _append_error(
                errors,
                "metrics_does_not_cover_injection",
            )

            modality_result["status"] = (
                "invalid"
            )

        else:
            _append_error(
                warnings,
                (
                    f"{modality}"
                    "_does_not_cover_injection"
                ),
            )

            modality_result["status"] = (
                "available_but_does_not_cover_injection"
            )

        modality_results[modality] = (
            modality_result
        )

    if not root_service_found:
        _append_error(
            errors,
            "root_cause_service_absent",
        )

    return {
        "case_id": case_id,
        "dataset": dataset,
        "fault": fault,
        "root_cause_service": (
            root_cause_service
        ),
        "ground_truth": {
            "time_start_ms": (
                ground_truth_start
            ),
            "inject_time_ms": (
                injection_timestamp
            ),
            "time_end_ms": (
                ground_truth_end
            ),
            "window_valid": (
                ground_truth_window_valid
            ),
        },
        "modalities": modality_results,
        "root_cause_service_present": (
            root_service_found
        ),
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }