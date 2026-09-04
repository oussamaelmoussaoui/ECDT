from types import SimpleNamespace

from src.ingestion.dataset_integrity import (
    validate_case_integrity,
)


def make_case_info():
    return SimpleNamespace(
        case_id="case-001",
        dataset="RE2-OB",
        fault="cpu",
        root_cause_service="checkoutservice",
        time_start_ms=1_000_000,
        inject_time_ms=2_000_000,
        time_end_ms=3_000_000,
    )


def make_modality_statistics():
    return {
        "metrics": {
            "row_count": 10,
            "timestamp_min": 1000,
            "timestamp_max": 3000,
            "timestamp_unit": "seconds",
            "root_service_present": True,
        },
        "logs": {
            "row_count": 20,
            "timestamp_min": 1000,
            "timestamp_max": 3000,
            "timestamp_unit": "seconds",
            "root_service_present": True,
        },
        "traces": {
            "row_count": 30,
            "timestamp_min": 1_000_000,
            "timestamp_max": 3_000_000,
            "timestamp_unit": "milliseconds",
            "root_service_present": True,
        },
    }


def test_valid_case_integrity():
    result = validate_case_integrity(
        make_case_info(),
        make_modality_statistics(),
    )

    assert result["case_id"] == "case-001"
    assert result["valid"] is True
    assert result["errors"] == []

    assert (
        result["modalities"]["metrics"][
            "covers_injection"
        ]
        is True
    )

    assert (
        result["modalities"]["logs"][
            "time_start_ms"
        ]
        == 1_000_000
    )

    assert (
        result["modalities"]["traces"][
            "time_end_ms"
        ]
        == 3_000_000
    )

    assert (
        result["root_cause_service_present"]
        is True
    )


def test_invalid_case_integrity_reports_reasons():
    statistics = make_modality_statistics()

    statistics["logs"]["row_count"] = 0

    statistics["metrics"][
        "timestamp_unit"
    ] = "minutes"

    for modality in statistics.values():
        modality["root_service_present"] = False

    result = validate_case_integrity(
        make_case_info(),
        statistics,
    )

    assert result["valid"] is False

    assert "logs_empty" in result["errors"]

    assert (
        "metrics_timestamp_unit_invalid"
        in result["errors"]
    )

    assert (
        "root_cause_service_absent"
        in result["errors"]
    )


def test_unavailable_source_modality_is_reported_not_rejected():
    """A modality absent from RCAEval must not invalidate the case."""

    case_info = make_case_info()

    case_info.modalities = {
        "metrics": {
            "available_in_source": True,
        },
        "logs": {
            "available_in_source": True,
        },
        "traces": {
            "available_in_source": False,
            "source_row_count": 0,
        },
    }

    statistics = make_modality_statistics()

    statistics["traces"] = {
        "row_count": 0,
        "timestamp_min": None,
        "timestamp_max": None,
        "timestamp_unit": "milliseconds",
        "root_service_present": False,
    }

    result = validate_case_integrity(
        case_info,
        statistics,
    )

    assert result["valid"] is True
    assert result["errors"] == []

    assert (
        "traces_not_available_in_source"
        in result["warnings"]
    )

    assert (
        result["modalities"]["traces"]["status"]
        == "not_available_in_source"
    )


def test_optional_modality_without_injection_coverage_is_warning():
    """Partial optional telemetry coverage must remain explicit."""

    case_info = make_case_info()

    case_info.modalities = {
        "metrics": {
            "available_in_source": True,
        },
        "logs": {
            "available_in_source": True,
        },
        "traces": {
            "available_in_source": True,
        },
    }

    statistics = make_modality_statistics()

    statistics["logs"] = {
        "row_count": 20,
        "timestamp_min": 2123,
        "timestamp_max": 3000,
        "timestamp_unit": "seconds",
        "root_service_present": False,
    }

    result = validate_case_integrity(
        case_info,
        statistics,
    )

    assert result["valid"] is True
    assert result["errors"] == []

    assert (
        "logs_does_not_cover_injection"
        in result["warnings"]
    )

    assert (
        result["modalities"]["logs"]["status"]
        == (
            "available_but_does_not_cover_injection"
        )
    )