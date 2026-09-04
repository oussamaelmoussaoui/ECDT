"""Tests for the repaired, ground-truth-safe Phase 2 evaluator."""

from types import SimpleNamespace

import pytest

from evaluation import phase2_evaluation as evaluation
from src.ingestion.models import AnomalyEvent, DetectionMethod, IncidentType


def _case(case_id, fault):
    return SimpleNamespace(
        case_id=case_id,
        dataset="RE2-OB",
        fault=fault,
        root_cause_service="service-a",
        time_start_ms=0,
        inject_time_ms=6000,
        time_end_ms=12000,
    )


def _metric(signal, value=1.0):
    return {
        "source": "metric",
        "case_id": "case-001",
        "timestamp_ms": 7000,
        "service_name": "service-a",
        "signal_type": signal,
        "metric_name": f"service-a_{signal}",
        "value": value,
    }


def _success_report(case_id, fault):
    return {
        "case_id": case_id,
        "status": "success",
        "operational": {
            "metric_quality": {"all_rejections_explained": True},
            "predictions": {"observer_score_contract_violations": 0},
            "reproducibility": {"identical": True},
        },
        "evaluation": {
            "ground_truth": {"fault": fault},
            "detected": True,
            "semantic_signal_coverage": {"semantic_input_available": True},
        },
    }


def _keys(value):
    if isinstance(value, dict):
        for key, nested in value.items():
            yield str(key).lower()
            yield from _keys(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _keys(nested)


def test_pilot_selection_is_balanced_and_deterministic():
    cases = [
        _case(f"{fault}-{index}", fault)
        for fault in reversed(evaluation.SUPPORTED_FAULTS)
        for index in (4, 2, 1, 3)
    ]

    first = evaluation.select_pilot_case_ids(cases, cases_per_fault=3)
    second = evaluation.select_pilot_case_ids(reversed(cases), cases_per_fault=3)

    assert first == second
    assert len(first) == 12
    for fault in evaluation.SUPPORTED_FAULTS:
        assert sum(item.startswith(f"{fault}-") for item in first) == 3


@pytest.mark.parametrize(
    ("fault", "signal"),
    [
        ("cpu", "cpu"),
        ("delay", "latency-90"),
        ("loss", "error"),
        ("socket", "socket"),
    ],
)
def test_each_fault_has_semantically_relevant_detector_input(fault, signal):
    summary = evaluation.summarize_semantic_signal_coverage(
        [_metric(signal), _metric("mem")],
        fault=fault,
    )

    assert summary["semantic_input_available"] is True
    assert summary["relevant_metric_rows"] == 1
    assert signal in summary["detector_signal_types"]


def test_evaluate_one_case_does_not_pass_labels_to_detector():
    case_info = _case("case-001", "cpu")
    records = [_metric("cpu")]

    class Frame:
        def to_dicts(self):
            return records

    class Loader:
        def get_case_info(self, case_id):
            assert case_id == "case-001"
            return case_info

        def load_metrics(self, **kwargs):
            assert kwargs == {
                "case_id": "case-001",
                "time_start_ms": 0,
                "time_end_ms": 12000,
                "long_format": True,
            }
            return object()

    class Normalizer:
        def normalize_metrics(self, metrics):
            return Frame()

    class Detector:
        def __init__(self):
            self.calls = []

        def detect_in_events(self, events, **kwargs):
            self.calls.append((events, kwargs))
            return [
                AnomalyEvent(
                    event_id="a-1",
                    case_id="case-001",
                    timestamp=7000,
                    service="service-a",
                    signal_type="cpu",
                    value=10.0,
                    detection_method=DetectionMethod.Z_SCORE,
                    score=-4.0,
                    incident_type=IncidentType.CPU_SATURATION,
                )
            ]

    detector = Detector()
    report = evaluation.evaluate_one_case(
        Loader(),
        Normalizer(),
        detector,
        "case-001",
    )

    assert len(detector.calls) == 2
    for _, kwargs in detector.calls:
        assert kwargs == {"baseline_end": 6000}
    assert "ground_truth" not in report["operational"]
    assert "semantic_signal_coverage" not in report["operational"]
    assert not {
        "fault",
        "root_cause_service",
        "expected_incident_type",
    }.intersection(_keys(report["operational"]))
    assert report["evaluation"]["ground_truth"]["fault"] == "cpu"
    prediction = report["operational"]["predictions"]["sample"][0]
    assert prediction["signed_detection_score"] == -4.0
    assert prediction["observer_score"] == 4.0
    assert report["operational"]["reproducibility"]["identical"] is True


def test_default_campaign_executes_three_cases_per_fault(monkeypatch):
    cases = [
        _case(f"{fault}-{index}", fault)
        for fault in evaluation.SUPPORTED_FAULTS
        for index in range(1, 5)
    ]
    by_id = {item.case_id: item for item in cases}

    class Loader:
        def list_cases(self):
            return list(reversed(sorted(by_id)))

        def get_case_info(self, case_id):
            return by_id[case_id]

    monkeypatch.setattr(
        evaluation,
        "evaluate_one_case",
        lambda loader, normalizer, detector, case_id, **kwargs: (
            _success_report(case_id, by_id[case_id].fault)
        ),
    )

    report = evaluation.run_evaluation(
        loader=Loader(),
        normalizer=object(),
        detector=object(),
    )

    assert report["selection"]["mode"] == "balanced_pilot"
    assert report["aggregate"]["selected_cases"] == 12
    assert report["aggregate"]["failed_cases"] == 0
    assert set(report["aggregate"]["by_fault"]) == set(
        evaluation.SUPPORTED_FAULTS
    )
    assert all(
        stats["total"] == 3
        for stats in report["aggregate"]["by_fault"].values()
    )
