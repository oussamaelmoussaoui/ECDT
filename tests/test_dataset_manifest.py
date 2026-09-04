import hashlib
import importlib
from types import SimpleNamespace


def make_fake_loader(tmp_path):
    files = {}

    for name in (
        "metrics",
        "logs",
        "traces",
        "ground_truth",
    ):
        path = tmp_path / f"{name}.csv"
        path.write_text(
            f"{name}-content\n",
            encoding="utf-8",
        )
        files[name] = path

    paths = SimpleNamespace(
        metrics_path=files["metrics"],
        logs_path=files["logs"],
        traces_path=files["traces"],
        ground_truth_path=files["ground_truth"],
    )

    cases = {
        "case-b": SimpleNamespace(
            case_id="case-b",
            dataset="RE2-SS",
            fault="delay",
            root_cause_service="service-b",
            time_start_ms=1000,
            inject_time_ms=2000,
            time_end_ms=3000,
            incident_type="db_latency",
        ),
        "case-a": SimpleNamespace(
            case_id="case-a",
            dataset="RE2-OB",
            fault="cpu",
            root_cause_service="service-a",
            time_start_ms=4000,
            inject_time_ms=5000,
            time_end_ms=6000,
            incident_type="cpu_saturation",
        ),
    }

    loader = SimpleNamespace(
        paths=paths,
        list_cases=lambda: ["case-b", "case-a"],
        get_case_info=lambda case_id: cases[case_id],
    )

    inventory_path = (
        tmp_path
        / "data"
        / "rcaeval_cases_inventory.csv"
    )

    inventory_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    inventory_path.write_text(
        (
            "case,n_metrics,n_timesteps,"
            "has_logs,n_logs,has_traces,n_traces\n"
            "case-a,5,3,true,10,true,20\n"
            "case-b,4,3,true,8,false,0\n"
        ),
        encoding="utf-8",
    )

    return loader, files


def test_manifest_records_cases_files_and_provenance(
    tmp_path,
):
    module = importlib.import_module(
        "scripts.generate_dataset_manifest"
    )

    loader, files = make_fake_loader(tmp_path)

    manifest = module.build_manifest(
        loader,
        project_root=tmp_path,
    )

    assert manifest["schema_version"] == "1.1"
    cases_by_id = {
        case["case_id"]: case
        for case in manifest["cases"]
    }

    assert cases_by_id["case-a"]["associated_files"] == [
        "metrics",
        "logs",
        "traces",
        "ground_truth",
    ]

    assert cases_by_id["case-b"]["associated_files"] == [
        "metrics",
        "logs",
        "ground_truth",
    ]

    assert (
        cases_by_id["case-b"]
        ["modalities"]
        ["traces"]
        ["available_in_source"]
        is False
    )

    assert (
        cases_by_id["case-b"]
        ["modalities"]
        ["traces"]
        ["source_row_count"]
        == 0
    )
    assert manifest["selection"]["total_cases"] == 2
    assert manifest["selection"]["by_fault"] == {
        "cpu": 1,
        "delay": 1,
    }

    assert manifest["source_provenance"] == {
        "dataset": "RCAEval",
        "repository_url": None,
        "commit": None,
        "status": "unavailable_in_local_copy",
    }

    assert [
        case["case_id"]
        for case in manifest["cases"]
    ] == ["case-a", "case-b"]

    for source_name, source_path in files.items():
        expected_hash = hashlib.sha256(
            source_path.read_bytes()
        ).hexdigest()

        assert (
            manifest["files"][source_name]["sha256"]
            == expected_hash
        )


def test_manifest_generation_is_deterministic(
    tmp_path,
):
    module = importlib.import_module(
        "scripts.generate_dataset_manifest"
    )

    loader, _ = make_fake_loader(tmp_path)

    first = module.build_manifest(
        loader,
        project_root=tmp_path,
    )

    second = module.build_manifest(
        loader,
        project_root=tmp_path,
    )

    assert first == second