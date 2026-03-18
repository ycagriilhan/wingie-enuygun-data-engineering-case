from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from weg_case_etl.cli import app
from weg_case_etl.config import load_config
import weg_case_etl.pipeline as pipeline_module
from weg_case_etl.pipeline import PipelineError, _render_sql_template


runner = CliRunner()


def _write_config(tmp_path: Path, source_dir: Path, run_mode: str = "local") -> Path:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                f"run_mode: {run_mode}",
                "paths:",
                f"  source_dir: {source_dir.as_posix()}",
                "  landing_dir: data/landing",
                "  raw_dir: data/raw",
                "  staging_dir: data/staging",
                "  mart_dir: data/mart",
                "  artifact_dir: reports/artifacts",
                "cloud:",
                "  bucket: test-bucket",
                "  project_id: test-project",
                "  landing_prefix: landing",
                "  bigquery_location: EU",
                "  dataset_raw: raw",
                "  dataset_staging: staging",
                "  dataset_mart: mart",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _run(command: str, config_path: Path, repo_root: Path):
    return runner.invoke(
        app,
        [command, "--config", str(config_path), "--env-file", str(repo_root / ".env.not.used")],
    )


def _write_cloud_phase_reports(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "reports" / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "extract_upload_manifest.json").write_text(
        json.dumps({"command": "extract-upload"}, indent=2), encoding="utf-8"
    )
    (artifact_dir / "load_raw_report.json").write_text(
        json.dumps({"command": "load-raw"}, indent=2), encoding="utf-8"
    )
    (artifact_dir / "transform_report.json").write_text(
        json.dumps({"command": "transform"}, indent=2), encoding="utf-8"
    )
    return artifact_dir


def _write_json_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_cli_json_payload(output: str) -> dict:
    lines = output.splitlines()
    for index, line in enumerate(lines):
        if line.strip().startswith("{"):
            return json.loads("\n".join(lines[index:]))
    raise AssertionError(f"Could not find JSON payload in command output: {output!r}")


def _write_validation_artifacts(
    artifact_dir: Path,
    *,
    run_mode: str,
    run_id: str = "20260101T000000Z",
    load_raw_run_id: str | None = None,
    dq_status: str = "pass",
) -> None:
    _write_json_payload(
        artifact_dir / "profile_summary.json",
        {
            "command": "profile",
            "run_mode": run_mode,
        },
    )
    _write_json_payload(
        artifact_dir / "extract_upload_manifest.json",
        {
            "command": "extract-upload",
            "run_id": run_id,
            "run_mode": run_mode,
        },
    )

    executed_order = ["profile", "extract-upload"]
    results = [
        {
            "step": "profile",
            "result": {"command": "profile"},
        },
        {
            "step": "extract-upload",
            "result": {"command": "extract-upload", "run_id": run_id},
        },
    ]

    if run_mode == "cloud":
        load_raw_effective_run_id = load_raw_run_id or run_id
        _write_json_payload(
            artifact_dir / "load_raw_report.json",
            {
                "command": "load-raw",
                "run_id": load_raw_effective_run_id,
                "project_id": "test-project",
                "load_jobs": [{"job_id": "job-1"}, {"job_id": "job-2"}],
            },
        )
        _write_json_payload(
            artifact_dir / "transform_report.json",
            {
                "command": "transform",
                "project_id": "test-project",
                "output_tables": [{"table": "t1"}, {"table": "t2"}],
            },
        )
        _write_json_payload(
            artifact_dir / "dq_report.json",
            {
                "command": "dq",
                "overall_status": dq_status,
            },
        )
        executed_order.extend(["load-raw", "transform", "dq"])
        results.extend(
            [
                {
                    "step": "load-raw",
                    "result": {"command": "load-raw", "table_count": 2},
                },
                {
                    "step": "transform",
                    "result": {"command": "transform", "output_table_count": 2},
                },
                {
                    "step": "dq",
                    "result": {"command": "dq", "overall_status": dq_status},
                },
            ]
        )

    _write_json_payload(
        artifact_dir / "run_all_report.json",
        {
            "command": "run-all",
            "overall_status": "pass",
            "failed_step": None,
            "error_message": None,
            "expected_order": ["profile", "extract-upload", "load-raw", "transform", "dq"],
            "executed_order": executed_order,
            "results": results,
        },
    )


def test_profile_and_extract_upload_local_success(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    profile_result = _run("profile", config_path, repo_root)
    assert profile_result.exit_code == 0, profile_result.output

    extract_result = _run("extract-upload", config_path, repo_root)
    assert extract_result.exit_code == 0, extract_result.output

    manifest_path = tmp_path / "reports" / "artifacts" / "extract_upload_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["cloud_upload"]["status"] == "not_applicable_in_local_mode"
    assert len(manifest["landed_files"]) == 4


def test_load_raw_fails_in_local_mode_with_clear_message(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    _run("extract-upload", config_path, repo_root)
    load_result = _run("load-raw", config_path, repo_root)
    assert load_result.exit_code == 1
    assert "requires BigQuery execution" in load_result.output


def test_transform_fails_in_local_mode_with_clear_message(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    transform_result = _run("transform", config_path, repo_root)
    assert transform_result.exit_code == 1
    assert "requires BigQuery execution" in transform_result.output


def test_run_all_fails_in_local_mode_at_bigquery_step(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    result = _run("run-all", config_path, repo_root)
    assert result.exit_code == 1
    assert "requires BigQuery execution" in result.output

    report_path = tmp_path / "reports" / "artifacts" / "run_all_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["overall_status"] == "fail"
    assert report["failed_step"] == "load-raw"
    assert "requires BigQuery execution" in report["error_message"]
    assert report["executed_order"] == ["profile", "extract-upload"]

    evidence_path = tmp_path / "reports" / "artifacts" / "validation_evidence_report.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["overall_status"] == "pending"


def test_load_raw_requires_gcs_uri_in_cloud_mode(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")

    artifact_dir = tmp_path / "reports" / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    fake_manifest = {
        "command": "extract-upload",
        "run_id": "20260101T000000Z",
        "landed_files": [
            {
                "file_name": "booking_10k.parquet",
                "dataset": "booking",
                "domain": "fact",
                "landing_path": str(tmp_path / "data" / "landing" / "booking_10k.parquet"),
                "gcs_uri": "",
            }
        ],
    }
    (artifact_dir / "extract_upload_manifest.json").write_text(
        json.dumps(fake_manifest, indent=2), encoding="utf-8"
    )

    load_result = _run("load-raw", config_path, repo_root)
    assert load_result.exit_code == 1
    assert "missing gcs_uri" in load_result.output.lower()


def test_sql_templates_include_dedup_and_reject_contracts() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sql_dir = repo_root / "sql" / "staging"
    files = {path.name: path.read_text(encoding="utf-8") for path in sql_dir.glob("*.sql")}

    assert set(files) == {
        "01_provider_clean.sql",
        "02_airport_reference_clean.sql",
        "03_booking_clean.sql",
        "04_booking_reject.sql",
        "05_search_clean.sql",
        "06_search_reject.sql",
    }
    for content in files.values():
        assert "{{PROJECT_ID}}" in content
        assert "{{RAW_DATASET}}" in content or "{{STAGING_DATASET}}" in content

    booking_clean = files["03_booking_clean.sql"]
    assert "PARTITION BY booking_id" in booking_clean
    assert "ORDER BY updated_at DESC, created_at DESC" in booking_clean
    assert "direction_flag_mismatch" in booking_clean

    search_clean = files["05_search_clean.sql"]
    assert "PARTITION BY request_id" in search_clean
    assert "ORDER BY created_at DESC, session_id DESC" in search_clean
    assert "direction_return_date_mismatch" in search_clean

    booking_reject = files["04_booking_reject.sql"]
    search_reject = files["06_search_reject.sql"]
    assert "duplicate_not_latest" in booking_reject
    assert "duplicate_not_latest" in search_reject


def test_mart_sql_template_includes_required_joins() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    mart_sql_dir = repo_root / "sql" / "mart"
    files = {path.name: path.read_text(encoding="utf-8") for path in mart_sql_dir.glob("*.sql")}

    assert set(files) == {"01_booking_enriched.sql"}
    booking_enriched = files["01_booking_enriched.sql"]
    assert "{{PROJECT_ID}}" in booking_enriched
    assert "{{STAGING_DATASET}}" in booking_enriched
    assert "{{MART_DATASET}}" in booking_enriched
    assert "booking_clean" in booking_enriched
    assert "search_clean" in booking_enriched
    assert "provider_clean" in booking_enriched
    assert "airport_reference_clean" in booking_enriched
    assert "b.request_id = s.request_id" in booking_enriched


def test_dq_cloud_mode_passes_when_all_mandatory_checks_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")
    _write_cloud_phase_reports(tmp_path)

    monkeypatch.setattr(pipeline_module, "_get_bigquery_client", lambda config: (object(), object()))
    monkeypatch.setattr(
        pipeline_module,
        "_evaluate_mandatory_business_rule_checks",
        lambda config, client: [
            {
                "name": "booking_origin_destination_not_equal",
                "status": "pass",
                "message": "ok",
                "metric_value": 0,
                "expected_value": 0,
            }
        ],
    )

    result = _run("dq", config_path, repo_root)
    assert result.exit_code == 0, result.output

    report_path = tmp_path / "reports" / "artifacts" / "dq_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["overall_status"] == "pass"


def test_dq_cloud_mode_fails_when_mandatory_checks_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")
    _write_cloud_phase_reports(tmp_path)

    monkeypatch.setattr(pipeline_module, "_get_bigquery_client", lambda config: (object(), object()))
    monkeypatch.setattr(
        pipeline_module,
        "_evaluate_mandatory_business_rule_checks",
        lambda config, client: [
            {
                "name": "booking_negative_total_absent",
                "status": "fail",
                "message": "booking_clean must not contain negative total.",
                "metric_value": 3,
                "expected_value": 0,
            }
        ],
    )

    result = _run("dq", config_path, repo_root)
    assert result.exit_code == 1
    assert "DQ checks failed" in result.output

    report_path = tmp_path / "reports" / "artifacts" / "dq_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["overall_status"] == "fail"


def test_run_all_fails_when_dq_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")

    run_id = "20260101T000000Z"

    def _profile(config):
        _write_json_payload(config.paths.artifact_dir / "profile_summary.json", {"command": "profile"})
        return {"command": "profile"}

    def _extract(config):
        _write_json_payload(
            config.paths.artifact_dir / "extract_upload_manifest.json",
            {"command": "extract-upload", "run_id": run_id},
        )
        return {"command": "extract-upload", "run_id": run_id}

    def _load_raw(config):
        _write_json_payload(
            config.paths.artifact_dir / "load_raw_report.json",
            {"command": "load-raw", "run_id": run_id, "load_jobs": [{"job_id": "job-1"}]},
        )
        return {"command": "load-raw", "table_count": 1}

    def _transform(config):
        _write_json_payload(
            config.paths.artifact_dir / "transform_report.json",
            {"command": "transform", "output_tables": [{"table": "t1"}]},
        )
        return {"command": "transform", "output_table_count": 1}

    monkeypatch.setattr(pipeline_module, "profile", _profile)
    monkeypatch.setattr(pipeline_module, "extract_upload", _extract)
    monkeypatch.setattr(pipeline_module, "load_raw", _load_raw)
    monkeypatch.setattr(pipeline_module, "transform", _transform)

    def _failing_dq(config):
        raise PipelineError("forced dq failure")

    monkeypatch.setattr(pipeline_module, "dq", _failing_dq)

    result = _run("run-all", config_path, repo_root)
    assert result.exit_code == 1
    assert "forced dq failure" in result.output

    report_path = tmp_path / "reports" / "artifacts" / "run_all_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["overall_status"] == "fail"
    assert report["failed_step"] == "dq"
    assert report["executed_order"] == ["profile", "extract-upload", "load-raw", "transform"]

    evidence_path = tmp_path / "reports" / "artifacts" / "validation_evidence_report.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["overall_status"] == "fail"


def test_run_all_success_writes_validation_evidence_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")
    run_id = "20260101T000000Z"

    def _profile(config):
        report_path = config.paths.artifact_dir / "profile_summary.json"
        _write_json_payload(report_path, {"command": "profile"})
        return {"command": "profile", "report_path": str(report_path)}

    def _extract(config):
        manifest_path = config.paths.artifact_dir / "extract_upload_manifest.json"
        _write_json_payload(manifest_path, {"command": "extract-upload", "run_id": run_id})
        return {
            "command": "extract-upload",
            "manifest_path": str(manifest_path),
            "run_id": run_id,
            "landed_file_count": 4,
            "cloud_status": "uploaded",
        }

    def _load_raw(config):
        report_path = config.paths.artifact_dir / "load_raw_report.json"
        _write_json_payload(
            report_path,
            {
                "command": "load-raw",
                "run_id": run_id,
                "project_id": "test-project",
                "load_jobs": [{"job_id": "job-1"}, {"job_id": "job-2"}],
            },
        )
        return {"command": "load-raw", "report_path": str(report_path), "table_count": 2}

    def _transform(config):
        report_path = config.paths.artifact_dir / "transform_report.json"
        _write_json_payload(
            report_path,
            {
                "command": "transform",
                "project_id": "test-project",
                "output_tables": [{"table": "t1"}, {"table": "t2"}],
            },
        )
        return {"command": "transform", "report_path": str(report_path), "output_table_count": 2}

    def _dq(config):
        report_path = config.paths.artifact_dir / "dq_report.json"
        _write_json_payload(report_path, {"command": "dq", "overall_status": "pass"})
        return {"command": "dq", "overall_status": "pass", "report_path": str(report_path)}

    monkeypatch.setattr(pipeline_module, "profile", _profile)
    monkeypatch.setattr(pipeline_module, "extract_upload", _extract)
    monkeypatch.setattr(pipeline_module, "load_raw", _load_raw)
    monkeypatch.setattr(pipeline_module, "transform", _transform)
    monkeypatch.setattr(pipeline_module, "dq", _dq)

    result = _run("run-all", config_path, repo_root)
    assert result.exit_code == 0, result.output

    payload = _parse_cli_json_payload(result.output)
    assert "validation_evidence_report_path" in payload

    run_all_report_path = Path(payload["report_path"])
    evidence_report_path = Path(payload["validation_evidence_report_path"])
    assert run_all_report_path.exists()
    assert evidence_report_path.exists()

    run_all_report = json.loads(run_all_report_path.read_text(encoding="utf-8"))
    assert run_all_report["overall_status"] == "pass"
    assert run_all_report["failed_step"] is None
    assert run_all_report["executed_order"] == ["profile", "extract-upload", "load-raw", "transform", "dq"]

    evidence_report = json.loads(evidence_report_path.read_text(encoding="utf-8"))
    assert evidence_report["overall_status"] == "pass"


def test_validation_evidence_report_cloud_passes_with_consistent_artifacts(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")
    config = load_config(config_path=config_path, env_path=repo_root / ".env.not.used")
    _write_validation_artifacts(config.paths.artifact_dir, run_mode="cloud")

    report = pipeline_module._build_validation_evidence_report(config)
    assert report["overall_status"] == "pass"
    assert report["summary"]["fail_count"] == 0
    assert report["summary"]["pending_count"] == 0


def test_validation_evidence_report_cloud_fails_on_run_id_mismatch(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="cloud")
    config = load_config(config_path=config_path, env_path=repo_root / ".env.not.used")
    _write_validation_artifacts(
        config.paths.artifact_dir,
        run_mode="cloud",
        run_id="20260101T000000Z",
        load_raw_run_id="20260101T000001Z",
    )

    report = pipeline_module._build_validation_evidence_report(config)
    check_map = {check["name"]: check for check in report["checks"]}
    assert report["overall_status"] == "fail"
    assert check_map["run_id_consistency_across_reports"]["status"] == "fail"


def test_validation_evidence_report_local_marks_cloud_checks_pending(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")
    config = load_config(config_path=config_path, env_path=repo_root / ".env.not.used")
    _write_validation_artifacts(config.paths.artifact_dir, run_mode="local")

    report = pipeline_module._build_validation_evidence_report(config)
    check_map = {check["name"]: check for check in report["checks"]}
    assert report["overall_status"] == "pending"
    assert check_map["load_raw_report_exists"]["status"] == "pending"
    assert check_map["transform_report_exists"]["status"] == "pending"
    assert check_map["dq_report_exists"]["status"] == "pending"
    assert check_map["run_all_step_order_consistency"]["status"] == "pass"


def test_sql_template_renderer_replaces_all_placeholders() -> None:
    rendered = _render_sql_template(
        "SELECT '{{PROJECT_ID}}' p, '{{RAW_DATASET}}' r, '{{STAGING_DATASET}}' s",
        {
            "PROJECT_ID": "p1",
            "RAW_DATASET": "raw",
            "STAGING_DATASET": "staging",
        },
    )
    assert "{{" not in rendered
    assert "p1" in rendered
    assert "raw" in rendered
    assert "staging" in rendered


def test_sql_template_renderer_fails_on_unresolved_placeholders() -> None:
    with pytest.raises(PipelineError):
        _render_sql_template(
            "SELECT '{{PROJECT_ID}}', '{{RAW_DATASET}}', '{{UNKNOWN}}'",
            {"PROJECT_ID": "p1", "RAW_DATASET": "raw"},
        )
