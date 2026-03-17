from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from weg_case_etl.cli import app
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
    assert "BigQuery-only in Phase 3" in load_result.output


def test_transform_fails_in_local_mode_with_clear_message(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    transform_result = _run("transform", config_path, repo_root)
    assert transform_result.exit_code == 1
    assert "BigQuery-only in Phase 3" in transform_result.output


def test_run_all_fails_in_local_mode_at_bigquery_step(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir, run_mode="local")

    result = _run("run-all", config_path, repo_root)
    assert result.exit_code == 1
    assert "BigQuery-only in Phase 3" in result.output


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
