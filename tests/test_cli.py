from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from weg_case_etl.cli import app


runner = CliRunner()


def _write_config(tmp_path: Path, source_dir: Path) -> Path:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                "run_mode: local",
                "paths:",
                f"  source_dir: {source_dir.as_posix()}",
                "  landing_dir: data/landing",
                "  raw_dir: data/raw",
                "  staging_dir: data/staging",
                "  mart_dir: data/mart",
                "  artifact_dir: reports/artifacts",
                "cloud:",
                "  bucket: ${GCS_BUCKET}",
                "  project_id: ${GCP_PROJECT_ID}",
                "  landing_prefix: ${GCS_LANDING_PREFIX}",
                "  dataset_raw: ${GCP_DATASET_RAW}",
                "  dataset_staging: ${GCP_DATASET_STAGING}",
                "  dataset_mart: ${GCP_DATASET_MART}",
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


def test_phase1_commands_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir)

    for command in ["profile", "extract-upload", "load-raw", "transform", "dq"]:
        result = _run(command, config_path, repo_root)
        assert result.exit_code == 0, result.output
        assert "completed" in result.output

    run_all_result = _run("run-all", config_path, repo_root)
    assert run_all_result.exit_code == 0, run_all_result.output


def test_profile_fails_for_missing_source_files(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    missing_source_dir = tmp_path / "missing_source"
    missing_source_dir.mkdir(parents=True, exist_ok=True)
    config_path = _write_config(tmp_path, missing_source_dir)

    result = _run("profile", config_path, repo_root)
    assert result.exit_code == 1
    assert "Required source files not found" in result.output


def test_run_all_executes_in_required_order(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir)

    result = _run("run-all", config_path, repo_root)
    assert result.exit_code == 0, result.output

    run_all_report_path = tmp_path / "reports" / "artifacts" / "run_all_report.json"
    payload = json.loads(run_all_report_path.read_text(encoding="utf-8"))

    assert payload["expected_order"] == ["profile", "extract-upload", "load-raw", "transform", "dq"]
    assert payload["executed_order"] == ["profile", "extract-upload", "load-raw", "transform", "dq"]

    raw_manifest_path = tmp_path / "data" / "raw" / "raw_table_manifest.json"
    raw_manifest = json.loads(raw_manifest_path.read_text(encoding="utf-8"))
    target_tables = {row["target_table"] for row in raw_manifest["tables"]}
    assert target_tables == {
        "raw.airport_reference",
        "raw.booking",
        "raw.provider",
        "raw.search",
    }


def test_extract_upload_writes_classified_landing_manifest(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_dir = repo_root / "data" / "source"
    config_path = _write_config(tmp_path, source_dir)

    result = _run("extract-upload", config_path, repo_root)
    assert result.exit_code == 0, result.output

    manifest_path = tmp_path / "reports" / "artifacts" / "extract_upload_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_id = manifest["run_id"]
    landed_files = manifest["landed_files"]
    assert len(landed_files) == 4

    dataset_names = {row["dataset"] for row in landed_files}
    assert dataset_names == {"airport_reference", "booking", "provider", "search"}
    assert manifest["cloud_upload"]["status"] == "not_applicable_in_local_mode"

    for row in landed_files:
        relative_path = row["landing_relative_path"]
        assert run_id in relative_path
        assert row["dataset"] in relative_path
        assert row["file_size_bytes"] > 0
