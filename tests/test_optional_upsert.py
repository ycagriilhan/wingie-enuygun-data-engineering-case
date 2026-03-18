from __future__ import annotations

import json
from pathlib import Path

import pytest

from weg_case_etl.config import load_config
import weg_case_etl.pipeline as pipeline_module
from weg_case_etl.pipeline import PipelineError


def _write_cloud_config(
    tmp_path: Path,
    *,
    enable_merge_upsert: bool,
    optional_sql_dir: str = "sql/optional",
) -> Path:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                "run_mode: cloud",
                "paths:",
                "  source_dir: data/source",
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
                "features:",
                f"  enable_merge_upsert: {'true' if enable_merge_upsert else 'false'}",
                "  enable_airflow: false",
                "optional_paths:",
                f"  sql_optional_dir: {optional_sql_dir}",
                "  airflow_dags_dir: orchestration/airflow/dags",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def _write_sql(path: Path, body: str = "SELECT 1;") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _write_raw_manifest(config) -> None:
    config.paths.raw_dir.mkdir(parents=True, exist_ok=True)
    payload = {"run_id": "20260101T000000Z", "tables": [{"dataset": "booking"}]}
    (config.paths.raw_dir / "raw_table_manifest.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def test_resolve_transform_sql_plan_uses_mart_when_upsert_disabled(tmp_path: Path) -> None:
    config_path = _write_cloud_config(tmp_path, enable_merge_upsert=False)
    _write_sql(tmp_path / "sql" / "staging" / "01_staging.sql")
    _write_sql(tmp_path / "sql" / "mart" / "01_mart.sql")
    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")

    staging_sql_files, final_sql_files, final_layer = pipeline_module._resolve_transform_sql_plan(config)

    assert final_layer == "mart"
    assert [path.name for path in staging_sql_files] == ["01_staging.sql"]
    assert [path.name for path in final_sql_files] == ["01_mart.sql"]


def test_resolve_transform_sql_plan_uses_optional_when_upsert_enabled(tmp_path: Path) -> None:
    config_path = _write_cloud_config(tmp_path, enable_merge_upsert=True)
    _write_sql(tmp_path / "sql" / "staging" / "01_staging.sql")
    _write_sql(tmp_path / "sql" / "optional" / "01_upsert.sql")
    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")

    staging_sql_files, final_sql_files, final_layer = pipeline_module._resolve_transform_sql_plan(config)

    assert final_layer == "optional_upsert"
    assert [path.name for path in staging_sql_files] == ["01_staging.sql"]
    assert [path.name for path in final_sql_files] == ["01_upsert.sql"]


def test_resolve_transform_sql_plan_fails_when_optional_sql_missing(tmp_path: Path) -> None:
    config_path = _write_cloud_config(
        tmp_path,
        enable_merge_upsert=True,
        optional_sql_dir="sql/optional_missing",
    )
    _write_sql(tmp_path / "sql" / "staging" / "01_staging.sql")
    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")

    with pytest.raises(PipelineError, match="Optional Upsert SQL directory not found"):
        pipeline_module._resolve_transform_sql_plan(config)


def test_transform_executes_mart_layer_when_upsert_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_cloud_config(tmp_path, enable_merge_upsert=False)
    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")
    _write_raw_manifest(config)

    calls: list[dict[str, object]] = []

    def _fake_execute_sql_files(client, config, sql_files, template_values, layer):
        calls.append({"layer": layer, "files": [str(path) for path in sql_files]})
        return [{"layer": layer, "sql_file": "dummy.sql", "job_id": "job-1", "statement_type": "SCRIPT"}]

    monkeypatch.setattr(pipeline_module, "_get_bigquery_client", lambda cfg: (object(), object()))
    monkeypatch.setattr(pipeline_module, "_ensure_dataset", lambda client, bq, dataset, loc: None)
    monkeypatch.setattr(pipeline_module, "_load_staging_sql_files", lambda cfg: [cfg.project_root / "sql/staging.sql"])
    monkeypatch.setattr(pipeline_module, "_load_mart_sql_files", lambda cfg: [cfg.project_root / "sql/mart.sql"])
    monkeypatch.setattr(
        pipeline_module,
        "_load_optional_upsert_sql_files",
        lambda cfg: (_ for _ in ()).throw(AssertionError("optional upsert loader must not run")),
    )
    monkeypatch.setattr(pipeline_module, "_execute_sql_files", _fake_execute_sql_files)
    monkeypatch.setattr(pipeline_module, "_get_table_row_count", lambda client, fqn: 1)

    result = pipeline_module.transform(config)
    assert result["output_table_count"] == 7
    assert [call["layer"] for call in calls] == ["staging", "mart"]


def test_transform_executes_optional_layer_when_upsert_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_cloud_config(tmp_path, enable_merge_upsert=True)
    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")
    _write_raw_manifest(config)

    calls: list[dict[str, object]] = []

    def _fake_execute_sql_files(client, config, sql_files, template_values, layer):
        calls.append({"layer": layer, "files": [str(path) for path in sql_files]})
        return [{"layer": layer, "sql_file": "dummy.sql", "job_id": "job-1", "statement_type": "SCRIPT"}]

    monkeypatch.setattr(pipeline_module, "_get_bigquery_client", lambda cfg: (object(), object()))
    monkeypatch.setattr(pipeline_module, "_ensure_dataset", lambda client, bq, dataset, loc: None)
    monkeypatch.setattr(pipeline_module, "_load_staging_sql_files", lambda cfg: [cfg.project_root / "sql/staging.sql"])
    monkeypatch.setattr(
        pipeline_module,
        "_load_mart_sql_files",
        lambda cfg: (_ for _ in ()).throw(AssertionError("mart loader must be skipped when upsert is enabled")),
    )
    monkeypatch.setattr(
        pipeline_module,
        "_load_optional_upsert_sql_files",
        lambda cfg: [cfg.project_root / "sql/optional/01_booking_enriched_upsert.sql"],
    )
    monkeypatch.setattr(pipeline_module, "_execute_sql_files", _fake_execute_sql_files)
    monkeypatch.setattr(pipeline_module, "_get_table_row_count", lambda client, fqn: 1)

    result = pipeline_module.transform(config)
    assert result["output_table_count"] == 7
    assert [call["layer"] for call in calls] == ["staging", "optional_upsert"]


def test_optional_upsert_sql_contract_is_merge_without_delete() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sql_path = repo_root / "sql" / "optional" / "01_booking_enriched_upsert.sql"
    sql_text = sql_path.read_text(encoding="utf-8")
    sql_text_lower = sql_text.lower()

    assert "merge `{{project_id}}.{{mart_dataset}}.booking_enriched`" in sql_text_lower
    assert "on target.booking_id = source.booking_id" in sql_text_lower
    assert "when matched then" in sql_text_lower
    assert "when not matched then" in sql_text_lower
    assert "not matched by source then delete" not in sql_text_lower
