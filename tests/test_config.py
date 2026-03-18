from __future__ import annotations

from pathlib import Path

import pytest

from weg_case_etl.config import ConfigError, load_config


def _write_settings(tmp_path: Path) -> Path:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"project_root: {tmp_path.as_posix()}",
                "run_mode: local",
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
                "  enable_merge_upsert: false",
                "  enable_airflow: false",
                "optional_paths:",
                "  sql_optional_dir: sql/optional",
                "  airflow_dags_dir: orchestration/airflow/dags",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_optional_features_default_to_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _write_settings(tmp_path)
    monkeypatch.delenv("WEG_ENABLE_MERGE_UPSERT", raising=False)
    monkeypatch.delenv("WEG_ENABLE_AIRFLOW", raising=False)

    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")

    assert config.features.enable_merge_upsert is False
    assert config.features.enable_airflow is False
    assert config.optional_paths.sql_optional_dir == (tmp_path / "sql" / "optional").resolve()
    assert config.optional_paths.airflow_dags_dir == (
        tmp_path / "orchestration" / "airflow" / "dags"
    ).resolve()


def test_optional_features_support_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _write_settings(tmp_path)
    monkeypatch.setenv("WEG_ENABLE_MERGE_UPSERT", "true")
    monkeypatch.setenv("WEG_ENABLE_AIRFLOW", "1")
    monkeypatch.setenv("WEG_SQL_OPTIONAL_DIR", "sql/optional/custom")
    monkeypatch.setenv("WEG_AIRFLOW_DAGS_DIR", "orchestration/airflow/custom_dags")

    config = load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")

    assert config.features.enable_merge_upsert is True
    assert config.features.enable_airflow is True
    assert config.optional_paths.sql_optional_dir == (tmp_path / "sql" / "optional" / "custom").resolve()
    assert config.optional_paths.airflow_dags_dir == (
        tmp_path / "orchestration" / "airflow" / "custom_dags"
    ).resolve()


def test_optional_features_reject_invalid_boolean_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_settings(tmp_path)
    monkeypatch.setenv("WEG_ENABLE_AIRFLOW", "maybe")

    with pytest.raises(ConfigError, match="WEG_ENABLE_AIRFLOW"):
        load_config(config_path=config_path, env_path=tmp_path / ".env.not.used")
