from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

from weg_case_etl.contracts import COMMAND_ORDER


def _load_dag_module():
    repo_root = Path(__file__).resolve().parents[1]
    dag_path = repo_root / "orchestration" / "airflow" / "dags" / "weg_case_optional_etl_dag.py"
    spec = importlib.util.spec_from_file_location("phase08_optional_airflow_dag", dag_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load DAG module from path: {dag_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_settings(tmp_path: Path, *, enable_airflow: bool) -> Path:
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
                f"  enable_airflow: {'true' if enable_airflow else 'false'}",
                "optional_paths:",
                "  sql_optional_dir: sql/optional",
                "  airflow_dags_dir: orchestration/airflow/dags",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_dag_command_order_matches_pipeline_contract() -> None:
    dag_module = _load_dag_module()

    assert dag_module.CLI_COMMAND_ORDER == tuple(COMMAND_ORDER)
    assert tuple(COMMAND_ORDER) == ("profile", "extract-upload", "load-raw", "transform", "dq")


def test_airflow_flag_guard_fails_when_disabled(tmp_path: Path) -> None:
    dag_module = _load_dag_module()
    config_path = _write_settings(tmp_path, enable_airflow=False)
    env_file = tmp_path / ".env.not.used"

    with pytest.raises(RuntimeError, match="WEG_ENABLE_AIRFLOW"):
        dag_module.validate_airflow_feature_flag(config_path=config_path, env_file=env_file)


def test_airflow_flag_guard_passes_when_enabled(tmp_path: Path) -> None:
    dag_module = _load_dag_module()
    config_path = _write_settings(tmp_path, enable_airflow=True)
    env_file = tmp_path / ".env.not.used"

    result = dag_module.validate_airflow_feature_flag(config_path=config_path, env_file=env_file)

    assert result["status"] == "enabled"
    assert Path(result["config_path"]).resolve() == config_path.resolve()


def test_dag_chain_contract_is_deterministic_and_linear() -> None:
    dag_module = _load_dag_module()
    expected_chain = (
        dag_module.AIRFLOW_FLAG_GUARD_TASK_ID,
        "run_profile",
        "run_extract_upload",
        "run_load_raw",
        "run_transform",
        "run_dq",
    )

    assert dag_module.LINEAR_TASK_CHAIN == expected_chain

    if dag_module.AIRFLOW_AVAILABLE:
        dag = dag_module.dag
        assert dag is not None

        for index, task_id in enumerate(expected_chain[:-1]):
            downstream = dag.get_task(task_id).downstream_task_ids
            assert downstream == {expected_chain[index + 1]}
        assert dag.get_task(expected_chain[-1]).downstream_task_ids == set()


def test_cli_command_wiring_uses_explicit_config_and_env_file() -> None:
    dag_module = _load_dag_module()
    task_specs = list(dag_module.CLI_TASK_SPECS)

    assert [spec.cli_command for spec in task_specs] == list(COMMAND_ORDER)
    for spec in task_specs:
        assert spec.bash_command.startswith("cd ")
        assert "python cli.py" in spec.bash_command
        assert f"python cli.py {spec.cli_command}" in spec.bash_command
        assert "--config" in spec.bash_command
        assert "--env-file" in spec.bash_command
