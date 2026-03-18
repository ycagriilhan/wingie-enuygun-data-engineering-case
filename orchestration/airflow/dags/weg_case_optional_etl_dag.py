from __future__ import annotations

import os
import shlex
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from weg_case_etl.config import ConfigError, load_config
from weg_case_etl.contracts import COMMAND_ORDER


DAG_ID = "weg_case_optional_etl"
AIRFLOW_FLAG_GUARD_TASK_ID = "check_enable_airflow_flag"
AIRFLOW_CONFIG_PATH_ENV = "WEG_AIRFLOW_CONFIG_PATH"
AIRFLOW_ENV_FILE_ENV = "WEG_AIRFLOW_ENV_FILE"

DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "settings.yaml"
DEFAULT_ENV_FILE = REPO_ROOT / ".env"
EXPECTED_COMMAND_ORDER = ("profile", "extract-upload", "load-raw", "transform", "dq")

if tuple(COMMAND_ORDER) != EXPECTED_COMMAND_ORDER:
    raise RuntimeError(
        "Airflow DAG command order must stay aligned with pipeline contract. "
        f"Expected {EXPECTED_COMMAND_ORDER}, got {tuple(COMMAND_ORDER)}."
    )


@dataclass(frozen=True)
class CliTaskSpec:
    task_id: str
    cli_command: str
    bash_command: str


def _resolve_runtime_path(env_name: str, default_path: Path) -> Path:
    value = os.getenv(env_name, "").strip()
    if not value:
        return default_path.resolve()
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def resolve_dag_runtime_paths() -> tuple[Path, Path]:
    config_path = _resolve_runtime_path(AIRFLOW_CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH)
    env_path = _resolve_runtime_path(AIRFLOW_ENV_FILE_ENV, DEFAULT_ENV_FILE)
    return config_path, env_path


def _build_cli_bash_command(cli_command: str, config_path: Path, env_file: Path) -> str:
    repo_root = shlex.quote(str(REPO_ROOT))
    config_quoted = shlex.quote(str(config_path))
    env_quoted = shlex.quote(str(env_file))
    return (
        f"cd {repo_root} && "
        f"python cli.py {cli_command} --config {config_quoted} --env-file {env_quoted}"
    )


def build_cli_task_specs(config_path: Path | None = None, env_file: Path | None = None) -> list[CliTaskSpec]:
    resolved_config, resolved_env_file = resolve_dag_runtime_paths()
    if config_path is not None:
        resolved_config = Path(config_path).resolve()
    if env_file is not None:
        resolved_env_file = Path(env_file).resolve()

    task_specs: list[CliTaskSpec] = []
    for command_name in COMMAND_ORDER:
        task_specs.append(
            CliTaskSpec(
                task_id=f"run_{command_name.replace('-', '_')}",
                cli_command=command_name,
                bash_command=_build_cli_bash_command(command_name, resolved_config, resolved_env_file),
            )
        )
    return task_specs


def validate_airflow_feature_flag(
    config_path: str | Path | None = None,
    env_file: str | Path | None = None,
) -> dict[str, Any]:
    resolved_config, resolved_env_file = resolve_dag_runtime_paths()
    if config_path is not None:
        resolved_config = Path(config_path).resolve()
    if env_file is not None:
        resolved_env_file = Path(env_file).resolve()

    try:
        config = load_config(config_path=resolved_config, env_path=resolved_env_file)
    except (ConfigError, FileNotFoundError) as exc:
        raise RuntimeError(
            "Airflow preflight failed while loading config/env for optional DAG execution."
        ) from exc

    if not config.features.enable_airflow:
        raise RuntimeError(
            "Optional Airflow DAG is disabled. Set features.enable_airflow=true "
            "or WEG_ENABLE_AIRFLOW=true before triggering this DAG."
        )

    return {
        "status": "enabled",
        "config_path": str(resolved_config),
        "env_file": str(resolved_env_file),
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }


CLI_TASK_SPECS = tuple(build_cli_task_specs())
CLI_COMMAND_ORDER = tuple(spec.cli_command for spec in CLI_TASK_SPECS)
CLI_TASK_IDS = tuple(spec.task_id for spec in CLI_TASK_SPECS)
LINEAR_TASK_CHAIN = (AIRFLOW_FLAG_GUARD_TASK_ID, *CLI_TASK_IDS)

AIRFLOW_AVAILABLE = False
AIRFLOW_IMPORT_ERROR: str | None = None
dag = None

try:  # pragma: no cover - optional dependency path
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    AIRFLOW_AVAILABLE = True

    with DAG(
        dag_id=DAG_ID,
        description="Optional WEG ETL DAG that wraps the existing CLI command contract.",
        schedule=None,
        catchup=False,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        tags=["weg-case", "phase-08", "optional"],
    ) as dag:
        guard_task = PythonOperator(
            task_id=AIRFLOW_FLAG_GUARD_TASK_ID,
            python_callable=validate_airflow_feature_flag,
        )

        previous_task = guard_task
        for spec in CLI_TASK_SPECS:
            command_task = BashOperator(
                task_id=spec.task_id,
                bash_command=spec.bash_command,
            )
            previous_task >> command_task
            previous_task = command_task
except ImportError as exc:  # pragma: no cover - optional dependency path
    AIRFLOW_IMPORT_ERROR = str(exc)

