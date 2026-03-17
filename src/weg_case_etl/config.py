from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


ENV_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)\}")
VALID_RUN_MODES = {"local", "cloud"}


class ConfigError(ValueError):
    """Raised when config is missing or invalid."""


@dataclass(frozen=True)
class PathConfig:
    source_dir: Path
    landing_dir: Path
    raw_dir: Path
    staging_dir: Path
    mart_dir: Path
    artifact_dir: Path


@dataclass(frozen=True)
class CloudConfig:
    bucket: str
    project_id: str
    dataset_raw: str
    dataset_staging: str
    dataset_mart: str


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    run_mode: str
    paths: PathConfig
    cloud: CloudConfig
    source_config_path: Path


def _expand_env_tokens(value: str) -> str:
    def _replacer(match: re.Match[str]) -> str:
        return os.getenv(match.group(1), "")

    return ENV_PATTERN.sub(_replacer, value).strip()


def _resolve_path(path_value: str | Path, base_dir: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _get_nested_map(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ConfigError(f"'{key}' must be a mapping in config file.")
    return value


def _resolve_text(
    env_name: str, raw_value: str | None, default: str = "", *, allow_empty: bool = True
) -> str:
    env_value = os.getenv(env_name)
    if env_value is not None:
        result = env_value.strip()
    elif raw_value is None:
        result = default
    else:
        result = _expand_env_tokens(str(raw_value))
    if not allow_empty and not result:
        raise ConfigError(f"Config value for '{env_name}' cannot be empty.")
    return result


def load_config(
    config_path: str | Path = "config/settings.yaml", env_path: str | Path = ".env"
) -> AppConfig:
    config_path = Path(config_path).resolve()
    env_path = Path(env_path).resolve()

    if env_path.exists():
        load_dotenv(env_path)

    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as stream:
        raw_config = yaml.safe_load(stream) or {}

    if not isinstance(raw_config, dict):
        raise ConfigError("Root config structure must be a mapping.")

    project_root_raw = os.getenv("WEG_PROJECT_ROOT", str(raw_config.get("project_root", ".")))
    project_root = _resolve_path(project_root_raw, Path.cwd())

    run_mode = os.getenv("WEG_RUN_MODE", str(raw_config.get("run_mode", "local"))).lower().strip()
    if run_mode not in VALID_RUN_MODES:
        raise ConfigError(f"Invalid run_mode '{run_mode}'. Valid values: {sorted(VALID_RUN_MODES)}")

    path_map = _get_nested_map(raw_config, "paths")
    paths = PathConfig(
        source_dir=_resolve_path(
            os.getenv("WEG_SOURCE_DIR", str(path_map.get("source_dir", "data/source"))), project_root
        ),
        landing_dir=_resolve_path(
            os.getenv("WEG_LANDING_DIR", str(path_map.get("landing_dir", "data/landing"))), project_root
        ),
        raw_dir=_resolve_path(
            os.getenv("WEG_RAW_DIR", str(path_map.get("raw_dir", "data/raw"))), project_root
        ),
        staging_dir=_resolve_path(
            os.getenv("WEG_STAGING_DIR", str(path_map.get("staging_dir", "data/staging"))), project_root
        ),
        mart_dir=_resolve_path(
            os.getenv("WEG_MART_DIR", str(path_map.get("mart_dir", "data/mart"))), project_root
        ),
        artifact_dir=_resolve_path(
            os.getenv("WEG_ARTIFACT_DIR", str(path_map.get("artifact_dir", "reports/artifacts"))),
            project_root,
        ),
    )

    cloud_map = _get_nested_map(raw_config, "cloud")
    cloud = CloudConfig(
        bucket=_resolve_text("GCS_BUCKET", cloud_map.get("bucket"), default=""),
        project_id=_resolve_text("GCP_PROJECT_ID", cloud_map.get("project_id"), default=""),
        dataset_raw=_resolve_text("GCP_DATASET_RAW", cloud_map.get("dataset_raw"), default="raw"),
        dataset_staging=_resolve_text(
            "GCP_DATASET_STAGING", cloud_map.get("dataset_staging"), default="staging"
        ),
        dataset_mart=_resolve_text("GCP_DATASET_MART", cloud_map.get("dataset_mart"), default="mart"),
    )

    if run_mode == "cloud":
        missing = []
        if not cloud.bucket:
            missing.append("GCS_BUCKET")
        if not cloud.project_id:
            missing.append("GCP_PROJECT_ID")
        if missing:
            raise ConfigError(
                "Cloud mode requires non-empty values for: " + ", ".join(sorted(missing))
            )

    return AppConfig(
        project_root=project_root,
        run_mode=run_mode,
        paths=paths,
        cloud=cloud,
        source_config_path=config_path,
    )
