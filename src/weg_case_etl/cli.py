from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, Callable

import typer

from weg_case_etl.config import ConfigError, load_config
from weg_case_etl.pipeline import PipelineError, dq, extract_upload, load_raw, profile, run_all, transform


app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Wingie/Enuygun data engineering case CLI (Phase 5 tests + validation evidence baseline).",
)


def _execute(
    command_name: str,
    handler: Callable[..., dict[str, Any]],
    config_path: Path,
    env_file: Path,
) -> None:
    try:
        config = load_config(config_path=config_path, env_path=env_file)
        result = handler(config)
    except (ConfigError, PipelineError, FileNotFoundError) as exc:
        typer.secho(f"[{command_name}] failed: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # pragma: no cover - defensive guard
        typer.secho(f"[{command_name}] unexpected error: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.secho(f"[{command_name}] completed", fg=typer.colors.GREEN)
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


def _default_config() -> Path:
    return Path("config/settings.yaml")


def _default_env() -> Path:
    return Path(".env")


@app.command("profile")
def profile_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("profile", profile, config, env_file)


@app.command("extract-upload")
def extract_upload_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("extract-upload", extract_upload, config, env_file)


@app.command("load-raw")
def load_raw_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("load-raw", load_raw, config, env_file)


@app.command("transform")
def transform_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("transform", transform, config, env_file)


@app.command("dq")
def dq_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("dq", dq, config, env_file)


@app.command("run-all")
def run_all_command(
    config: Annotated[
        Path, typer.Option("--config", help="Path to YAML config.")
    ] = _default_config(),
    env_file: Annotated[
        Path, typer.Option("--env-file", help="Path to .env file.")
    ] = _default_env(),
) -> None:
    _execute("run-all", run_all, config, env_file)
