from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any, Callable

import pyarrow.parquet as pq

from weg_case_etl.config import AppConfig
from weg_case_etl.contracts import COMMAND_ORDER, required_source_paths


class PipelineError(RuntimeError):
    """Raised when a pipeline command cannot proceed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
    return path


def _validate_source_files(config: AppConfig) -> dict[str, Path]:
    source_map = required_source_paths(config.paths.source_dir)
    missing = [name for name, path in source_map.items() if not path.exists()]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise PipelineError(
            f"Required source files not found in '{config.paths.source_dir}': {missing_list}"
        )
    return source_map


def profile(config: AppConfig) -> dict[str, Any]:
    source_map = _validate_source_files(config)
    _ensure_dir(config.paths.artifact_dir)

    profile_rows = []
    for file_name, file_path in source_map.items():
        parquet_file = pq.ParquetFile(file_path)
        profile_rows.append(
            {
                "file_name": file_name,
                "path": str(file_path),
                "row_count": parquet_file.metadata.num_rows,
                "column_count": parquet_file.metadata.num_columns,
                "columns": parquet_file.schema.names,
            }
        )

    report = {
        "command": "profile",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "source_dir": str(config.paths.source_dir),
        "file_profiles": profile_rows,
    }
    report_path = _write_json(config.paths.artifact_dir / "profile_summary.json", report)

    return {
        "command": "profile",
        "report_path": str(report_path),
        "files_profiled": len(profile_rows),
    }


def extract_upload(config: AppConfig) -> dict[str, Any]:
    source_map = _validate_source_files(config)
    _ensure_dir(config.paths.landing_dir)
    _ensure_dir(config.paths.artifact_dir)

    landed = []
    for file_name, source_path in source_map.items():
        destination = config.paths.landing_dir / file_name
        copy2(source_path, destination)
        landed.append({"file_name": file_name, "source": str(source_path), "landing": str(destination)})

    manifest = {
        "command": "extract-upload",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "landed_files": landed,
        "cloud_upload": (
            {
                "status": "skipped_in_phase_1",
                "bucket": config.cloud.bucket,
            }
            if config.run_mode == "cloud"
            else {"status": "not_applicable_in_local_mode"}
        ),
    }
    manifest_path = _write_json(config.paths.artifact_dir / "extract_upload_manifest.json", manifest)

    return {
        "command": "extract-upload",
        "manifest_path": str(manifest_path),
        "landed_file_count": len(landed),
    }


def load_raw(config: AppConfig) -> dict[str, Any]:
    landing_map = required_source_paths(config.paths.landing_dir)
    missing = [name for name, path in landing_map.items() if not path.exists()]
    if missing:
        raise PipelineError(
            "Landing files are missing. Run 'extract-upload' first. Missing: "
            + ", ".join(sorted(missing))
        )

    _ensure_dir(config.paths.raw_dir)
    _ensure_dir(config.paths.artifact_dir)

    table_manifest = [
        {
            "source_file": file_name,
            "landing_path": str(file_path),
            "target_table": f"raw.{Path(file_name).stem}",
            "status": "stubbed_phase_1",
        }
        for file_name, file_path in landing_map.items()
    ]
    _write_json(config.paths.raw_dir / "raw_table_manifest.json", {"tables": table_manifest})

    report = {
        "command": "load-raw",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "tables": table_manifest,
        "bigquery_load": (
            {
                "status": "skipped_in_phase_1",
                "project_id": config.cloud.project_id,
                "dataset": config.cloud.dataset_raw,
            }
            if config.run_mode == "cloud"
            else {"status": "not_applicable_in_local_mode"}
        ),
    }
    report_path = _write_json(config.paths.artifact_dir / "load_raw_report.json", report)

    return {
        "command": "load-raw",
        "report_path": str(report_path),
        "table_count": len(table_manifest),
    }


def transform(config: AppConfig) -> dict[str, Any]:
    raw_manifest_path = config.paths.raw_dir / "raw_table_manifest.json"
    if not raw_manifest_path.exists():
        raise PipelineError(
            f"Raw manifest not found at '{raw_manifest_path}'. Run 'load-raw' first."
        )

    _ensure_dir(config.paths.staging_dir)
    _ensure_dir(config.paths.mart_dir)
    _ensure_dir(config.paths.artifact_dir)

    staging_placeholder = config.paths.staging_dir / "phase1_staging_placeholder.json"
    mart_placeholder = config.paths.mart_dir / "booking_enriched_placeholder.json"
    _write_json(
        staging_placeholder,
        {
            "layer": "staging",
            "status": "stubbed_phase_1",
            "generated_at_utc": _utc_now(),
            "note": "Business transformations start in later phases.",
        },
    )
    _write_json(
        mart_placeholder,
        {
            "layer": "mart",
            "table": "booking_enriched",
            "status": "stubbed_phase_1",
            "generated_at_utc": _utc_now(),
            "note": "Business mart logic starts in later phases.",
        },
    )

    report = {
        "command": "transform",
        "timestamp_utc": _utc_now(),
        "staging_output": str(staging_placeholder),
        "mart_output": str(mart_placeholder),
    }
    report_path = _write_json(config.paths.artifact_dir / "transform_report.json", report)

    return {
        "command": "transform",
        "report_path": str(report_path),
        "outputs": [str(staging_placeholder), str(mart_placeholder)],
    }


def dq(config: AppConfig) -> dict[str, Any]:
    _validate_source_files(config)

    checks = [
        {
            "name": "landing_manifest_exists",
            "status": "pass"
            if (config.paths.artifact_dir / "extract_upload_manifest.json").exists()
            else "fail",
            "message": "extract_upload_manifest.json must exist.",
        },
        {
            "name": "raw_manifest_exists",
            "status": "pass"
            if (config.paths.raw_dir / "raw_table_manifest.json").exists()
            else "fail",
            "message": "raw_table_manifest.json must exist.",
        },
        {
            "name": "transform_output_exists",
            "status": "pass"
            if (config.paths.mart_dir / "booking_enriched_placeholder.json").exists()
            else "fail",
            "message": "booking_enriched_placeholder.json must exist.",
        },
        {
            "name": "mandatory_business_rules",
            "status": "pending",
            "message": "Rule-level DQ implementation is planned for later phases.",
        },
    ]

    status = "pass" if all(check["status"] in {"pass", "pending"} for check in checks) else "fail"
    report = {
        "command": "dq",
        "timestamp_utc": _utc_now(),
        "overall_status": status,
        "checks": checks,
    }
    report_path = _write_json(config.paths.artifact_dir / "dq_report.json", report)

    return {
        "command": "dq",
        "overall_status": status,
        "report_path": str(report_path),
    }


def run_all(config: AppConfig) -> dict[str, Any]:
    _ensure_dir(config.paths.artifact_dir)

    steps: list[tuple[str, Callable[[AppConfig], dict[str, Any]]]] = [
        ("profile", profile),
        ("extract-upload", extract_upload),
        ("load-raw", load_raw),
        ("transform", transform),
        ("dq", dq),
    ]

    step_results: list[dict[str, Any]] = []
    for step_name, step_function in steps:
        result = step_function(config)
        step_results.append({"step": step_name, "result": result})

    report = {
        "command": "run-all",
        "timestamp_utc": _utc_now(),
        "expected_order": list(COMMAND_ORDER),
        "executed_order": [item["step"] for item in step_results],
        "results": step_results,
    }
    report_path = _write_json(config.paths.artifact_dir / "run_all_report.json", report)

    return {
        "command": "run-all",
        "report_path": str(report_path),
        "executed_steps": [item["step"] for item in step_results],
    }
