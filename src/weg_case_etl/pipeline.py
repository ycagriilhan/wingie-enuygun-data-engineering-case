from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any, Callable

import pyarrow.parquet as pq

from weg_case_etl.config import AppConfig
from weg_case_etl.contracts import COMMAND_ORDER, classify_source_file, required_source_paths


class PipelineError(RuntimeError):
    """Raised when a pipeline command cannot proceed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PipelineError(f"Required manifest not found: {path}")
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    if not isinstance(payload, dict):
        raise PipelineError(f"Invalid JSON payload in manifest: {path}")
    return payload


def _extract_manifest_path(config: AppConfig) -> Path:
    return config.paths.artifact_dir / "extract_upload_manifest.json"


def _validate_source_files(config: AppConfig) -> dict[str, Path]:
    source_map = required_source_paths(config.paths.source_dir)
    missing = [name for name, path in source_map.items() if not path.exists()]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise PipelineError(
            f"Required source files not found in '{config.paths.source_dir}': {missing_list}"
        )
    return source_map


def _classify_and_land_files(config: AppConfig, run_identifier: str) -> list[dict[str, Any]]:
    source_map = _validate_source_files(config)
    _ensure_dir(config.paths.landing_dir)

    landed_records: list[dict[str, Any]] = []
    for file_name, source_path in source_map.items():
        classification = classify_source_file(file_name)
        dataset = classification["dataset"]
        domain = classification["domain"]

        dataset_landing_dir = config.paths.landing_dir / run_identifier / dataset
        _ensure_dir(dataset_landing_dir)
        landing_path = dataset_landing_dir / file_name
        copy2(source_path, landing_path)

        parquet_file = pq.ParquetFile(source_path)
        landed_records.append(
            {
                "file_name": file_name,
                "dataset": dataset,
                "domain": domain,
                "source_path": str(source_path),
                "landing_path": str(landing_path),
                "landing_relative_path": str(landing_path.relative_to(config.paths.landing_dir)).replace(
                    "\\", "/"
                ),
                "row_count": parquet_file.metadata.num_rows,
                "column_count": parquet_file.metadata.num_columns,
                "columns": parquet_file.schema.names,
                "file_size_bytes": source_path.stat().st_size,
            }
        )

    return sorted(landed_records, key=lambda item: item["file_name"])


def _upload_to_gcs(config: AppConfig, run_identifier: str, landed_files: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        from google.cloud import storage
    except ImportError as exc:  # pragma: no cover - cloud path
        raise PipelineError(
            "Cloud mode requires 'google-cloud-storage'. Install it before running extract-upload."
        ) from exc

    prefix = (config.cloud.landing_prefix or "").strip("/")

    try:
        client = storage.Client(project=config.cloud.project_id)
        bucket = client.bucket(config.cloud.bucket)

        uploaded_objects: list[dict[str, str]] = []
        for item in landed_files:
            local_path = Path(item["landing_path"])
            object_parts = [part for part in [prefix, run_identifier, item["dataset"], item["file_name"]] if part]
            object_name = "/".join(object_parts)
            blob = bucket.blob(object_name)
            blob.upload_from_filename(str(local_path))
            uploaded_objects.append(
                {
                    "file_name": item["file_name"],
                    "object_name": object_name,
                    "gcs_uri": f"gs://{config.cloud.bucket}/{object_name}",
                }
            )
    except Exception as exc:  # pragma: no cover - cloud path
        raise PipelineError(
            f"GCS upload failed for bucket '{config.cloud.bucket}'. Check credentials and permissions."
        ) from exc

    return {
        "status": "uploaded",
        "bucket": config.cloud.bucket,
        "prefix": prefix,
        "uploaded_objects": uploaded_objects,
    }


def profile(config: AppConfig) -> dict[str, Any]:
    source_map = _validate_source_files(config)
    _ensure_dir(config.paths.artifact_dir)

    profile_rows = []
    for file_name, file_path in source_map.items():
        parquet_file = pq.ParquetFile(file_path)
        classification = classify_source_file(file_name)
        profile_rows.append(
            {
                "file_name": file_name,
                "dataset": classification["dataset"],
                "domain": classification["domain"],
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
    _ensure_dir(config.paths.artifact_dir)
    run_identifier = _run_id()

    landed_files = _classify_and_land_files(config, run_identifier)
    cloud_upload: dict[str, Any]
    if config.run_mode == "cloud":
        cloud_upload = _upload_to_gcs(config, run_identifier, landed_files)
        upload_index = {row["file_name"]: row["gcs_uri"] for row in cloud_upload["uploaded_objects"]}
        for item in landed_files:
            item["gcs_uri"] = upload_index.get(item["file_name"], "")
    else:
        cloud_upload = {"status": "not_applicable_in_local_mode"}

    manifest = {
        "command": "extract-upload",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "run_id": run_identifier,
        "source_dir": str(config.paths.source_dir),
        "landing_root": str(config.paths.landing_dir),
        "landed_files": landed_files,
        "cloud_upload": cloud_upload,
    }
    manifest_path = _write_json(_extract_manifest_path(config), manifest)

    return {
        "command": "extract-upload",
        "manifest_path": str(manifest_path),
        "run_id": run_identifier,
        "landed_file_count": len(landed_files),
        "cloud_status": cloud_upload["status"],
    }


def load_raw(config: AppConfig) -> dict[str, Any]:
    extract_manifest = _read_json(_extract_manifest_path(config))
    landed_files = extract_manifest.get("landed_files", [])
    if not landed_files:
        raise PipelineError("No landed files found in extract-upload manifest. Run 'extract-upload' first.")

    missing_landed = [row["landing_path"] for row in landed_files if not Path(row["landing_path"]).exists()]
    if missing_landed:
        raise PipelineError(
            "Landing files are missing. Re-run 'extract-upload'. Missing paths: "
            + ", ".join(missing_landed)
        )

    _ensure_dir(config.paths.raw_dir)
    _ensure_dir(config.paths.artifact_dir)

    table_manifest = []
    for row in landed_files:
        table_manifest.append(
            {
                "source_file": row["file_name"],
                "dataset": row["dataset"],
                "domain": row["domain"],
                "landing_path": row["landing_path"],
                "gcs_uri": row.get("gcs_uri", ""),
                "target_table": f"raw.{row['dataset']}",
                "status": "ready_for_phase_3_raw_load",
            }
        )

    _write_json(
        config.paths.raw_dir / "raw_table_manifest.json",
        {"run_id": extract_manifest.get("run_id"), "tables": table_manifest},
    )

    report = {
        "command": "load-raw",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "run_id": extract_manifest.get("run_id"),
        "tables": table_manifest,
        "bigquery_load": (
            {
                "status": "pending_phase_3_implementation",
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
    extract_manifest_path = _extract_manifest_path(config)
    extract_manifest_exists = extract_manifest_path.exists()
    landing_structure_status = "fail"

    if extract_manifest_exists:
        payload = _read_json(extract_manifest_path)
        run_identifier = payload.get("run_id", "")
        landing_files = payload.get("landed_files", [])
        if run_identifier and landing_files:
            landing_structure_status = (
                "pass"
                if all(f"/{run_identifier}/" in row.get("landing_relative_path", "") for row in landing_files)
                else "fail"
            )

    checks = [
        {
            "name": "extract_manifest_exists",
            "status": "pass" if extract_manifest_exists else "fail",
            "message": "extract_upload_manifest.json must exist.",
        },
        {
            "name": "classified_landing_layout",
            "status": landing_structure_status,
            "message": "Landed files should include a run_id and dataset folder in landing path.",
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
