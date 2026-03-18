from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any, Callable

import pyarrow.parquet as pq

from weg_case_etl.config import AppConfig
from weg_case_etl.contracts import COMMAND_ORDER, classify_source_file, required_source_paths


PLACEHOLDER_PATTERN = re.compile(r"\{\{[A-Z_]+\}\}")


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


def _staging_sql_dir(config: AppConfig) -> Path:
    return config.project_root / "sql" / "staging"


def _mart_sql_dir(config: AppConfig) -> Path:
    return config.project_root / "sql" / "mart"


def _validate_source_files(config: AppConfig) -> dict[str, Path]:
    source_map = required_source_paths(config.paths.source_dir)
    missing = [name for name, path in source_map.items() if not path.exists()]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise PipelineError(
            f"Required source files not found in '{config.paths.source_dir}': {missing_list}"
        )
    return source_map


def _require_bigquery_mode(config: AppConfig, command_name: str) -> None:
    if config.run_mode != "cloud":
        raise PipelineError(
            f"'{command_name}' requires BigQuery execution. Set run_mode to 'cloud' and provide GCP config."
        )

    missing = []
    if not config.cloud.project_id:
        missing.append("GCP_PROJECT_ID")
    if not config.cloud.dataset_raw:
        missing.append("GCP_DATASET_RAW")
    if not config.cloud.dataset_staging:
        missing.append("GCP_DATASET_STAGING")
    if not config.cloud.bigquery_location:
        missing.append("GCP_BQ_LOCATION")
    if command_name in {"transform", "dq"} and not config.cloud.dataset_mart:
        missing.append("GCP_DATASET_MART")
    if missing:
        raise PipelineError(
            "Missing required cloud configuration for BigQuery execution: " + ", ".join(sorted(missing))
        )


def _get_bigquery_client(config: AppConfig):
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - dependency path
        raise PipelineError(
            "BigQuery execution requires 'google-cloud-bigquery'. Install requirements first."
        ) from exc

    try:
        client = bigquery.Client(project=config.cloud.project_id)
    except Exception as exc:
        raise PipelineError(
            "BigQuery client initialization failed. Configure Google Cloud credentials (ADC) and permissions."
        ) from exc

    return bigquery, client


def _ensure_dataset(client, bigquery, dataset_fqn: str, location: str) -> None:
    dataset = bigquery.Dataset(dataset_fqn)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def _table_fqn(config: AppConfig, dataset_name: str, table_name: str) -> str:
    return f"{config.cloud.project_id}.{dataset_name}.{table_name}"


def _render_sql_template(template: str, values: dict[str, str]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)

    unresolved = PLACEHOLDER_PATTERN.findall(rendered)
    if unresolved:
        raise PipelineError(f"Unresolved SQL placeholders: {sorted(set(unresolved))}")
    return rendered


def _load_staging_sql_files(config: AppConfig) -> list[Path]:
    return _load_sql_files(_staging_sql_dir(config), "staging")


def _load_mart_sql_files(config: AppConfig) -> list[Path]:
    return _load_sql_files(_mart_sql_dir(config), "mart")


def _load_sql_files(sql_dir: Path, layer_name: str) -> list[Path]:
    if not sql_dir.exists():
        raise PipelineError(f"{layer_name.title()} SQL directory not found: {sql_dir}")

    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        raise PipelineError(f"No {layer_name} SQL files found under: {sql_dir}")

    return sql_files


def _get_table_row_count(client, table_fqn: str) -> int:
    table = client.get_table(table_fqn)
    return int(table.num_rows)


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
    _require_bigquery_mode(config, "load-raw")
    extract_manifest = _read_json(_extract_manifest_path(config))
    landed_files = extract_manifest.get("landed_files", [])
    if not landed_files:
        raise PipelineError("No landed files found in extract-upload manifest. Run 'extract-upload' first.")

    missing_gcs = [row.get("file_name", "<unknown>") for row in landed_files if not row.get("gcs_uri")]
    if missing_gcs:
        raise PipelineError(
            "Extract manifest is missing gcs_uri values for: "
            + ", ".join(sorted(missing_gcs))
            + ". Run 'extract-upload' in cloud mode before 'load-raw'."
        )

    _ensure_dir(config.paths.raw_dir)
    _ensure_dir(config.paths.artifact_dir)

    bigquery, client = _get_bigquery_client(config)
    raw_dataset_fqn = f"{config.cloud.project_id}.{config.cloud.dataset_raw}"
    _ensure_dataset(client, bigquery, raw_dataset_fqn, config.cloud.bigquery_location)

    load_jobs: list[dict[str, Any]] = []
    table_manifest: list[dict[str, Any]] = []
    for row in landed_files:
        dataset_name = row["dataset"]
        target_table = _table_fqn(config, config.cloud.dataset_raw, dataset_name)
        gcs_uri = row["gcs_uri"]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(
            gcs_uri,
            target_table,
            job_config=job_config,
            location=config.cloud.bigquery_location,
        )
        load_job.result()
        loaded_rows = _get_table_row_count(client, target_table)

        load_jobs.append(
            {
                "file_name": row["file_name"],
                "dataset": dataset_name,
                "job_id": load_job.job_id,
                "source_uri": gcs_uri,
                "target_table": target_table,
                "loaded_rows": loaded_rows,
            }
        )
        table_manifest.append(
            {
                "source_file": row["file_name"],
                "dataset": dataset_name,
                "domain": row["domain"],
                "gcs_uri": gcs_uri,
                "target_table": target_table,
                "status": "loaded",
                "loaded_rows": loaded_rows,
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
        "project_id": config.cloud.project_id,
        "dataset_raw": config.cloud.dataset_raw,
        "location": config.cloud.bigquery_location,
        "load_jobs": load_jobs,
    }
    report_path = _write_json(config.paths.artifact_dir / "load_raw_report.json", report)

    return {
        "command": "load-raw",
        "report_path": str(report_path),
        "table_count": len(table_manifest),
    }


def transform(config: AppConfig) -> dict[str, Any]:
    _require_bigquery_mode(config, "transform")
    raw_manifest_path = config.paths.raw_dir / "raw_table_manifest.json"
    raw_manifest = _read_json(raw_manifest_path)
    raw_tables = raw_manifest.get("tables", [])
    if not raw_tables:
        raise PipelineError("Raw table manifest is empty. Run 'load-raw' first.")

    _ensure_dir(config.paths.artifact_dir)

    bigquery, client = _get_bigquery_client(config)
    staging_dataset_fqn = f"{config.cloud.project_id}.{config.cloud.dataset_staging}"
    mart_dataset_fqn = f"{config.cloud.project_id}.{config.cloud.dataset_mart}"
    _ensure_dataset(client, bigquery, staging_dataset_fqn, config.cloud.bigquery_location)
    _ensure_dataset(client, bigquery, mart_dataset_fqn, config.cloud.bigquery_location)

    staging_sql_files = _load_staging_sql_files(config)
    mart_sql_files = _load_mart_sql_files(config)
    template_values = {
        "PROJECT_ID": config.cloud.project_id,
        "RAW_DATASET": config.cloud.dataset_raw,
        "STAGING_DATASET": config.cloud.dataset_staging,
        "MART_DATASET": config.cloud.dataset_mart,
    }

    staging_steps = _execute_sql_files(
        client=client,
        config=config,
        sql_files=staging_sql_files,
        template_values=template_values,
        layer="staging",
    )
    mart_steps = _execute_sql_files(
        client=client,
        config=config,
        sql_files=mart_sql_files,
        template_values=template_values,
        layer="mart",
    )

    staging_output_table_names = [
        "provider_clean",
        "airport_reference_clean",
        "booking_clean",
        "booking_reject",
        "search_clean",
        "search_reject",
    ]
    mart_output_table_names = ["booking_enriched"]

    output_tables = []
    for table_name in staging_output_table_names:
        fqn = _table_fqn(config, config.cloud.dataset_staging, table_name)
        output_tables.append(
            {
                "layer": "staging",
                "table": fqn,
                "row_count": _get_table_row_count(client, fqn),
            }
        )
    for table_name in mart_output_table_names:
        fqn = _table_fqn(config, config.cloud.dataset_mart, table_name)
        output_tables.append(
            {
                "layer": "mart",
                "table": fqn,
                "row_count": _get_table_row_count(client, fqn),
            }
        )

    report = {
        "command": "transform",
        "timestamp_utc": _utc_now(),
        "run_mode": config.run_mode,
        "project_id": config.cloud.project_id,
        "dataset_staging": config.cloud.dataset_staging,
        "dataset_mart": config.cloud.dataset_mart,
        "location": config.cloud.bigquery_location,
        "executed_steps": [*staging_steps, *mart_steps],
        "output_tables": output_tables,
    }
    report_path = _write_json(config.paths.artifact_dir / "transform_report.json", report)

    return {
        "command": "transform",
        "report_path": str(report_path),
        "output_table_count": len(output_tables),
    }


def _execute_sql_files(
    client,
    config: AppConfig,
    sql_files: list[Path],
    template_values: dict[str, str],
    layer: str,
) -> list[dict[str, Any]]:
    executed_steps: list[dict[str, Any]] = []
    for sql_file in sql_files:
        template = sql_file.read_text(encoding="utf-8")
        rendered_sql = _render_sql_template(template, template_values)
        query_job = client.query(rendered_sql, location=config.cloud.bigquery_location)
        query_job.result()
        executed_steps.append(
            {
                "layer": layer,
                "sql_file": str(sql_file.relative_to(config.project_root)).replace("\\", "/"),
                "job_id": query_job.job_id,
                "statement_type": query_job.statement_type or "",
            }
        )
    return executed_steps


def _query_single_metric(client, sql: str, location: str) -> int:
    query_job = client.query(sql, location=location)
    rows = list(query_job.result())
    if not rows:
        raise PipelineError("DQ query returned no rows.")
    row = rows[0]
    metric_value: Any = None
    if isinstance(row, dict):
        metric_value = row.get("metric_value")
    if metric_value is None:
        try:
            metric_value = row["metric_value"]
        except Exception:
            metric_value = row[0]
    return int(metric_value or 0)


def _evaluate_mandatory_business_rule_checks(config: AppConfig, client) -> list[dict[str, Any]]:
    booking_clean = _table_fqn(config, config.cloud.dataset_staging, "booking_clean")
    search_clean = _table_fqn(config, config.cloud.dataset_staging, "search_clean")
    booking_enriched = _table_fqn(config, config.cloud.dataset_mart, "booking_enriched")

    check_specs = [
        {
            "name": "booking_origin_destination_not_equal",
            "message": "booking_clean must not contain origin = destination.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{booking_clean}` WHERE origin = destination",
        },
        {
            "name": "search_origin_destination_not_equal",
            "message": "search_clean must not contain origin = destination.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{search_clean}` WHERE origin = destination",
        },
        {
            "name": "booking_direction_consistency",
            "message": "booking_clean direction_type must match return_date nullability.",
            "sql": (
                "SELECT COUNT(1) AS metric_value "
                f"FROM `{booking_clean}` "
                "WHERE (return_date IS NULL AND direction_type != 'oneway') "
                "OR (return_date IS NOT NULL AND direction_type != 'roundtrip')"
            ),
        },
        {
            "name": "search_direction_consistency",
            "message": "search_clean direction_type must match return_date nullability.",
            "sql": (
                "SELECT COUNT(1) AS metric_value "
                f"FROM `{search_clean}` "
                "WHERE (return_date IS NULL AND direction_type != 'oneway') "
                "OR (return_date IS NOT NULL AND direction_type != 'roundtrip')"
            ),
        },
        {
            "name": "booking_created_at_timestamp_compatibility",
            "message": "booking_clean created_at must be populated as TIMESTAMP.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{booking_clean}` WHERE created_at IS NULL",
        },
        {
            "name": "search_created_at_timestamp_compatibility",
            "message": "search_clean created_at must be populated as TIMESTAMP.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{search_clean}` WHERE created_at IS NULL",
        },
        {
            "name": "booking_negative_total_absent",
            "message": "booking_clean must not contain negative total.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{booking_clean}` WHERE total < 0",
        },
        {
            "name": "search_negative_price_absent",
            "message": "search_clean must not contain negative cheapest_price.",
            "sql": f"SELECT COUNT(1) AS metric_value FROM `{search_clean}` WHERE cheapest_price < 0",
        },
        {
            "name": "booking_clean_duplicate_grain_zero",
            "message": "booking_clean must be unique on booking_id.",
            "sql": (
                "SELECT COUNT(1) AS metric_value FROM ("
                f"SELECT booking_id FROM `{booking_clean}` GROUP BY booking_id HAVING COUNT(1) > 1)"
            ),
        },
        {
            "name": "search_clean_duplicate_grain_zero",
            "message": "search_clean must be unique on request_id.",
            "sql": (
                "SELECT COUNT(1) AS metric_value FROM ("
                f"SELECT request_id FROM `{search_clean}` GROUP BY request_id HAVING COUNT(1) > 1)"
            ),
        },
        {
            "name": "mart_booking_enriched_duplicate_grain_zero",
            "message": "mart.booking_enriched must be unique on booking_id.",
            "sql": (
                "SELECT COUNT(1) AS metric_value FROM ("
                f"SELECT booking_id FROM `{booking_enriched}` GROUP BY booking_id HAVING COUNT(1) > 1)"
            ),
        },
    ]

    checks: list[dict[str, Any]] = []
    for spec in check_specs:
        try:
            metric_value = _query_single_metric(client, spec["sql"], config.cloud.bigquery_location)
            status = "pass" if metric_value == 0 else "fail"
            checks.append(
                {
                    "name": spec["name"],
                    "status": status,
                    "message": spec["message"],
                    "metric_value": metric_value,
                    "expected_value": 0,
                }
            )
        except Exception as exc:
            checks.append(
                {
                    "name": spec["name"],
                    "status": "fail",
                    "message": f"{spec['message']} Query execution failed.",
                    "metric_value": None,
                    "expected_value": 0,
                    "error": str(exc),
                }
            )
    return checks


def dq(config: AppConfig) -> dict[str, Any]:
    _validate_source_files(config)
    _ensure_dir(config.paths.artifact_dir)
    extract_manifest_path = _extract_manifest_path(config)
    load_raw_report_path = config.paths.artifact_dir / "load_raw_report.json"
    transform_report_path = config.paths.artifact_dir / "transform_report.json"

    checks = [
        {
            "name": "extract_manifest_exists",
            "status": "pass" if extract_manifest_path.exists() else "fail",
            "message": "extract_upload_manifest.json must exist.",
            "metric_value": None,
            "expected_value": None,
        },
    ]

    if config.run_mode == "cloud":
        _require_bigquery_mode(config, "dq")
        checks.extend(
            [
                {
                    "name": "load_raw_report_exists",
                    "status": "pass" if load_raw_report_path.exists() else "fail",
                    "message": "load_raw_report.json must exist for BigQuery phase.",
                    "metric_value": None,
                    "expected_value": None,
                },
                {
                    "name": "transform_report_exists",
                    "status": "pass" if transform_report_path.exists() else "fail",
                    "message": "transform_report.json must exist for BigQuery phase.",
                    "metric_value": None,
                    "expected_value": None,
                },
            ]
        )

        if all(check["status"] == "pass" for check in checks):
            _, client = _get_bigquery_client(config)
            checks.extend(_evaluate_mandatory_business_rule_checks(config, client))
        else:
            checks.append(
                {
                    "name": "mandatory_business_rules",
                    "status": "fail",
                    "message": "Mandatory business rule checks skipped because prerequisites are missing.",
                    "metric_value": None,
                    "expected_value": 0,
                }
            )
    else:
        checks.extend(
            [
                {
                    "name": "load_raw_report_exists",
                    "status": "pending",
                    "message": "BigQuery checks are pending in local mode.",
                    "metric_value": None,
                    "expected_value": None,
                },
                {
                    "name": "transform_report_exists",
                    "status": "pending",
                    "message": "BigQuery checks are pending in local mode.",
                    "metric_value": None,
                    "expected_value": None,
                },
                {
                    "name": "mandatory_business_rules",
                    "status": "pending",
                    "message": "Mandatory business rule checks run only in cloud mode (BigQuery).",
                    "metric_value": None,
                    "expected_value": 0,
                },
            ]
        )

    if config.run_mode == "cloud":
        status = "pass" if all(check["status"] == "pass" for check in checks) else "fail"
    else:
        status = "pass" if all(check["status"] in {"pass", "pending"} for check in checks) else "fail"

    report = {
        "command": "dq",
        "timestamp_utc": _utc_now(),
        "overall_status": status,
        "checks": checks,
    }
    report_path = _write_json(config.paths.artifact_dir / "dq_report.json", report)

    result = {
        "command": "dq",
        "overall_status": status,
        "report_path": str(report_path),
    }

    if status == "fail":
        failed_check_names = [check["name"] for check in checks if check["status"] == "fail"]
        raise PipelineError(
            "DQ checks failed: " + ", ".join(failed_check_names) + f". See report: {report_path}"
        )

    return result


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
