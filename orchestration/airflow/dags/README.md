# Optional Airflow DAG Contract

Phase 8 implements optional Airflow orchestration over the existing CLI command contract.

## DAG Artifact

- `weg_case_optional_etl_dag.py`
- DAG id: `weg_case_optional_etl`

## Runtime Behavior

- Manual trigger only (`schedule=None`, `catchup=False`).
- Deterministic task order:
  - `check_enable_airflow_flag`
  - `run_profile`
  - `run_extract_upload`
  - `run_load_raw`
  - `run_transform`
  - `run_dq`
- Guard task fails fast when `features.enable_airflow=false` or `WEG_ENABLE_AIRFLOW=false`.

## Command Wiring

Each CLI task runs from repository root and passes explicit runtime files:

- `python cli.py <command> --config <path> --env-file <path>`

Optional environment overrides for DAG runtime paths:

- `WEG_AIRFLOW_CONFIG_PATH` (default: `config/settings.yaml`)
- `WEG_AIRFLOW_ENV_FILE` (default: `.env`)

## Dependency Note

Airflow remains optional for this repository. The DAG module is import-safe when Airflow is not installed, while core tests and CLI workflows remain unchanged.
