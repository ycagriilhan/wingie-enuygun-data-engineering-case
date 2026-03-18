# Phase 06 - Optional Foundation

## 1. Objective

Reserve optional extension interfaces for MERGE/Upsert and Airflow without changing core Phase 1-5 behavior or command contracts.

## 2. What Was Implemented

- Added optional feature flags in config:
  - `features.enable_merge_upsert` (`WEG_ENABLE_MERGE_UPSERT`)
  - `features.enable_airflow` (`WEG_ENABLE_AIRFLOW`)
- Added optional path configuration in config:
  - `optional_paths.sql_optional_dir` (`WEG_SQL_OPTIONAL_DIR`)
  - `optional_paths.airflow_dags_dir` (`WEG_AIRFLOW_DAGS_DIR`)
- Implemented strict boolean parsing and env override support for optional flags.
- Reserved optional contract directories and README contracts:
  - `sql/optional/`
  - `orchestration/airflow/dags/`
- Kept core CLI command set unchanged (`profile`, `extract-upload`, `load-raw`, `transform`, `dq`, `run-all`).

## 3. Commands Run

```powershell
pytest -q
python cli.py profile --env-file .env.example
```

## 4. Key Files Created/Changed

- `src/weg_case_etl/config.py`
- `config/settings.yaml`
- `.env.example`
- `tests/test_config.py`
- `sql/optional/README.md`
- `orchestration/airflow/dags/README.md`
- `README.md`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- Optional defaults remain disabled (`false`) unless explicitly enabled by env/config.
- Config tests validate:
  - disabled defaults
  - env overrides
  - invalid boolean rejection
- Core command contract remains stable and runnable after optional scaffolding.

## 6. Risks / Assumptions

- Phase 6 is intentionally non-executing for optional features; runtime optional behavior starts in later phases.
- Cloud commands still depend on valid GCP credentials and permissions.

## 7. What Comes Next

Implement executable optional behavior:
- Phase 7: MERGE/Upsert SQL path and transform selection.
- Phase 8: Airflow DAG orchestration over existing CLI contracts.
