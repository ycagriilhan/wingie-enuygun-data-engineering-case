# Phase 08 - Optional Airflow DAG

## 1. Objective

Implement optional Airflow DAG orchestration over the existing CLI command contract with deterministic task order and clear runtime outcomes.

## 2. What Was Implemented

- Added DAG artifact:
  - `orchestration/airflow/dags/weg_case_optional_etl_dag.py`
- Implemented deterministic linear orchestration chain:
  - `check_enable_airflow_flag -> run_profile -> run_extract_upload -> run_load_raw -> run_transform -> run_dq`
- Added fail-fast preflight guard:
  - DAG execution fails with a clear message when `enable_airflow` is disabled (`features.enable_airflow=false` / `WEG_ENABLE_AIRFLOW=false`).
- Wired each stage to existing CLI command contract with explicit runtime files:
  - `python cli.py <command> --config <path> --env-file <path>`
- Kept Airflow as an optional dependency:
  - DAG module imports safely even when Airflow is not installed.
- Added DAG-focused test coverage for command order, guard behavior, deterministic chain, and command wiring.

## 3. Commands Run

```powershell
pytest -q
python -c "import importlib.util, pathlib; p=pathlib.Path('orchestration/airflow/dags/weg_case_optional_etl_dag.py').resolve(); s=importlib.util.spec_from_file_location('phase08_dag_check', p); m=importlib.util.module_from_spec(s); import sys; sys.modules[s.name]=m; s.loader.exec_module(m); print('AIRFLOW_AVAILABLE=', m.AIRFLOW_AVAILABLE); print('CLI_COMMAND_ORDER=', m.CLI_COMMAND_ORDER)"
```

## 4. Key Files Created/Changed

- `orchestration/airflow/dags/weg_case_optional_etl_dag.py`
- `orchestration/airflow/dags/README.md`
- `tests/test_optional_airflow_dag.py`
- `README.md`
- `docs/progress/phase_08.md`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- Full test suite passes with Phase 8 additions (`30 passed`).
- DAG module import check confirms optional dependency behavior:
  - `AIRFLOW_AVAILABLE=False` on current environment (no Airflow installed)
  - Command sequence contract remains exact:
    - `('profile', 'extract-upload', 'load-raw', 'transform', 'dq')`
- New tests validate:
  - command order alignment with pipeline contract
  - fail-fast behavior when Airflow feature flag is off
  - successful preflight when flag is enabled
  - deterministic linear task-chain contract
  - explicit CLI command wiring with `--config` and `--env-file`

## 6. Risks / Assumptions

- Airflow package is not installed in the current local test environment; runtime DAG parsing inside an actual Airflow scheduler is therefore not executed here.
- DAG runtime expects valid project config/env paths; defaults can be overridden with:
  - `WEG_AIRFLOW_CONFIG_PATH`
  - `WEG_AIRFLOW_ENV_FILE`
- `run_mode` behavior is unchanged: `load-raw` and `transform` remain BigQuery/cloud-required as defined by existing pipeline rules.

## 7. What Comes Next

Proceed to Phase 09 for final documentation and interview-summary packaging with clear separation of core and optional completion evidence.
