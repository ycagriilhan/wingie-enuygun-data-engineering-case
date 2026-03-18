# Wingie / Enuygun Data Engineering Case

Core baseline covers Phases 1-5 for a local-first ETL workflow with a stable command contract.

## Data Contract

Source files are versioned under `data/source/`:

- `airports.parquet`
- `booking_10k.parquet`
- `provider.parquet`
- `search_500k.parquet`

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

## Command Contract

All commands are exposed from one entrypoint:

```powershell
python cli.py profile
python cli.py extract-upload
python cli.py load-raw
python cli.py transform
python cli.py dq
python cli.py run-all
```

Options:

- `--config` (default: `config/settings.yaml`)
- `--env-file` (default: `.env`)

## Phase Roadmap (Current Numbering)

- Phase 1-5: Core implementation (completed baseline)
- Phase 6: Optional Foundation
- Phase 7: Optional MERGE/Upsert
- Phase 8: Optional Airflow DAG
- Phase 9: Docs and Interview Prep

## Phase 6 - Optional Foundation

Phase 6 optional foundation is implemented as reserved interfaces only (no core behavior changes):
Phase 5 remains core tests/validation only.

- Feature flags in config/env (default disabled):
  - `features.enable_merge_upsert` / `WEG_ENABLE_MERGE_UPSERT=false`
  - `features.enable_airflow` / `WEG_ENABLE_AIRFLOW=false`
- Reserved optional paths:
  - `sql/optional/` for MERGE/Upsert SQL scripts
  - `orchestration/airflow/dags/` for Airflow DAG files

Core command contract remains unchanged.

## Phase 7 - Optional MERGE/Upsert

Phase 7 adds optional upsert behavior for `mart.booking_enriched` with feature flag control:

- Flag off (`features.enable_merge_upsert=false` / `WEG_ENABLE_MERGE_UPSERT=false`):
  - `transform` keeps existing mart SQL flow under `sql/mart/`
- Flag on (`features.enable_merge_upsert=true` / `WEG_ENABLE_MERGE_UPSERT=true`):
  - `transform` runs optional SQL under `sql/optional/`
  - standard mart create/replace SQL is skipped
  - upsert key is `booking_id`
  - behavior is update + insert only (no delete of stale keys)

## Phase 2 Behavior

- `extract-upload` classifies source files into dataset groups and lands them under:
  - `data/landing/<run_id>/<dataset>/<file>`
- In `cloud` mode, `extract-upload` uploads landed files to:
  - `gs://<bucket>/<landing_prefix>/<run_id>/<dataset>/<file>`
- `load-raw` now reads the extract manifest and maps classified files to `raw.*` target tables.

## Phase 3 Behavior

- `load-raw` is BigQuery-only in Phase 3 and performs real load jobs from `gcs_uri` values in extract manifest.
- `transform` is BigQuery-only in Phase 3 and executes SQL scripts under `sql/staging/` to build:
  - `provider_clean`
  - `airport_reference_clean`
  - `booking_clean`
  - `booking_reject`
  - `search_clean`
  - `search_reject`
- Invalid records are split into reject tables with explicit reason codes; clean tables keep valid latest records per grain.

## Phase 4 Behavior

- `transform` is extended to execute `sql/mart/` after staging and build:
  - `mart.booking_enriched`
- `dq` in `cloud` mode executes mandatory business-rule checks against staging/mart outputs.
- In `cloud` mode, `dq` fails the command (non-zero exit) when any mandatory rule check fails.

## Phase 5 Behavior

- `run-all` always writes `run_all_report.json`, including failure metadata (`overall_status`, `failed_step`, `error_message`).
- `run-all` still exits non-zero when a stage fails.
- A consolidated `validation_evidence_report.json` is generated to validate cross-artifact consistency.
- Cloud-mode evidence checks include report existence, run_id consistency, step-count consistency, and `dq` pass state.
- Local mode marks cloud-only evidence checks as `pending`.
