# Wingie / Enuygun Data Engineering Case

Phase 1 + Phase 2 + Phase 3 + Phase 4 baseline for a local-first ETL workflow with a stable command contract.

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
