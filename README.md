# Wingie / Enuygun Data Engineering Case

Phase 1 + Phase 2 baseline for a local-first ETL workflow with a stable command contract.

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
