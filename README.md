# Wingie / Enuygun Data Engineering Case

Phase 1 bootstrap for a local-first ETL workflow with a stable command contract.

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
