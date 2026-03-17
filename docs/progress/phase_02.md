# Phase 02 - Extract, Classify, Upload

## 1. Objective

Implement a real Phase 2 flow that classifies source parquet datasets, lands them in a structured path, and supports optional cloud bucket upload in cloud mode.

## 2. What Was Implemented

- `extract-upload` now:
  - classifies files by dataset/domain (`booking`, `search`, `provider`, `airport_reference`)
  - creates run-scoped landing paths: `data/landing/<run_id>/<dataset>/...`
  - writes enriched extract manifest with row count, columns, size, and landing metadata
  - uploads to GCS in `cloud` mode via `google-cloud-storage`
- `load-raw` now consumes the extract manifest instead of flat landing assumptions.
- `profile` now reports classification metadata.
- Phase 2 tests were added for classified landing outputs and raw table mapping.

## 3. Commands Run

```powershell
python cli.py extract-upload
python cli.py load-raw
python cli.py run-all
pytest -q
```

## 4. Key Files Created/Changed

- `src/weg_case_etl/pipeline.py`
- `src/weg_case_etl/contracts.py`
- `src/weg_case_etl/config.py`
- `config/settings.yaml`
- `.env.example`
- `requirements.txt`
- `tests/test_cli.py`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- `extract_upload_manifest.json` includes `run_id`, classified datasets, and landing paths.
- `raw_table_manifest.json` maps to `raw.airport_reference`, `raw.booking`, `raw.provider`, and `raw.search`.
- `pytest -q` passes with new Phase 2 assertions.

## 6. Risks / Assumptions

- Real cloud upload requires valid GCP credentials and bucket permissions in runtime environment.
- BigQuery raw table loading remains scheduled for Phase 3.

## 7. What Comes Next

Implement Phase 3 raw BigQuery loading and staging-layer transformations using the Phase 2 extract manifest as the ingestion contract.
