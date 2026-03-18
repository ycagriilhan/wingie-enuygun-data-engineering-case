# Phase 07 - Optional MERGE/Upsert

## 1. Objective

Implement optional MERGE/Upsert behavior for `mart.booking_enriched` while keeping core behavior unchanged when the optional flag is disabled.

## 2. What Was Implemented

- Added optional SQL artifact:
  - `sql/optional/01_booking_enriched_upsert.sql`
- Updated `transform` planning logic:
  - always executes staging SQL as before
  - executes `sql/mart/*.sql` when `enable_merge_upsert=false`
  - executes `sql/optional/*.sql` and skips `sql/mart/*.sql` when `enable_merge_upsert=true`
- Added explicit optional SQL loading path through `optional_paths.sql_optional_dir`.
- Added tests for:
  - transform SQL-plan selection with flag off/on
  - clear failure when optional upsert SQL is missing
  - optional MERGE SQL contract (target, key, matched/not-matched branches, no delete branch)

## 3. Commands Run

```powershell
pytest -q
python cli.py profile --env-file .env.example
$env:GOOGLE_APPLICATION_CREDENTIALS=(Resolve-Path '.gcloud/application_default_credentials.json').Path; python cli.py run-all
$env:GOOGLE_APPLICATION_CREDENTIALS=(Resolve-Path '.gcloud/application_default_credentials.json').Path; $env:WEG_ENABLE_MERGE_UPSERT='true'; python cli.py transform
$env:GOOGLE_APPLICATION_CREDENTIALS=(Resolve-Path '.gcloud/application_default_credentials.json').Path; $env:WEG_ENABLE_MERGE_UPSERT='true'; python cli.py dq
```

## 4. Key Files Created/Changed

- `src/weg_case_etl/pipeline.py`
- `sql/optional/01_booking_enriched_upsert.sql`
- `tests/test_optional_upsert.py`
- `README.md`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- Test suite passes with Phase 7 additions (`25 passed`).
- Cloud baseline run (`run-all`) succeeds with flag off.
- Cloud upsert validation with `WEG_ENABLE_MERGE_UPSERT=true`:
  - `transform` succeeds and reports `layer: optional_upsert` for `sql/optional/01_booking_enriched_upsert.sql`
  - `dq` succeeds with `overall_status=pass`
- Output contract remains stable (`transform` still reports `output_table_count=7` and standard output table list).

## 6. Risks / Assumptions

- Phase 7 scope is mart-only upsert (`mart.booking_enriched`), not staging-table upsert.
- Upsert policy is update+insert only; rows missing from latest source are retained in target table.
- Cloud-mode end-to-end execution depends on credentials and GCS permissions; when optional flag is enabled, direct `transform`/`dq` validation is still possible if `extract-upload` is blocked by storage permissions.

## 7. What Comes Next

Implement Phase 08 optional Airflow DAG orchestration using the existing command contract and optional feature toggles.
