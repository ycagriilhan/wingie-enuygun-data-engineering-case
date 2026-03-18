# Phase 04 - Mart Build and Mandatory DQ

## 1. Objective

Build the minimum required mart layer and enforce mandatory business-rule DQ checks with strict failure behavior in cloud mode.

## 2. What Was Implemented

- Added mart SQL pipeline under `sql/mart/` with `booking_enriched` output.
- Extended `transform` to:
  - execute staging SQL first, then mart SQL
  - ensure mart dataset exists
  - report mart outputs in `transform_report.json`
- Replaced placeholder DQ status with mandatory BigQuery assertions for:
  - `origin != destination`
  - `return_date` and `direction_type` consistency
  - timestamp compatibility (`created_at`)
  - negative total/price exclusion
  - duplicate grain checks (`booking_id`, `request_id`, mart `booking_id`)
- Updated DQ to strict mode:
  - write detailed `dq_report.json`
  - raise pipeline failure when any mandatory check fails in cloud mode

## 3. Commands Run

```powershell
pytest -q
python cli.py dq --env-file .env.example
```

## 4. Key Files Created/Changed

- `src/weg_case_etl/pipeline.py`
- `sql/mart/01_booking_enriched.sql`
- `tests/test_cli.py`
- `README.md`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- New tests added for:
  - mart SQL contract
  - DQ cloud pass/fail paths
  - `run-all` failure propagation when DQ fails
- Existing Phase 1-3 tests remain green.
- DQ reports now include per-check metric values and expected values.

## 6. Risks / Assumptions

- Full cloud integration for DQ/transform still depends on valid GCP credentials and permissions.
- Local mode keeps cloud DQ checks as pending by design.

## 7. What Comes Next

Expand Phase 5 validation outputs and improve end-to-end evidence coverage for cloud runs.
