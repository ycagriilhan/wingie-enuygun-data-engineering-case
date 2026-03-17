# Phase 03 - Raw BigQuery Load and Staging

## 1. Objective

Implement real BigQuery raw loading and staging-table transformations with rule-based clean/reject split for booking and search datasets.

## 2. What Was Implemented

- `load-raw` upgraded from placeholder to real BigQuery load jobs.
- BigQuery runtime preflight added for `load-raw` and `transform` (cloud mode, project, datasets, location).
- Raw dataset creation and full-refresh parquet loads (`WRITE_TRUNCATE`) implemented.
- Staging SQL pipeline added under `sql/staging/` and orchestrated by `transform`.
- Staging tables implemented:
  - `provider_clean`
  - `airport_reference_clean`
  - `booking_clean`
  - `booking_reject`
  - `search_clean`
  - `search_reject`
- Booking and search deduplication implemented on required grains and sort priorities.
- Invalid rows split into reject tables with explicit reason codes.

## 3. Commands Run

```powershell
pytest -q
python cli.py profile
python cli.py extract-upload
python cli.py load-raw        # expected failure in local mode (BigQuery-only preflight)
python cli.py transform       # expected failure in local mode (BigQuery-only preflight)
```

## 4. Key Files Created/Changed

- `src/weg_case_etl/pipeline.py`
- `src/weg_case_etl/config.py`
- `config/settings.yaml`
- `requirements.txt`
- `sql/staging/*.sql`
- `tests/test_cli.py`
- `docs/progress/master_status.md`

## 5. Validation Evidence

- Local unit test suite passes (`pytest -q`).
- SQL contract tests verify dedup and reject-reason logic.
- CLI preflight tests verify actionable failures when BigQuery execution is attempted without cloud mode.
- Extract command still works in local mode and writes manifest contract.

## 6. Risks / Assumptions

- End-to-end BigQuery integration validation requires real GCP credentials and bucket permissions.
- This phase intentionally focuses on raw and staging layers; mart-level outputs remain Phase 4 scope.

## 7. What Comes Next

Implement Phase 4 mart build and mandatory DQ assertions against staging clean/reject outputs, then finalize test and reporting coverage.
