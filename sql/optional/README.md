# Optional SQL Contract

This directory contains executable optional SQL artifacts for Phase 7 MERGE/Upsert behavior.

## Active Artifact

- `01_booking_enriched_upsert.sql`

## Runtime Selection

- When `features.enable_merge_upsert=false` (`WEG_ENABLE_MERGE_UPSERT=false`), `transform` executes standard mart SQL under `sql/mart/`.
- When `features.enable_merge_upsert=true` (`WEG_ENABLE_MERGE_UPSERT=true`), `transform` executes SQL in this folder and skips `sql/mart/`.

## Upsert Policy

- Target table: `{{PROJECT_ID}}.{{MART_DATASET}}.booking_enriched`
- Merge key: `booking_id`
- Behavior: `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`
- No stale-key delete branch (`NOT MATCHED BY SOURCE THEN DELETE`) by design.
