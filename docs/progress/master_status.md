# Master Phase Status

| Phase | Branch | Status | Notes |
| --- | --- | --- | --- |
| 01 - Setup and Config | `phase/01-setup-config` | Completed | CLI scaffold, config schema, source data contract, and baseline tests added. |
| 02 - Extract Classify Upload | `phase/02-extract-classify-upload` | Completed | Classified landing layout with run_id paths and optional GCS upload support implemented. |
| 03 - Raw and Staging | `phase/03-raw-and-staging` | Completed | BigQuery raw load jobs and staging clean/reject SQL pipeline implemented. |
| 04 - Mart and DQ | `phase/04-mart-and-dq` | Completed | Mart booking_enriched build and strict mandatory business-rule DQ checks implemented. |
| 05 - Tests Validation | `phase/05-tests-validation` | Completed | Added validation evidence artifact and failure-aware run-all reporting with expanded test coverage; Phase 5 scope is core-only. |
| 06 - Optional Foundation | `phase/06-optional-foundation` | Completed (Local) | Optional feature flags and reserved optional paths are implemented locally on the Phase 6 branch. |
| 07 - Optional MERGE Upsert | `phase/07-optional-merge-upsert` | Completed | Added flag-controlled optional MERGE/Upsert for `mart.booking_enriched` keyed by `booking_id`, with update+insert semantics and no delete branch; cloud transform+dq validation passed with flag enabled. |
| 08 - Optional Airflow DAG | `phase/08-optional-airflow-dag` | Completed | Added optional Airflow DAG with deterministic command-chain orchestration (`profile -> extract-upload -> load-raw -> transform -> dq`), manual trigger mode, and fail-fast `enable_airflow` guard. |
| 09 - Docs Interview | `phase/09-docs-interview` | Pending | Final documentation and interview summary after optional phases complete. |
