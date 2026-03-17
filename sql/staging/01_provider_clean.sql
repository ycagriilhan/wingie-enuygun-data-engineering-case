CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{STAGING_DATASET}}.provider_clean` AS
SELECT
  CAST(id AS INT64) AS provider_id,
  CAST(name AS STRING) AS provider_name,
  CAST(slug AS STRING) AS provider_slug,
  CAST(is_active AS INT64) AS is_active
FROM `{{PROJECT_ID}}.{{RAW_DATASET}}.provider`;
