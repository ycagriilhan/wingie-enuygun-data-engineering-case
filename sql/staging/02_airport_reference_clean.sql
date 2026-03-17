CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{STAGING_DATASET}}.airport_reference_clean` AS
SELECT
  UPPER(TRIM(CAST(airport_code AS STRING))) AS airport_code,
  CAST(city AS STRING) AS city,
  UPPER(TRIM(CAST(country_code AS STRING))) AS country_code,
  CAST(timezone AS STRING) AS timezone
FROM `{{PROJECT_ID}}.{{RAW_DATASET}}.airport_reference`;
