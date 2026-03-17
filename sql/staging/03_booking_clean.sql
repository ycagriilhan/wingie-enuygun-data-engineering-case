CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{STAGING_DATASET}}.booking_clean` AS
WITH booking_base AS (
  SELECT
    CAST(id AS STRING) AS booking_id,
    CAST(request_id AS STRING) AS request_id,
    CAST(provider_id AS INT64) AS provider_id,
    UPPER(TRIM(CAST(origin AS STRING))) AS origin,
    UPPER(TRIM(CAST(destination AS STRING))) AS destination,
    SAFE_CAST(NULLIF(TRIM(CAST(departure_date AS STRING)), '') AS DATE) AS departure_date,
    SAFE_CAST(NULLIF(TRIM(CAST(return_date AS STRING)), '') AS DATE) AS return_date,
    CASE
      WHEN NULLIF(TRIM(CAST(return_date AS STRING)), '') IS NULL THEN 'oneway'
      ELSE 'roundtrip'
    END AS direction_type,
    CASE
      WHEN CAST(is_one_way AS INT64) = 1 THEN 'oneway'
      ELSE 'roundtrip'
    END AS direction_type_from_flag,
    UPPER(TRIM(CAST(currency AS STRING))) AS currency,
    CAST(total AS FLOAT64) AS total,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(is_one_way AS INT64) AS is_one_way,
    CAST(status_code AS INT64) AS status_code,
    CASE
      WHEN UPPER(TRIM(CAST(origin AS STRING))) = UPPER(TRIM(CAST(destination AS STRING))) THEN 'origin_equals_destination'
      WHEN NOT REGEXP_CONTAINS(UPPER(TRIM(CAST(currency AS STRING))), r'^[A-Z]{3}$') THEN 'invalid_currency'
      WHEN CAST(total AS FLOAT64) < 0 THEN 'negative_total'
      WHEN (
        CASE
          WHEN NULLIF(TRIM(CAST(return_date AS STRING)), '') IS NULL THEN 'oneway'
          ELSE 'roundtrip'
        END
      ) != (
        CASE
          WHEN CAST(is_one_way AS INT64) = 1 THEN 'oneway'
          ELSE 'roundtrip'
        END
      ) THEN 'direction_flag_mismatch'
      WHEN SAFE_CAST(NULLIF(TRIM(CAST(departure_date AS STRING)), '') AS DATE) IS NULL THEN 'invalid_departure_date'
      ELSE NULL
    END AS quality_reason
  FROM `{{PROJECT_ID}}.{{RAW_DATASET}}.booking`
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY booking_id
      ORDER BY updated_at DESC, created_at DESC
    ) AS row_num
  FROM booking_base
)
SELECT
  booking_id,
  request_id,
  provider_id,
  origin,
  destination,
  departure_date,
  return_date,
  direction_type,
  direction_type_from_flag,
  currency,
  total,
  created_at,
  updated_at,
  is_one_way,
  status_code
FROM ranked
WHERE row_num = 1
  AND quality_reason IS NULL;
