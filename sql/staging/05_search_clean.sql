CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{STAGING_DATASET}}.search_clean` AS
WITH search_base AS (
  SELECT
    CAST(environment AS STRING) AS environment,
    UPPER(TRIM(CAST(origin AS STRING))) AS origin,
    UPPER(TRIM(CAST(destination AS STRING))) AS destination,
    SAFE_CAST(NULLIF(TRIM(CAST(departure_date AS STRING)), '') AS DATE) AS departure_date,
    SAFE_CAST(NULLIF(TRIM(CAST(return_date AS STRING)), '') AS DATE) AS return_date,
    LOWER(TRIM(CAST(direction_type AS STRING))) AS direction_type,
    CAST(locale AS STRING) AS locale,
    UPPER(TRIM(CAST(market AS STRING))) AS market,
    UPPER(TRIM(CAST(selected_currency AS STRING))) AS selected_currency,
    CAST(request_id AS STRING) AS request_id,
    CAST(session_id AS STRING) AS session_id,
    CAST(user_id AS STRING) AS user_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(passenger_types.adult AS INT64) AS passenger_adult,
    CAST(passenger_types.child AS INT64) AS passenger_child,
    CAST(passenger_types.infant AS INT64) AS passenger_infant,
    CAST(price_detay.cheapest_price AS FLOAT64) AS cheapest_price,
    CAST(price_detay.flight_count AS INT64) AS flight_count,
    UPPER(TRIM(CAST(price_detay.currency AS STRING))) AS price_currency,
    CASE
      WHEN NULLIF(TRIM(CAST(return_date AS STRING)), '') IS NULL THEN 'oneway'
      ELSE 'roundtrip'
    END AS direction_type_expected,
    CASE
      WHEN UPPER(TRIM(CAST(origin AS STRING))) = UPPER(TRIM(CAST(destination AS STRING))) THEN 'origin_equals_destination'
      WHEN LOWER(TRIM(CAST(direction_type AS STRING))) != (
        CASE
          WHEN NULLIF(TRIM(CAST(return_date AS STRING)), '') IS NULL THEN 'oneway'
          ELSE 'roundtrip'
        END
      ) THEN 'direction_return_date_mismatch'
      WHEN NOT REGEXP_CONTAINS(UPPER(TRIM(CAST(selected_currency AS STRING))), r'^[A-Z]{3}$') THEN 'invalid_selected_currency'
      WHEN NOT REGEXP_CONTAINS(UPPER(TRIM(CAST(price_detay.currency AS STRING))), r'^[A-Z]{3}$') THEN 'invalid_price_currency'
      WHEN CAST(price_detay.cheapest_price AS FLOAT64) < 0 THEN 'negative_cheapest_price'
      WHEN SAFE_CAST(NULLIF(TRIM(CAST(departure_date AS STRING)), '') AS DATE) IS NULL THEN 'invalid_departure_date'
      ELSE NULL
    END AS quality_reason
  FROM `{{PROJECT_ID}}.{{RAW_DATASET}}.search`
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY request_id
      ORDER BY created_at DESC, session_id DESC
    ) AS row_num
  FROM search_base
)
SELECT
  environment,
  origin,
  destination,
  departure_date,
  return_date,
  direction_type,
  direction_type_expected,
  locale,
  market,
  selected_currency,
  request_id,
  session_id,
  user_id,
  created_at,
  passenger_adult,
  passenger_child,
  passenger_infant,
  cheapest_price,
  flight_count,
  price_currency
FROM ranked
WHERE row_num = 1
  AND quality_reason IS NULL;
