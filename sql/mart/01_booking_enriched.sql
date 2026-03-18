CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{MART_DATASET}}.booking_enriched` AS
SELECT
  b.booking_id,
  b.request_id,
  b.provider_id,
  p.provider_name,
  p.provider_slug,
  p.is_active AS provider_is_active,
  b.origin AS origin_airport_code,
  ao.city AS origin_city,
  ao.country_code AS origin_country_code,
  ao.timezone AS origin_timezone,
  b.destination AS destination_airport_code,
  ad.city AS destination_city,
  ad.country_code AS destination_country_code,
  ad.timezone AS destination_timezone,
  b.departure_date,
  b.return_date,
  b.direction_type,
  b.direction_type_from_flag,
  b.currency,
  b.total,
  s.market,
  s.locale,
  s.environment,
  s.session_id,
  s.user_id,
  s.selected_currency,
  s.price_currency AS search_price_currency,
  s.cheapest_price AS search_cheapest_price,
  s.flight_count AS search_flight_count,
  s.passenger_adult,
  s.passenger_child,
  s.passenger_infant,
  b.created_at AS booking_created_at,
  b.updated_at AS booking_updated_at,
  s.created_at AS search_created_at
FROM `{{PROJECT_ID}}.{{STAGING_DATASET}}.booking_clean` AS b
LEFT JOIN `{{PROJECT_ID}}.{{STAGING_DATASET}}.search_clean` AS s
  ON b.request_id = s.request_id
LEFT JOIN `{{PROJECT_ID}}.{{STAGING_DATASET}}.provider_clean` AS p
  ON b.provider_id = p.provider_id
LEFT JOIN `{{PROJECT_ID}}.{{STAGING_DATASET}}.airport_reference_clean` AS ao
  ON b.origin = ao.airport_code
LEFT JOIN `{{PROJECT_ID}}.{{STAGING_DATASET}}.airport_reference_clean` AS ad
  ON b.destination = ad.airport_code;
