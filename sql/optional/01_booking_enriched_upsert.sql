CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{MART_DATASET}}.booking_enriched` AS
SELECT *
FROM (
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
    ON b.destination = ad.airport_code
) AS seed
WHERE FALSE;

MERGE `{{PROJECT_ID}}.{{MART_DATASET}}.booking_enriched` AS target
USING (
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
    ON b.destination = ad.airport_code
) AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN
  UPDATE SET
    request_id = source.request_id,
    provider_id = source.provider_id,
    provider_name = source.provider_name,
    provider_slug = source.provider_slug,
    provider_is_active = source.provider_is_active,
    origin_airport_code = source.origin_airport_code,
    origin_city = source.origin_city,
    origin_country_code = source.origin_country_code,
    origin_timezone = source.origin_timezone,
    destination_airport_code = source.destination_airport_code,
    destination_city = source.destination_city,
    destination_country_code = source.destination_country_code,
    destination_timezone = source.destination_timezone,
    departure_date = source.departure_date,
    return_date = source.return_date,
    direction_type = source.direction_type,
    direction_type_from_flag = source.direction_type_from_flag,
    currency = source.currency,
    total = source.total,
    market = source.market,
    locale = source.locale,
    environment = source.environment,
    session_id = source.session_id,
    user_id = source.user_id,
    selected_currency = source.selected_currency,
    search_price_currency = source.search_price_currency,
    search_cheapest_price = source.search_cheapest_price,
    search_flight_count = source.search_flight_count,
    passenger_adult = source.passenger_adult,
    passenger_child = source.passenger_child,
    passenger_infant = source.passenger_infant,
    booking_created_at = source.booking_created_at,
    booking_updated_at = source.booking_updated_at,
    search_created_at = source.search_created_at
WHEN NOT MATCHED THEN
  INSERT (
    booking_id,
    request_id,
    provider_id,
    provider_name,
    provider_slug,
    provider_is_active,
    origin_airport_code,
    origin_city,
    origin_country_code,
    origin_timezone,
    destination_airport_code,
    destination_city,
    destination_country_code,
    destination_timezone,
    departure_date,
    return_date,
    direction_type,
    direction_type_from_flag,
    currency,
    total,
    market,
    locale,
    environment,
    session_id,
    user_id,
    selected_currency,
    search_price_currency,
    search_cheapest_price,
    search_flight_count,
    passenger_adult,
    passenger_child,
    passenger_infant,
    booking_created_at,
    booking_updated_at,
    search_created_at
  )
  VALUES (
    source.booking_id,
    source.request_id,
    source.provider_id,
    source.provider_name,
    source.provider_slug,
    source.provider_is_active,
    source.origin_airport_code,
    source.origin_city,
    source.origin_country_code,
    source.origin_timezone,
    source.destination_airport_code,
    source.destination_city,
    source.destination_country_code,
    source.destination_timezone,
    source.departure_date,
    source.return_date,
    source.direction_type,
    source.direction_type_from_flag,
    source.currency,
    source.total,
    source.market,
    source.locale,
    source.environment,
    source.session_id,
    source.user_id,
    source.selected_currency,
    source.search_price_currency,
    source.search_cheapest_price,
    source.search_flight_count,
    source.passenger_adult,
    source.passenger_child,
    source.passenger_infant,
    source.booking_created_at,
    source.booking_updated_at,
    source.search_created_at
  );
