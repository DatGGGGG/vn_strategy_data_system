\pset pager off

WITH checks AS (
  SELECT
    'core.dim_game_info'::text AS object_name,
    COUNT(*)::bigint AS rows,
    COUNT(DISTINCT unified_app_id)::bigint AS entities,
    NULL::date AS min_date,
    NULL::date AS max_date
  FROM core.dim_game_info

  UNION ALL
  SELECT
    'core.dim_app_info',
    COUNT(*)::bigint,
    COUNT(DISTINCT app_id)::bigint,
    NULL::date,
    NULL::date
  FROM core.dim_app_info

  UNION ALL
  SELECT
    'core.fact_app_performance_daily',
    COUNT(*)::bigint,
    COUNT(DISTINCT app_id)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_app_performance_daily

  UNION ALL
  SELECT
    'core.fact_app_performance_active_users',
    COUNT(*)::bigint,
    COUNT(DISTINCT app_id)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_app_performance_active_users

  UNION ALL
  SELECT
    'core.fact_app_performance_retention',
    COUNT(*)::bigint,
    COUNT(DISTINCT app_id)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_app_performance_retention

  UNION ALL
  SELECT
    'core.fact_user_demographics',
    COUNT(*)::bigint,
    COUNT(DISTINCT app_id)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_user_demographics

  UNION ALL
  SELECT
    'analytics.agg_game_performance_daily',
    COUNT(*)::bigint,
    COUNT(DISTINCT unified_app_id)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM analytics.agg_game_performance_daily

  UNION ALL
  SELECT
    'analytics.agg_game_performance_monthly',
    COUNT(*)::bigint,
    COUNT(DISTINCT unified_app_id)::bigint,
    MIN(month)::date,
    MAX(month)::date
  FROM analytics.agg_game_performance_monthly

  UNION ALL
  SELECT
    'analytics.agg_game_performance_yearly',
    COUNT(*)::bigint,
    COUNT(DISTINCT unified_app_id)::bigint,
    MAKE_DATE(MIN(year), 1, 1),
    MAKE_DATE(MAX(year), 1, 1)
  FROM analytics.agg_game_performance_yearly
)
SELECT *
FROM checks
ORDER BY object_name;

SELECT
  source,
  country,
  rows,
  min_date,
  max_date
FROM (
  SELECT
    'core.performance.android'::text AS source,
    country_android AS country,
    COUNT(*)::bigint AS rows,
    MIN("date")::date AS min_date,
    MAX("date")::date AS max_date
  FROM core.fact_app_performance_daily
  GROUP BY country_android

  UNION ALL
  SELECT
    'core.performance.ios',
    country_ios,
    COUNT(*)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_app_performance_daily
  GROUP BY country_ios

  UNION ALL
  SELECT
    'core.active_users',
    country,
    COUNT(*)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM core.fact_app_performance_active_users
  GROUP BY country

  UNION ALL
  SELECT
    'analytics.daily',
    country,
    COUNT(*)::bigint,
    MIN("date")::date,
    MAX("date")::date
  FROM analytics.agg_game_performance_daily
  GROUP BY country
) by_country
WHERE country IN ('VN', 'CN', 'WW')
ORDER BY source, country;
