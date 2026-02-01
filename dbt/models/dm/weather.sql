{%- set rebuild_days = var('rebuild_days', 3) -%}
{%- set pre_hooks = [] -%}
{%- if is_incremental() -%}
  {# Rebuild the recent N days on each incremental run #}
  {%- do pre_hooks.append(
    "ALTER TABLE " ~ this ~
    " DELETE WHERE date >= today() - INTERVAL " ~ rebuild_days ~ " DAY" ~
    " SETTINGS mutations_sync = 2"
  ) -%}
{%- endif -%}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=pre_hooks
) }}

WITH ranked AS (
  SELECT
    toDate(startTime) AS date,
    locationName,
    weatherValue,
    weatherPhenomenon,
    minTemperature,
    maxTemperature,
    comfortIndex,
    row_number() OVER (
      PARTITION BY toDate(startTime), locationName
      ORDER BY endTime DESC
    ) AS rn
  FROM {{ ref('dw_weather') }}
  {% if is_incremental() %}
  WHERE startTime >= now() - INTERVAL {{ rebuild_days }} DAY
  {% endif %}
)
SELECT
  date,
  locationName,
  weatherValue,
  weatherPhenomenon,
  minTemperature,
  maxTemperature,
  comfortIndex
FROM ranked
WHERE rn = 1