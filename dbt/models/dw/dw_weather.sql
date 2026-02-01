{%- set rebuild_days = var('rebuild_days', 3) -%}
{%- set pre_hooks = [] -%}
{%- if is_incremental() -%}
  {# Rebuild the recent N days on each incremental run #}
  {%- do pre_hooks.append(
    "ALTER TABLE " ~ this ~
    " DELETE WHERE startTime >= now() - INTERVAL " ~ rebuild_days ~ " DAY" ~
    " SETTINGS mutations_sync = 2"
  ) -%}
{%- endif -%}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=pre_hooks
) }}

select
  startTime,
  endTime,
  locationName,
  WxName as weatherPhenomenon,
  WxValue as weatherValue,
  MinTName as minTemperature,
  MaxTName as maxTemperature,
  CIName as comfortIndex
from {{ source('ods_weather', 'weather') }}
{% if is_incremental() %}
where startTime >= now() - INTERVAL {{ rebuild_days }} DAY
{% endif %}
