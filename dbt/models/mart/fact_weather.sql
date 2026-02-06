{{ config(
    materialized='table',
    engine='MergeTree()',
    schema='mart'
) }}

SELECT
    locationName AS location_name,
    startTime AS start_time,
    endTime AS end_time,
    WeatherConditionName AS weather_desc,
    ProbabilityofPrecipitation AS rain_chance_pct,
    MaximumTemperature AS max_temp,
    MinimumTemperature AS min_temp,
    (MaximumTemperature - MinimumTemperature) AS temp_range,
    CASE 
        WHEN ComfortIndex LIKE '%寒冷%' THEN '需加強保暖'
        WHEN ComfortIndex LIKE '%舒適%' THEN '天氣宜人'
        ELSE '注意氣溫變化'
    END AS activity_suggestion
FROM {{ ref('stg_api__weathers') }}