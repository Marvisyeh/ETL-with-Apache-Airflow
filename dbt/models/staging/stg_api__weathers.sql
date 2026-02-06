SELECT
    toDateTime(startTime) AS startTime,
    toDateTime(endTime) AS endTime,
    locationName,
    toInt32(MAX(CASE WHEN elementName = 'Wx' THEN parameterValue  END)) AS WeatherConditionCode,
    MAX(CASE WHEN elementName = 'Wx' THEN parameterName  END) AS WeatherConditionName,
    toInt32(MAX(CASE WHEN elementName = 'PoP' THEN parameterName END)) AS ProbabilityofPrecipitation,
    MAX(CASE WHEN elementName = 'PoP' THEN parameterUnit END) AS ProbabilityofPrecipitationUnit,
    toInt32(MAX(CASE WHEN elementName = 'MinT' THEN parameterName END)) AS MinimumTemperature,
    toInt32(MAX(CASE WHEN elementName = 'MaxT' THEN parameterName END)) AS MaximumTemperature,
    MAX(CASE WHEN elementName = 'MinT' THEN parameterUnit END) AS TemperatureUnit,
    MAX(CASE WHEN elementName = 'CI'   THEN parameterName  END) AS ComfortIndex
FROM {{ source('landing_zone', 'raw_api__weathers') }}
GROUP BY
    startTime,
    endTime,
    locationName
ORDER BY
    locationName,
    startTime