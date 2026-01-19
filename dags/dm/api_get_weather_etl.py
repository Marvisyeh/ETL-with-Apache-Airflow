class WeatherDataMartETL:
  @staticmethod
  def create_table_schema_string():
    return """
    CREATE TABLE IF NOT EXISTS weather_summary (
      date DATE,
      year VARCHAR(4),
      month VARCHAR(2),
      day VARCHAR(2),
      week_day VARCHAR(2),
      locationName VARCHAR(50),
      startTime DateTime,
      endTime DateTime,
      weather_phenomenon VARCHAR(50),
      precipitation_probability DECIMAL(5,2),
      min_temperature DECIMAL(5,2),
      max_temperature DECIMAL(5,2),
      comfort_index VARCHAR(50),
      updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (date, locationName, startTime);
    """
  
  @staticmethod
  def read_dw_data():
    # Read from DW layer and join fact table with dimension tables
    sql_str = """
    SELECT 
      f.date,
      dt.year,
      dt.month,
      dt.day,
      dt.week_day,
      dl.locationName,
      f.startTime,
      f.endTime,
      f.weather_phenomenon,
      f.precipitation_probability,
      f.min_temperature,
      f.max_temperature,
      f.comfort_index,
      f.updated_at
    FROM dw.fact_weather f
    LEFT JOIN dw.dim_time dt ON f.date = dt.date
    LEFT JOIN dw.dim_location dl ON f.locationName = dl.locationName
    WHERE date(f.updated_at) = '{{ ds }}'
    """
    return sql_str
  
  @staticmethod
  def generate_insert_sql():
    # Insert data into DM layer from DW layer
    sql_str = """
    INSERT INTO weather_summary 
    SELECT 
      f.date,
      dt.year,
      dt.month,
      dt.day,
      dt.week_day,
      dl.locationName,
      f.startTime,
      f.endTime,
      f.weather_phenomenon,
      f.precipitation_probability,
      f.min_temperature,
      f.max_temperature,
      f.comfort_index,
      f.updated_at
    FROM dw.fact_weather f
    LEFT JOIN dw.dim_time dt ON f.date = dt.date
    LEFT JOIN dw.dim_location dl ON f.locationName = dl.locationName
    WHERE date(f.updated_at) = '{{ ds }}'
    """
    return sql_str
