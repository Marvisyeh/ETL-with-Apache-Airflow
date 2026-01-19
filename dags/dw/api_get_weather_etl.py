import pandas as pd

class GetWeatherDataETL:
  @staticmethod
  def create_table_schema_string():
    return (
      """
      CREATE TABLE IF NOT EXISTS dim_time (
        date DATE,
        year varchar(4),
        month varchar(2),
        day varchar(2),
        week_day varchar(2)
      ) ENGINE = ReplacingMergeTree(date)
      ORDER BY (date);""",
      """
      CREATE TABLE IF NOT EXISTS dim_location (
        locationName VARCHAR(50),
        updated_at DateTime DEFAULT now()
      ) ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (locationName);
      """,
      """
      CREATE TABLE IF NOT EXISTS fact_weather (
        date DATE,
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
      """)

  @staticmethod
  def read_weather_data():
    # Use Airflow template variable {{ ds }} directly in SQL
    # This will be replaced by Airflow at runtime
    sql_str = """
    SELECT * FROM weather WHERE date(updated_at) = '{{ ds }}'
    """
    return sql_str
  
  @staticmethod
  def transform_weather_data(data):
    df = pd.DataFrame(data)
    columns = ['startTime', 'endTime', 'locationName', 'WxName', 'WxValue', 'PoPName', 'PoPUnit', 'MinTName', 'MinTUnit', 'MaxTName', 'MaxTUnit', 'CIName', 'updated_at']
    df.columns = columns
    
    # Convert startTime to datetime if it's not already
    df['startTime'] = pd.to_datetime(df['startTime'])
    df['endTime'] = pd.to_datetime(df['endTime'])
    
    # Extract unique dates
    date_data = df['startTime'].dt.date.unique()
    date_data = pd.DataFrame(date_data, columns=['date'])
    
    # Convert date column to datetime for .dt accessor
    date_data['date'] = pd.to_datetime(date_data['date'])
    
    # Extract date components
    date_data['year'] = date_data['date'].dt.year.astype(str)
    date_data['month'] = date_data['date'].dt.month.astype(str).str.zfill(2)
    date_data['day'] = date_data['date'].dt.day.astype(str).str.zfill(2)
    date_data['week_day'] = (date_data['date'].dt.weekday + 1).astype(str)
    
    # Convert date back to date type for SQL
    date_data['date'] = date_data['date'].dt.strftime('%Y-%m-%d')
    print(date_data)

    location_data = df['locationName'].drop_duplicates()
    print(location_data)
    
    # Prepare fact table data - only measures and foreign keys
    fact_data = df.copy()
    fact_data['date'] = fact_data['startTime'].dt.date.astype(str)
    # Format datetime columns for SQL
    fact_data['startTime'] = fact_data['startTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    fact_data['endTime'] = fact_data['endTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Transform to fact table format: only measures (numeric values) and foreign keys
    # Extract numeric values from Unit columns, handle missing values
    fact_data['precipitation_probability'] = pd.to_numeric(fact_data['PoPUnit'], errors='coerce').fillna(0)
    fact_data['min_temperature'] = pd.to_numeric(fact_data['MinTUnit'], errors='coerce').fillna(0)
    fact_data['max_temperature'] = pd.to_numeric(fact_data['MaxTUnit'], errors='coerce').fillna(0)
    
    # Keep weather phenomenon and comfort index as they are categorical measures
    fact_data['weather_phenomenon'] = fact_data['WxValue']
    fact_data['comfort_index'] = fact_data['CIName']
    
    # Select only fact table columns: foreign keys + measures
    fact_data = fact_data[['date', 'locationName', 'startTime', 'endTime', 
                          'weather_phenomenon', 'precipitation_probability', 
                          'min_temperature', 'max_temperature', 'comfort_index']]
    print(fact_data)
    
    return date_data, location_data, fact_data
  
  @staticmethod
  def generate_load_data_sql_string(date_data, location_data, fact_data):
    dim_time_insert_sql = (
      "INSERT INTO dim_time (date, year, month, day, week_day) VALUES "
      f"{','.join([str(tuple(col)) for col in date_data.values])};"
    )
    dim_location_insert_sql = (
      "INSERT INTO dim_location (locationName) VALUES "
      f"{','.join([f"('{loc}')" for loc in location_data.values])};"
    )
    fact_weather_insert_sql = (
      "INSERT INTO fact_weather (date, locationName, startTime, endTime, weather_phenomenon, precipitation_probability, min_temperature, max_temperature, comfort_index) VALUES "
      f"{','.join([str(tuple(col)) for col in fact_data.values])};"
    )
    return {
      'dim_time_insert_sql': dim_time_insert_sql,
      'dim_location_insert_sql': dim_location_insert_sql,
      'fact_weather_insert_sql': fact_weather_insert_sql
    }
