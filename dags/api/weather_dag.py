from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from raw.api.weather import GetWeatherDataAPI


def get_raw_weather_data(**context):
    execution_date = context['ds']
    etl = GetWeatherDataAPI()
    file_path = etl.get_weather_data(execution_date)
    file_path = file_path.replace(r'/opt/airflow/dags/temp_data/', r'/var/lib/clickhouse/user_files/temp_data/')
    context['ti'].xcom_push(key='file_path', value=file_path)


default_args = {
    'owner': 'marvis',
    'start_date': '2026-01-16',
    'email': ['myemail@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='raw_api_weather',
    default_args=default_args,
    schedule='0 0,6 * * *',  # every 6 hours
    description='daily weather data from api',
    tags=['raw', 'api', 'weather']
)

raw_weather_data = PythonOperator(
    task_id='get_raw_weather_data',
    python_callable=get_raw_weather_data,
    dag=dag
)

## TODO 檢查分層策略以及轉成view效能和刪除機制
## ex: 如果以月季年為單位，需要考慮刪除機制，當跨月季年時，需要刪除該月季年的資料
create_table = ClickHouseOperator(
    task_id='create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS raw.raw_api__weathers (
      elementName Nullable(String),
      locationName String,
      startTime DateTime,
      endTime DateTime,
      parameterName Nullable(String),
      parameterValue Nullable(String),
      parameterUnit Nullable(String),
      loaded_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(loaded_at)
    ORDER BY (locationName, startTime, elementName)
    PRIMARY KEY (locationName, startTime)
    """,
    clickhouse_conn_id='clickhouse_raw',
    dag=dag
)

insert_data_from_parquet = ClickHouseOperator(
    task_id='insert_data_from_parquet',
    sql="""
    INSERT INTO raw.raw_api__weathers (elementName, locationName, startTime, endTime, parameterName, parameterValue, parameterUnit)
    SELECT 
      elementName,
      locationName,
      toDateTime(startTime) as startTime,
      toDateTime(endTime) as endTime,
      parameterName,
      parameterValue,
      parameterUnit
    FROM file('{{ ti.xcom_pull(task_ids='get_raw_weather_data', key='file_path') }}', 'Parquet')
    WHERE locationName IS NOT NULL
    """,
    clickhouse_conn_id='clickhouse_raw',
    dag=dag
)

## TODO: trigger dbt transform task


raw_weather_data >> create_table >> insert_data_from_parquet