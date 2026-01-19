from datetime import timedelta

from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

from dm.api_get_weather_etl import WeatherDataMartETL


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
    dag_id='dm_api_weather',
    default_args=default_args,
    # schedule='0 0,12 * * *',  # every 12 hours
    description='Data Mart layer: joined weather summary table'
)

create_table = ClickHouseOperator(
    task_id='create_table',
    sql=WeatherDataMartETL.create_table_schema_string(),
    clickhouse_conn_id='clickhouse_dm',
    dag=dag
)

load_weather_summary = ClickHouseOperator(
    task_id='load_weather_summary',
    sql=WeatherDataMartETL.generate_insert_sql(),
    clickhouse_conn_id='clickhouse_dm',
    dag=dag
)

optimize_data = ClickHouseOperator(
    task_id='optimize_data',
    sql="OPTIMIZE TABLE weather_summary FINAL;",
    clickhouse_conn_id='clickhouse_dm',
    dag=dag
)

# Set task dependencies
create_table >> load_weather_summary >> optimize_data
