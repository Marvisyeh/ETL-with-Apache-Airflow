from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dw.api_get_weather_etl import GetWeatherDataETL


def transform(**context):
    result = context['ti'].xcom_pull(task_ids='read_weather_data')
    
    if result:
        print(f"Retrieved {len(result)} records from weather table")
        date_data, location_data, fact_data = GetWeatherDataETL.transform_weather_data(result)
        sql_dict = GetWeatherDataETL.generate_load_data_sql_string(date_data, location_data, fact_data)
        context['ti'].xcom_push(key='dim_time_insert_sql', value=sql_dict['dim_time_insert_sql'])
        context['ti'].xcom_push(key='dim_location_insert_sql', value=sql_dict['dim_location_insert_sql'])
        context['ti'].xcom_push(key='fact_weather_insert_sql', value=sql_dict['fact_weather_insert_sql'])
    else:
        raise ValueError("No data retrieved from ODS layer")

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
    dag_id='dw_api_weather',
    default_args=default_args,
    # schedule='0 0,12 * * *',  # every 2 hours
    description='daily weather data from ods'
)

create_table = ClickHouseOperator(
    task_id='create_table',
    sql=GetWeatherDataETL.create_table_schema_string(),
    clickhouse_conn_id='clickhouse_dw',
    dag=dag
)

read_weather_data = ClickHouseOperator(
    task_id='read_weather_data',
    database='ods',
    sql=GetWeatherDataETL.read_weather_data(),
    # ClickHouseOperator automatically pushes SELECT query results to XCom
    # The result can be pulled using: ti.xcom_pull(task_ids='read_weather_data')
    query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
    clickhouse_conn_id='clickhouse_ods',
    dag=dag
)

transform_weather_data_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform,
    dag=dag
)

load_dim_time_data_task = ClickHouseOperator(
    task_id='load_dim_time_data',
    sql="{{ ti.xcom_pull(task_ids='transform_weather_data', key='dim_time_insert_sql') }}",
    clickhouse_conn_id='clickhouse_dw',
    dag=dag
)

load_dim_location_data_task = ClickHouseOperator(
    task_id='load_dim_location_data',
    sql="{{ ti.xcom_pull(task_ids='transform_weather_data', key='dim_location_insert_sql') }}",
    clickhouse_conn_id='clickhouse_dw',
    dag=dag
)

load_fact_weather_data_task = ClickHouseOperator(
    task_id='load_fact_weather_data',
    sql="{{ ti.xcom_pull(task_ids='transform_weather_data', key='fact_weather_insert_sql') }}",
    clickhouse_conn_id='clickhouse_dw',
    dag=dag
)

optimize_data_task = ClickHouseOperator(
    task_id='optimize_data',
    sql=("OPTIMIZE TABLE dim_time FINAL;","OPTIMIZE TABLE dim_location FINAL;","OPTIMIZE TABLE fact_weather FINAL;"),
    clickhouse_conn_id='clickhouse_dw',
    dag=dag
)

trigger_dm_api_weather = TriggerDagRunOperator(
    task_id="trigger_dm_api_weather",
    trigger_dag_id="dm_api_weather",
    wait_for_completion=False,
    reset_dag_run=True,
    dag=dag
)

# Set task dependencies
create_table >> read_weather_data >> transform_weather_data_task >> [load_dim_time_data_task ,load_dim_location_data_task, load_fact_weather_data_task] >> optimize_data_task >> trigger_dm_api_weather