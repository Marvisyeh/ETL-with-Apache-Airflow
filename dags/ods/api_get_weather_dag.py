from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ods.api_get_weather_etl import GetWeatherDataAPI


def extract(**context):
    run_id = context['dag_run'].run_id
    execution_date = context['ds']
    etl = GetWeatherDataAPI()
    file_path = etl.get_weather_data(execution_date)
    extract_result = {
      'file_path': file_path,
      'run_id': run_id,
      'execution_date': execution_date
    }
    context['ti'].xcom_push(key='extract_result', value=extract_result)


def transform(**context):
    extract_result = context['ti'].xcom_pull(task_ids='extract_data_from_api', key='extract_result')
    
    if extract_result['run_id'] != context['dag_run'].run_id:
        raise ValueError(f"Run ID mismatch: {extract_result['run_id']} != {context['dag_run'].run_id}")
    
    file_path = extract_result['file_path']
    execution_date = extract_result['execution_date']
    
    etl = GetWeatherDataAPI()
    
    sql_dict = etl.transform_weather_data(
        file_path=file_path, 
        execution_date=execution_date
    )

    context['ti'].xcom_push(key='insert_sql', value=sql_dict['insert_sql'])
    context['ti'].xcom_push(key='cleanup_sql', value=sql_dict['cleanup_sql'])



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
    dag_id='ods_api_weather',
    default_args=default_args,
    schedule='0 0,6 * * *',  # every 6 hours
    description='daily weather data from api'
)

create_table = ClickHouseOperator(
    task_id='create_table',
    sql=GetWeatherDataAPI.create_table_schema_string(),
    clickhouse_conn_id='clickhouse_ods',
    dag=dag
)

extract = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=extract,
    dag=dag
)

transformer = PythonOperator(
    task_id='transform_data_use_pandas',
    python_callable=transform,
    dag=dag
)

load = ClickHouseOperator(
    task_id="write_to_clickhouse",
    sql="{{ ti.xcom_pull(task_ids='transform_data_use_pandas', key='insert_sql') }}",
    # sql="SELECT version();",
    clickhouse_conn_id='clickhouse_ods',
    dag=dag
)

cleanup = ClickHouseOperator(
    task_id="cleanup_duplicate_records",
    sql="{{ ti.xcom_pull(task_ids='transform_data_use_pandas', key='cleanup_sql') }}",
    clickhouse_conn_id='clickhouse_ods',
    dag=dag
)

trigger_dw_api_weather = TriggerDagRunOperator(
    task_id="trigger_dw_api_weather",
    trigger_dag_id="dw_api_weather",
    wait_for_completion=False,
    reset_dag_run=True,
    dag=dag
)

create_table >> extract >> transformer >> load >> cleanup >> trigger_dw_api_weather