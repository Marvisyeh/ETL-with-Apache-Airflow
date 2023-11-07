from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG


import os
import json
from datetime import timedelta

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

filedir = r'./data'
if not os.path.exists(filedir):
    os.mkdir(filedir)


def get_apidata():

    url = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001'
    payload = {
        'Authorization': os.getenv('Authorization'),
        'format': 'json',
        'locationName': '臺北市'
    }
    res = requests.get(url=url, params=payload)
    print(res.text)
    if res.status_code == 200:
        weather = res.json()['records']
        with open(f'{filedir}/weather.json', 'w', encoding='utf8') as json_file:
            json.dump(weather, json_file, indent=6, ensure_ascii=False)
    else:
        None


def transform():
    with open(r'./data/weather.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    result = pd.DataFrame()
    temp = data['location'][0]['weatherElement']
    for i in temp:
        df = pd.json_normalize(i['time'])
        df = df.iloc[:, [0, 2]]
        df.columns = ['startTime', i['elementName']]
        result = pd.concat([result, df], axis=1)
    result = result.iloc[:, [0, 1, 3, 5, 7, 9]]

    with open(r'./data/weather.sql', 'w') as f:
        f.write(
            "DELETE FROM weather WHERE startTime IN ( '"
            + "','".join(result.startTime)
            + "' );"
        )
        f.write(
            "INSERT INTO weather VALUES "
            f"{','.join([str(tuple(col)) for col in result.values])};"
        )


default_args = {
    'owner': 'marvis',
    'start_date': days_ago(0),
    'email': ['myemail@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='daily weather message'
)


extract = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=get_apidata,
    dag=dag
)

transformer = PythonOperator(
    task_id='transform_data_use_pandas',
    bash_command=transform,
    dag=dag
)

load = PostgresOperator(
    task_id="write_to_sqlserever",
    postgres_conn_id="my_sqlserver",
    sql=r"./data/weather.sql",
    database="master",
    dag=dag,
)


extract >> transformer >> load
