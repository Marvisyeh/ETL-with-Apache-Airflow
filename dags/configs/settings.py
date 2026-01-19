import os

class OpendataGovTWConfig:
    WEATHER_URL = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001'
    WEATHER_API_KEY = os.getenv('AUTHORIZATION')

class PathsConfig:
    TEMP_DATA_PATH = r'/opt/airflow/dags/temp_data'

