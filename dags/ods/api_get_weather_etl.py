"""
https://opendata.cwa.gov.tw/userLogin
"""

import json
import requests
import pandas as pd

from configs.settings import OpendataGovTWConfig, PathsConfig

class GetWeatherDataAPI:
    def __init__(self):
        self.url = OpendataGovTWConfig.WEATHER_URL
        self.api_key = OpendataGovTWConfig.WEATHER_API_KEY
        self.temp_data_path = PathsConfig.TEMP_DATA_PATH
        self.payload = {
            'Authorization': self.api_key,
            'format': 'json',
            'locationName': ['臺北市', '新北市','基隆市']
        }
        
    def get_weather_data(self, execution_date):
        response = requests.get(self.url, params=self.payload)
        file_path = f'{self.temp_data_path}/weather_{execution_date}.json'
        if response.status_code == 200:
            
            weather = response.json()['records']
            with open(file_path, 'w', encoding='utf8') as json_file:
                json.dump(weather, json_file, indent=6, ensure_ascii=False)
                return file_path
        else:
            raise ValueError(f"Failed to get weather data: {response.status_code}")

    def transform_weather_data(self, file_path, execution_date):
        with open(file_path, 'r', encoding='utf-8') as f:
            weather_data = json.load(f)
        result = pd.DataFrame()

        for location in weather_data['location']:
            temp_df = pd.DataFrame()
            temp = location['weatherElement']
            for i in temp:
                df = pd.json_normalize(i['time'])
                df.columns = [col.replace('parameter.parameter',i['elementName']) for col in df.columns]
                temp_df = pd.concat([temp_df, df], axis=1)
            temp_df['locationName'] = location['locationName']
            result = pd.concat([result, temp_df], axis=0)
        
        # Remove duplicate columns (keep first occurrence)
        # This handles cases where startTime, endTime appear multiple times
        result = result.loc[:, ~result.columns.duplicated(keep='first')]
        result = result[['startTime', 'endTime', 'locationName', 'WxName', 'WxValue', 'PoPName', 'PoPUnit', 'MinTName', 'MinTUnit', 'MaxTName', 'MaxTUnit', 'CIName']]

        insert_sql = (
            "INSERT INTO weather (startTime, endTime, locationName, WxName, WxValue, PoPName, PoPUnit, MinTName, MinTUnit, MaxTName, MaxTUnit, CIName) VALUES "
            f"{','.join([str(tuple(col)) for col in result.values])}"
        )
        
        # Generate cleanup SQL using OPTIMIZE FINAL
        # This forces immediate merge and deduplication based on ReplacingMergeTree
        # It will keep records with the latest updated_at for each (startTime, locationName)
        cleanup_sql = "OPTIMIZE TABLE weather FINAL"
        
        file_path = f'{self.temp_data_path}/weather_{execution_date}.sql'
        with open(file_path, 'w', encoding='utf8') as f:
            f.write(insert_sql)
        
        return {'insert_sql': insert_sql, 'cleanup_sql': cleanup_sql}
    
    @staticmethod
    def create_table_schema_string():
        return """CREATE TABLE IF NOT EXISTS weather (
            startTime DateTime,
            endTime DateTime,
            locationName VARCHAR(50),
            WxName VARCHAR(50),
            WxValue VARCHAR(50),
            PoPName VARCHAR(50),
            PoPUnit VARCHAR(50),
            MinTName VARCHAR(50),
            MinTUnit VARCHAR(50),
            MaxTName VARCHAR(50),
            MaxTUnit VARCHAR(50),
            CIName VARCHAR(50),
            updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (startTime, locationName)
        PRIMARY KEY (startTime, locationName);
        """
    
if __name__ == "__main__":
    etl = GetWeatherDataAPI()
    file_path = etl.get_weather_data('2026-01-16')
    result = etl.transform_weather_data(file_path,'2026-01-16')
    print(result)
    print("Successfully transformed weather data")