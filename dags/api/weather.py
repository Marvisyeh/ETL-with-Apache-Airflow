"""
https://opendata.cwa.gov.tw/userLogin
"""

import os
import time
import requests
import pandas as pd

from configs.settings import OpendataGovTWConfig, PathsConfig

class GetWeatherDataAPI:
    def __init__(self, location_name=['臺北市', '新北市','基隆市']):
        self.url = OpendataGovTWConfig.WEATHER_URL
        self.api_key = OpendataGovTWConfig.WEATHER_API_KEY
        self.temp_data_path = PathsConfig.TEMP_DATA_PATH
        self.payload = {
            'Authorization': self.api_key,
            'format': 'json',
            'locationName': location_name
        }
    
    def get_weather_data(self, execution_date):
        """
        Get weather data from API and save to parquet file
        Args:
            execution_date: str, format: YYYY-MM-DD
        Returns:
            file_path: str, path to the parquet file
        """
        weather_data = self.get_weather_api()
        df = self.transform_weather_data(weather_data)
        file_path = self.save_weather_data(df, execution_date)
        return file_path
    
    def save_weather_data(self, df, execution_date):
        YYYY = execution_date.split('-')[0]
        file_path = f'{self.temp_data_path}/api/weather/{YYYY}/{execution_date}.parquet'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_parquet(file_path)
        return file_path
    
    def transform_weather_data(self, weather_data):
        if not weather_data or 'location' not in weather_data:
            return None
        df = pd.json_normalize(
            weather_data['location'],
            'weatherElement',
            ['locationName']
        )
        df = df.explode('time').reset_index(drop=True)
        df = pd.concat([df.drop('time', axis=1), pd.json_normalize(df['time'])], axis=1)
        df.columns = [col.replace('parameter.', '') for col in df.columns]
        print(df.columns)
        return df

    ## TODO 新增 retry 機制
    def get_weather_api(self):
        try:
          response = requests.get(self.url, params=self.payload)
          if response.status_code == 200:
              
              weather = response.json()['records']
              return weather
          else:
              return None
        except Exception as e:
          return None
    
if __name__ == "__main__":
    etl = GetWeatherDataAPI(location_name=['臺北市'])
    file_path = etl.get_weather_data('2026-01-16')
    print(file_path)
