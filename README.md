# ETL Pipeline with Apache Airflow
![](https://img.shields.io/badge/Python-3776AB?style=Social&logo=python&logoColor=white)
![](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=Social&logo=Apache%20Airflow&logoColor=white)


 
這個專案使用Apache Airflow建立一個ETL管道。主要目標是從中央氣象署接取36小時的天氣預報、轉換成需求的格式，然後寫入PostgresSQL資料庫中。這個管道毀以每六個小時更新最新資訊。


#### 資料來源
資料來源是氣象資料開放平臺[一般天氣預報-今明36小時天氣預報](https://opendata.cwb.gov.tw/dataset/all/F-C0032-001)，來源中會包含各縣市在未來36小時內逐 12 小時的天氣現象、最高氣溫、最低氣溫、降雨機率及舒適度指數。以API的形式串接JSON格式資料。


#### 資料管線設計
```mermaid 
graph LR
PythonOperator:extract --> PythonOperator:transformer --> PostgresOperator:load

```

管線包含以下Task:
1. extract_data_from_api: <br>
    使用Python Operator，向API請求台北市天氣資訊，並將資料存成Json檔案。

2. transform_data_use_pandas： <br>
    一樣使用Python Operator，將檔案整理成需要的格式，最後寫成SQL script。

3. write_to_sqlserever： <br>
    Postgres Operator，執行SQL script 將資料更新到資料庫中。
