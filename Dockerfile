# 使用官方 Airflow 鏡像作為基礎鏡像
# 参考: https://airflow.apache.org/docs/docker-stack/build.html
ARG AIRFLOW_VERSION=3.1.6
FROM apache/airflow:${AIRFLOW_VERSION}

# 切換到 root 用戶以安裝系統依賴
USER root

# 安裝系統級依賴（如果需要）
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     <package-name> \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# 切換回 airflow 用戶
USER airflow

# 複製 requirements.txt 到容器中
COPY requirements.txt /requirements.txt

# 安装 Python 依赖
# 使用 --user 標誌確保包安裝在用戶目錄中
RUN pip install --no-cache-dir --user -r /requirements.txt

# 設置工作目錄
WORKDIR /opt/airflow
