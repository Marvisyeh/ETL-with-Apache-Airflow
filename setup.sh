#!/bin/bash

# Apache Airflow 初始化設置
# 此腳本用於在第一次啟動前準備必要的目錄和配置文件
# 參考: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

set -e # 遇到錯誤時退出

echo "=========================================="
echo "Apache Airflow 環境初始化"
echo "=========================================="
echo ""

# 檢查 Docker 是否安裝
if ! command -v docker &> /dev/null; then
    echo "錯誤: Docker 未安裝"
    echo "請先安裝 Docker Community Edition (CE)"
    echo "參考: https://docs.docker.com/get-docker/"
    exit 1
fi

# 檢查 Docker Compose 是否安裝
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "錯誤: Docker Compose 未安裝"
    echo "請安裝 Docker Compose v2.14.0 或更新版本"
    exit 1
fi

echo "Docker 環境檢查通過"
echo ""

# 檢查必要文件
echo "檢查項目文件..."
if [ ! -f Dockerfile ]; then
    echo "   警告: 未找到 Dockerfile，將使用官方預設鏡像"
    echo "   建議創建 Dockerfile 以安裝自定義依賴"
else
    echo "   找到 Dockerfile"
fi

if [ ! -f requirements.txt ]; then
    echo "   警告: 未找到 requirements.txt"
    echo "   建議創建 requirements.txt 以管理 Python 依賴"
else
    echo "   找到 requirements.txt"
fi
echo ""

# 創建必要的目錄
echo "創建必要的目錄..."
mkdir -p ./dags ./logs ./plugins ./config ./dags/temp_data
echo "   已創建: dags/, logs/, plugins/, config/, dags/data/"
echo ""

# 創建 .env 文件（如果不存在）
# 根據官方文檔: Setting the right Airflow user
# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user
if [ ! -f .env ]; then
    echo "創建 .env 文件..."
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    echo "   .env 文件已創建，AIRFLOW_UID=$(id -u)"
else
    echo "檢查 .env 文件..."
    # 如果 .env 存在但沒有 AIRFLOW_UID，則添加
    if ! grep -q "AIRFLOW_UID" .env; then
        echo -e "AIRFLOW_UID=$(id -u)" >> .env
        echo "   已添加 AIRFLOW_UID 到現有 .env 文件"
    else
        echo "   .env 文件已包含 AIRFLOW_UID"
    fi
fi


# 步驟 2: 初始化 airflow.cfg (可選)
# 根據官方文檔: Initialize airflow.cfg (Optional)
# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#initialize-airflow-cfg-optional
echo "初始化 airflow.cfg (可選)..."
if [ -t 0 ]; then
    # 交互式環境
    read -p "是否要初始化 airflow.cfg 配置文件？(y/N): " -n 1 -r
    echo ""
    INIT_CFG=$REPLY
else
    # 非交互式環境，預設跳過
    INIT_CFG="n"
    echo "   非交互式環境，跳過 airflow.cfg 初始化"
fi

if [[ $INIT_CFG =~ ^[Yy]$ ]]; then
    echo "   正在初始化 airflow.cfg..."
    # 使用 docker compose 或 docker-compose
    if docker compose version &> /dev/null; then
        docker compose run airflow-cli airflow config list > /dev/null 2>&1 || true
    else
        docker-compose run airflow-cli airflow config list > /dev/null 2>&1 || true
    fi
    echo "   airflow.cfg 已初始化"
else
    echo "   跳過 airflow.cfg 初始化"
fi
echo ""

# 步驟 3: 初始化資料庫（必需）
# 根據官方文檔: Initialize the database
# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#initialize-the-database
echo "初始化資料庫（必需）..."
echo "   這將執行資料庫遷移並創建第一個管理員帳號"
echo "   這可能需要幾分鐘時間，請耐心等待..."
echo ""

# 使用 docker compose 或 docker-compose
if docker compose version &> /dev/null; then
    docker compose up airflow-init
else
    docker-compose up airflow-init
fi

echo ""
echo "=========================================="
echo "初始化完成！"
echo "=========================================="
echo ""
echo "已創建的目錄："
ls -ld ./dags ./logs ./plugins ./config 2>/dev/null | awk '{print "   " $9}' || echo "   (目錄已存在)"
echo ""
echo ".env 文件內容："
cat .env | sed 's/^/   /'
echo ""
echo "=========================================="
echo "下一步操作："
echo "=========================================="
echo ""
echo "啟動所有 Airflow 服務："
if docker compose version &> /dev/null; then
    echo "   docker compose up -d"
else
    echo "   docker-compose up -d"
fi
echo ""
echo "2. 查看服務狀態："
if docker compose version &> /dev/null; then
    echo "   docker compose ps"
else
    echo "   docker-compose ps"
fi
echo ""
echo "3. 訪問 Airflow Web UI："
echo "   http://localhost:8080"
echo "   預設帳號: airflow"
echo "   預設密碼: airflow"
echo ""
echo "4. 查看日誌："
if docker compose version &> /dev/null; then
    echo "   docker compose logs -f"
else
    echo "   docker-compose logs -f"
fi
echo ""
echo "更多資訊請參考："
echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html"
echo ""
