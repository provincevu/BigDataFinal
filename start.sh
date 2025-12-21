#!/bin/bash
# =====================================================
# RETAIL BIG DATA PIPELINE - STARTUP SCRIPT (Linux/Mac)
# =====================================================

echo ""
echo "====================================================="
echo "   RETAIL BIG DATA PIPELINE - STARTING UP"
echo "====================================================="
echo ""

# Kiểm tra Docker
if ! docker info > /dev/null 2>&1; then
    echo "[ERROR] Docker is not running! Please start Docker first."
    exit 1
fi

echo "[INFO] Docker is running..."
echo ""

# Di chuyển dữ liệu
mkdir -p data
if [ -f "online_retail.csv" ]; then
    echo "[INFO] Moving online_retail.csv to data folder..."
    cp online_retail.csv data/
fi

# Tạo thư mục
echo "[INFO] Creating required directories..."
mkdir -p config spark-apps notebooks mongo-init hive-queries

echo ""
echo "[INFO] Starting Docker Compose services..."
echo "[INFO] This may take several minutes on first run..."
echo ""

# Start services
docker-compose up -d

echo ""
echo "[INFO] Waiting for services to be ready..."
sleep 30

echo ""
echo "====================================================="
echo "   SERVICES STARTED SUCCESSFULLY!"
echo "====================================================="
echo ""
echo "Access the following URLs:"
echo ""
echo "  📊 HDFS NameNode:        http://localhost:9870"
echo "  💾 HDFS DataNode:        http://localhost:9864"
echo "  ⚡ Spark Master:         http://localhost:8080"
echo "  ⚡ Spark Worker:         http://localhost:8081"
echo "  🌐 Hue (Web GUI):        http://localhost:8888"
echo "  📒 Jupyter Notebook:     http://localhost:8889"
echo "  🍃 MongoDB Express:      http://localhost:8082"
echo ""
echo "MongoDB Credentials:"
echo "  - Username: admin"
echo "  - Password: admin123"
echo ""
echo "====================================================="
echo ""
echo "[TIP] Run 'docker-compose logs -f' to view logs"
echo "[TIP] Run './stop.sh' to stop all services"
echo ""
