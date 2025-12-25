#!/bin/bash
echo ""
echo "====================================================="
echo "   RETAIL BIG DATA PIPELINE - KHỞI ĐỘNG DỊCH VỤ"
echo "====================================================="
echo ""

# Kiểm tra Docker
if ! docker info > /dev/null 2>&1; then
    echo "[ERROR] Docker không chạy! Vui lòng khởi động Docker trước."
    exit 1
fi

echo "[INFO] Docker đang chạy..."
echo ""

# Di chuyển dữ liệu
mkdir -p data
if [ -f "online_retail.csv" ]; then
    echo "[INFO] chuyển online_retail.csv đến thư mục data..."
    cp online_retail.csv data/
fi

# Tạo thư mục
echo "[INFO] Đang tạo các thư mục cần thiết..."
mkdir -p config spark-apps notebooks mongo-init hive-queries

echo ""
echo "[INFO] Bắt đầu chạy dịch vụ Docker Compose..."
echo "[INFO] Quá trình này có thể mất vài phút khi chạy lần đầu..."
echo ""

# Start services
docker-compose up -d

echo ""
echo "[INFO] Chờ dịch vụ sẵn sàng..."
sleep 30

echo ""
echo "====================================================="
echo "   DỊCH VỤ ĐÃ KHỞI ĐỘNG THÀNH CÔNG!"
echo "====================================================="
echo ""
echo "truy cập các URL sau:"
echo ""
echo "  HDFS NameNode:        http://localhost:9870"
echo "  HDFS DataNode 1:        http://localhost:9864"
echo "  HDFS DataNode 2:        http://localhost:9865"
echo "  Spark Master:         http://localhost:8080"
echo "  Spark Worker 1:         http://localhost:8081"
echo "  Spark Worker 2:         http://localhost:8083"
echo "  MongoDB Express:      http://localhost:8082"
echo ""
echo "Thông tin đăng nhập MongoDB:"
echo "  - Username: admin"
echo "  - Password: admin123"
echo ""
echo "====================================================="
echo ""
echo "[TIP] Run 'docker-compose logs -f' để xem logs"
echo "[TIP] Run './stop.sh' để dừng tất cả dịch vụ"
echo ""
