# HƯỚNG DẪN SỬ DỤNG - Big Data Retail Analytics

## 1. Khởi động hệ thống

```bash
# Windows
start.bat

# Linux/Mac
./start.sh
```

Đợi khoảng 3-5 phút để tất cả services khởi động.

---

## 2. Kiểm tra hệ thống

```bash
docker ps
```

Phải có 12 containers đang chạy (namenode, datanode, spark-master, spark-worker, mongodb, mongo-express, hive-metastore, hive-server, hue, jupyter, postgres-metastore, webapp).

---

## 3. Upload dữ liệu lên HDFS

```bash
upload-data.bat
```

Hoặc thủ công:
```bash
docker exec namenode hdfs dfs -mkdir -p /user/retail
docker exec namenode hdfs dfs -put -f /data/online_retail.csv /user/retail/
```

Kiểm tra:
```bash
docker exec namenode hdfs dfs -ls /user/retail/
```

---

## 4. Chạy ETL Pipeline

```bash
run-etl.bat
```

Hoặc thủ công (sử dụng phiên bản đơn giản, nhanh hơn):
```bash
docker exec spark-master /spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 /spark-apps/retail_etl_simple.py
```

Đợi khoảng 2-5 phút để hoàn thành.

---

## 5. Chạy Customer Clustering

```bash
run-clustering.bat
```

Hoặc thủ công:
```bash
docker exec spark-master /spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 /spark-apps/customer_clustering_simple.py
```

---

## 6. Khởi động Web Application

```bash
run-webapp.bat
```

Hoặc thủ công:
```bash
docker-compose up -d --build webapp
```

---

## 7. Truy cập giao diện

| Dịch vụ | URL |
|---------|-----|
| **Web Dashboard** | http://localhost:5000 |
| HDFS NameNode UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8580 |
| Hue | http://localhost:8788 |
| Jupyter Notebook | http://localhost:8889 |
| MongoDB Express | http://localhost:8290 |

---

## 8. Kiểm tra dữ liệu MongoDB

```bash
docker exec mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.getSiblingDB('retail_analytics').getCollectionNames()"
```

---

## 9. Dừng hệ thống

```bash
# Windows
stop.bat

# Linux/Mac
./stop.sh
```

---

## 10. Xử lý lỗi thường gặp

### Lỗi port đã sử dụng
```bash
stop.bat
docker-compose down -v
start.bat
```

### Lỗi Hive Metastore
```bash
docker restart hive-metastore
docker restart hive-server
```

### Webapp hiển thị "No Data"
- Đảm bảo đã chạy ETL Pipeline (bước 4)
- Đảm bảo đã chạy Clustering (bước 5)
- Restart webapp: `docker restart webapp`

---

## 11. Quy trình hoàn chỉnh (tóm tắt)

```bash
start.bat                 # Bước 1: Khởi động
# Đợi 3-5 phút
upload-data.bat           # Bước 2: Upload data
run-etl.bat               # Bước 3: Chạy ETL
# Đợi 2-5 phút
run-clustering.bat        # Bước 4: Clustering
run-webapp.bat            # Bước 5: Web app
# Truy cập http://localhost:5000
```

---

## 12. Thông tin kết quả phân tích

Sau khi chạy xong pipeline, dữ liệu sẽ được lưu vào MongoDB với các collections:

| Collection | Mô tả |
|------------|-------|
| `customer_rfm` | Phân tích RFM cho từng khách hàng |
| `customer_clusters` | Kết quả phân cụm khách hàng |
| `cluster_statistics` | Thống kê theo từng phân khúc |
| `transactions` | Mẫu giao dịch (10,000 records) |
| `country_performance` | Hiệu suất theo quốc gia |
| `monthly_revenue` | Doanh thu theo tháng |
| `daily_revenue` | Doanh thu theo ngày trong tuần |
| `hourly_revenue` | Doanh thu theo giờ |
| `top_products_quantity` | Top sản phẩm theo số lượng |
| `top_products_revenue` | Top sản phẩm theo doanh thu |

---

## 13. Yêu cầu hệ thống

- **Docker Desktop** phiên bản 20.10+
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB)
- **Disk**: Tối thiểu 10GB trống
- **Ports**: 5000, 8290, 8580, 8788, 8889, 9870 (phải trống)
