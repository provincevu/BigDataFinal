# 🛒 RETAIL BIG DATA PIPELINE

## Hệ Thống Phân Tích Dữ Liệu Bán Lẻ Sử Dụng Công Nghệ Big Data

---

## 📋 MỤC LỤC

1. [Tổng Quan Dự Án](#-tổng-quan-dự-án)
2. [Kiến Trúc Hệ Thống](#-kiến-trúc-hệ-thống)
3. [Công Nghệ Sử Dụng](#-công-nghệ-sử-dụng)
4. [Yêu Cầu Hệ Thống](#-yêu-cầu-hệ-thống)
5. [Hướng Dẫn Cài Đặt](#-hướng-dẫn-cài-đặt)
6. [Hướng Dẫn Sử Dụng](#-hướng-dẫn-sử-dụng)
7. [Mô Tả Dataset](#-mô-tả-dataset)
8. [Các Chức Năng Phân Tích](#-các-chức-năng-phân-tích)
9. [Cấu Trúc Thư Mục](#-cấu-trúc-thư-mục)
10. [Chi Tiết Các File](#-chi-tiết-các-file)
11. [Các Lệnh Hữu Ích](#-các-lệnh-hữu-ích)
12. [Xử Lý Lỗi Thường Gặp](#-xử-lý-lỗi-thường-gặp)
13. [Tài Liệu Tham Khảo](#-tài-liệu-tham-khảo)

---

## 🎯 TỔNG QUAN DỰ ÁN

### Mục đích

Xây dựng một **Data Pipeline** hoàn chỉnh để phân tích dữ liệu bán lẻ của doanh nghiệp, giúp:

- Phân tích doanh thu theo từng thời điểm (giờ, ngày, tháng, năm)
- Xác định sản phẩm bán chạy nhất
- Phân khúc khách hàng theo hành vi mua sắm
- Phân cụm khách hàng có hành vi tương tự (Clustering)
- Dự đoán xu hướng mua hàng

### Đặc điểm nổi bật

- ✅ **Đầy đủ stack Big Data**: Hadoop, Hive, Spark, MongoDB
- ✅ **Containerized**: Chạy hoàn toàn trên Docker, dễ triển khai
- ✅ **Interactive UI**: Hue, Jupyter Notebook, Mongo Express
- ✅ **Real analytics**: RFM Analysis, K-Means Clustering
- ✅ **Visualization**: Biểu đồ trực quan với Matplotlib/Seaborn

---

## 🏗️ KIẾN TRÚC HỆ THỐNG

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           USER INTERFACE                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Hue:8788   │  │ Jupyter:8889 │  │  HDFS:9870   │  │ Spark:8580   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
└─────────┼─────────────────┼─────────────────┼─────────────────┼─────────┘
          │                 │                 │                 │
┌─────────┼─────────────────┼─────────────────┼─────────────────┼─────────┐
│         │          DATA PROCESSING LAYER    │                 │         │
│         │                 │                 │                 │         │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐ │
│  │ Hive Server  │  │ Spark Master │  │   NameNode   │  │ Spark Worker │ │
│  │   :10000     │  │    :7077     │  │    :9000     │  │              │ │
│  └──────┬───────┘  └──────────────┘  └──────┬───────┘  └──────────────┘ │
│         │                                   │                           │
│  ┌──────▼───────┐                    ┌──────▼───────┐                   │
│  │Hive Metastore│                    │   DataNode   │                   │
│  │    :9083     │                    │    :9864     │                   │
│  └──────┬───────┘                    └──────────────┘                   │
└─────────┼───────────────────────────────────────────────────────────────┘
          │
┌─────────┼───────────────────────────────────────────────────────────────┐
│         │                   DATA STORAGE LAYER                          │
│  ┌──────▼───────┐                    ┌──────────────┐                   │
│  │  PostgreSQL  │                    │   MongoDB    │                   │
│  │ (Metastore)  │                    │   :27017     │                   │
│  │    :5432     │                    │              │                   │
│  └──────────────┘                    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow (Luồng Dữ Liệu)

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Raw CSV    │ ──▶  │    HDFS     │ ──▶  │   Spark     │
│  (Input)    │      │  (Storage)  │      │ (Processing)│
└─────────────┘      └─────────────┘      └──────┬──────┘
                                                  │
                     ┌────────────────────────────┼────────────────────────┐
                     │                            │                        │
                     ▼                            ▼                        ▼
              ┌─────────────┐             ┌─────────────┐          ┌─────────────┐
              │    Hive     │             │   MongoDB   │          │   Jupyter   │
              │ (Warehouse) │             │  (NoSQL)    │          │ (Analysis)  │
              └─────────────┘             └─────────────┘          └─────────────┘
```

---

## 🔧 CÔNG NGHỆ SỬ DỤNG

| Công nghệ                | Phiên bản | Vai trò                              |
| ------------------------ | --------- | ------------------------------------ |
| **Apache Hadoop (HDFS)** | 3.2.1     | Lưu trữ dữ liệu phân tán             |
| **Apache Hive**          | 2.3.2     | Data Warehouse, SQL queries          |
| **Apache Spark**         | 3.1.1     | Xử lý dữ liệu & Machine Learning     |
| **MongoDB**              | 5.0       | NoSQL database cho kết quả phân tích |
| **PostgreSQL**           | 9.5       | Metastore cho Hive                   |
| **Hue**                  | 4.10.0    | Web GUI cho Hadoop/Hive              |
| **Jupyter Notebook**     | Latest    | Interactive data analysis            |
| **Docker**               | 20.10+    | Container orchestration              |
| **Python**               | 3.9       | Scripting, ML algorithms             |

### Thư viện Python sử dụng

- **PySpark**: Xử lý dữ liệu phân tán
- **Pandas**: Data manipulation
- **Matplotlib/Seaborn**: Data visualization
- **Scikit-learn**: Machine Learning (K-Means)
- **PyMongo**: Kết nối MongoDB

---

## 💻 YÊU CẦU HỆ THỐNG

### Phần cứng

| Yêu cầu  | Tối thiểu   | Khuyến nghị |
| -------- | ----------- | ----------- |
| **RAM**  | 8 GB        | 16 GB       |
| **CPU**  | 4 cores     | 8 cores     |
| **Disk** | 20 GB trống | 50 GB trống |

### Phần mềm

- **Docker Desktop** (Windows/Mac) hoặc Docker Engine (Linux)
- **Docker Compose** v2.0+
- **Git** (optional)

### Hệ điều hành hỗ trợ

- ✅ Windows 10/11 (với WSL2)
- ✅ macOS 10.15+
- ✅ Ubuntu 20.04+
- ✅ CentOS 7+

---

## 🚀 HƯỚNG DẪN CÀI ĐẶT

### Bước 1: Chuẩn bị thư mục

```powershell
# Di chuyển đến thư mục project
cd D:\BigDataFinal
```

### Bước 2: Đảm bảo có file dữ liệu

Đảm bảo file `online_retail.csv` nằm trong thư mục `D:\BigDataFinal\`

### Bước 3: Khởi động hệ thống

**Windows (PowerShell):**

```powershell
# Cách 1: Sử dụng script
.\start.bat

# Cách 2: Docker Compose trực tiếp
docker-compose up -d
```

**Linux/Mac:**

```bash
chmod +x start.sh
./start.sh
```

### Bước 4: Kiểm tra services

```powershell
# Xem trạng thái các container
docker-compose ps

# Tất cả services phải ở trạng thái "Up"
```

### Bước 5: Chờ khởi động hoàn tất

⏱️ Lần đầu tiên có thể mất **3-5 phút** để download images và khởi động.

Kiểm tra logs:

```powershell
docker-compose logs -f hive-metastore
# Chờ thấy: "Starting hive metastore on port 9083"
```

---

## 📖 HƯỚNG DẪN SỬ DỤNG

### 🎯 Cách 1: Sử Dụng Jupyter Notebook (Đề xuất - Nhanh nhất)

1. **Mở trình duyệt**: http://localhost:8889
2. **Nhập token**: `bigdata2024`
3. **Mở notebook**: `simple_retail_analysis.ipynb`
4. **Chạy tất cả cells**: Menu → Run → Run All Cells
5. **Xem kết quả**: Biểu đồ hiển thị trực tiếp trong notebook

### 🎯 Cách 2: Chạy Spark ETL Pipeline

```powershell
# Bước 1: Upload dữ liệu lên HDFS
.\upload-data.bat

# Bước 2: Chạy ETL Pipeline
.\run-etl.bat

# Bước 3: Chạy Customer Clustering
.\run-clustering.bat
```

### 🎯 Cách 3: Sử Dụng Hue (SQL Queries)

1. **Mở trình duyệt**: http://localhost:8788
2. **Tạo tài khoản**: Lần đầu tự tạo (admin/admin)
3. **Vào Query Editor**: Chọn Hive
4. **Chạy queries**: Sử dụng các query trong `hive-queries/retail_analytics.sql`

---

## 📊 MÔ TẢ DATASET

### Nguồn dữ liệu

**Online Retail Dataset** - Dữ liệu giao dịch thực tế của một công ty bán lẻ online có trụ sở tại UK.

### Thông tin dataset

| Thuộc tính              | Giá trị                 |
| ----------------------- | ----------------------- |
| Số bản ghi              | 541,909                 |
| Số bản ghi sau làm sạch | ~397,884                |
| Thời gian               | 01/12/2010 - 09/12/2011 |
| Số khách hàng           | ~4,372                  |
| Số sản phẩm             | ~3,958                  |
| Số quốc gia             | 38                      |

### Cấu trúc các cột

| Cột           | Kiểu dữ liệu | Mô tả                                                 | Ví dụ                              |
| ------------- | ------------ | ----------------------------------------------------- | ---------------------------------- |
| `InvoiceNo`   | String       | Mã hóa đơn (6 chữ số). Nếu bắt đầu bằng 'C' = đơn hủy | 536365, C536379                    |
| `StockCode`   | String       | Mã sản phẩm (5 chữ số)                                | 85123A                             |
| `Description` | String       | Tên/mô tả sản phẩm                                    | WHITE HANGING HEART T-LIGHT HOLDER |
| `Quantity`    | Integer      | Số lượng mua                                          | 6                                  |
| `InvoiceDate` | Timestamp    | Ngày giờ giao dịch                                    | 2010-12-01 08:26:00                |
| `UnitPrice`   | Double       | Đơn giá (£)                                           | 2.55                               |
| `CustomerID`  | Integer      | Mã khách hàng (5 chữ số)                              | 17850                              |
| `Country`     | String       | Quốc gia của khách hàng                               | United Kingdom                     |

### Xử lý dữ liệu (Data Cleaning)

Pipeline tự động loại bỏ:

- ❌ Bản ghi thiếu CustomerID
- ❌ Quantity <= 0 (sản phẩm trả lại)
- ❌ UnitPrice <= 0
- ❌ InvoiceNo bắt đầu bằng 'C' (đơn hàng hủy)

---

## 📈 CÁC CHỨC NĂNG PHÂN TÍCH

### 1. 📊 Phân Tích Doanh Thu Theo Thời Gian

| Phân tích            | Mô tả                                    | Output      |
| -------------------- | ---------------------------------------- | ----------- |
| Theo tháng           | Doanh thu từng tháng, xu hướng tăng/giảm | Bar chart   |
| Theo ngày trong tuần | Ngày nào bán được nhiều nhất             | Bar chart   |
| Theo giờ             | Giờ cao điểm bán hàng                    | Line chart  |
| Theo quý             | So sánh doanh thu các quý                | Stacked bar |

**Insight mẫu**: Tháng 11 có doanh thu cao nhất do mua sắm cuối năm.

### 2. 🏆 Phân Tích Sản Phẩm Bán Chạy

| Phân tích             | Mô tả                                  |
| --------------------- | -------------------------------------- |
| Top 15 theo doanh thu | Sản phẩm đóng góp doanh thu cao nhất   |
| Top 10 theo số lượng  | Sản phẩm bán được nhiều nhất           |
| Top theo số đơn hàng  | Sản phẩm xuất hiện nhiều đơn hàng nhất |

### 3. 👥 Phân Tích RFM (Recency-Frequency-Monetary)

**RFM** là phương pháp phân khúc khách hàng dựa trên 3 yếu tố:

| Yếu tố        | Ý nghĩa           | Cách tính               |
| ------------- | ----------------- | ----------------------- |
| **R**ecency   | Mới mua gần đây?  | Số ngày từ lần mua cuối |
| **F**requency | Mua thường xuyên? | Số lần mua hàng         |
| **M**onetary  | Chi tiêu nhiều?   | Tổng số tiền đã chi     |

**Các phân khúc khách hàng:**

| Segment                   | Mô tả                | Chiến lược                |
| ------------------------- | -------------------- | ------------------------- |
| 💎 **Champions**          | R↑ F↑ M↑ - Khách VIP | Giữ chân, ưu đãi đặc biệt |
| ❤️ **Loyal Customers**    | F↑ M↑                | Upsell, cross-sell        |
| ⭐ **Potential Loyalist** | R↑ F↓ M↑             | Khuyến khích mua thêm     |
| 🆕 **Recent Customers**   | R↑                   | Chào đón, giới thiệu SP   |
| ⚠️ **At Risk**            | R↓ F↑                | Win-back campaign         |
| 😴 **Lost**               | R↓ F↓ M↓             | Reactivation email        |

### 4. 🎯 Phân Cụm Khách Hàng (K-Means Clustering)

Sử dụng thuật toán **K-Means** để nhóm khách hàng có hành vi tương tự:

**Quy trình:**

1. Chuẩn hóa dữ liệu RFM (StandardScaler)
2. Tìm số cluster tối ưu bằng Elbow Method
3. Áp dụng K-Means với K=4
4. Phân tích đặc điểm từng cluster

**Các cluster điển hình:**

- 🔴 **Cluster 0**: VIP Customers - Chi tiêu cao, mua thường xuyên
- 🔵 **Cluster 1**: Frequent Buyers - Mua nhiều lần, giá trị trung bình
- 🟢 **Cluster 2**: Regular Customers - Khách hàng bình thường
- 🟡 **Cluster 3**: Inactive Customers - Đã lâu không mua

### 5. 🌍 Phân Tích Theo Địa Lý

- Doanh thu theo quốc gia
- Số khách hàng theo vùng
- Thị trường tiềm năng ngoài UK

---

## 📁 CẤU TRÚC THƯ MỤC

```
BigDataFinal/
│
├── 📄 docker-compose.yml      # Cấu hình Docker orchestration
├── 📄 online_retail.csv       # Dataset gốc
├── 📄 README.md               # Tài liệu này
├── 📄 HUONG_DAN_SU_DUNG.md    # Hướng dẫn tiếng Việt
├── 📄 README_UI.md            # Hướng dẫn sử dụng Web UI
│
├── 📁 config/                 # Cấu hình các services
│   ├── hadoop.env             # Biến môi trường Hadoop
│   ├── hive-site.xml          # Cấu hình Hive
│   └── hue.ini                # Cấu hình Hue
│
├── 📁 spark-apps/             # Các ứng dụng Spark
│   ├── retail_etl_pipeline.py # ETL Pipeline chính
│   ├── customer_clustering.py # Phân cụm K-Means
│   └── product_recommendation.py # Hệ thống gợi ý
│
├── 📁 notebooks/              # Jupyter Notebooks
│   ├── retail_analysis.ipynb  # Phân tích với Spark
│   └── simple_retail_analysis.ipynb # Phân tích với Pandas
│
├── 📁 hive-queries/           # SQL queries cho Hive
│   └── retail_analytics.sql   # Các query phân tích
│
├── 📁 mongo-init/             # Khởi tạo MongoDB
│   └── init-mongo.js          # Script tạo collections
│
├── 📁 data/                   # Thư mục dữ liệu (mount vào containers)
│   └── output/                # Kết quả phân tích
│
├── 🔧 start.bat / start.sh    # Script khởi động
├── 🔧 stop.bat / stop.sh      # Script dừng
├── 🔧 upload-data.bat         # Upload data lên HDFS
├── 🔧 run-etl.bat             # Chạy ETL Pipeline
└── 🔧 run-clustering.bat      # Chạy Clustering
```

---

## 📝 CHI TIẾT CÁC FILE

### docker-compose.yml

Định nghĩa 11 services:

| Service              | Image                                     | Ports      | Mô tả              |
| -------------------- | ----------------------------------------- | ---------- | ------------------ |
| `namenode`           | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1 | 9870, 9000 | HDFS Master        |
| `datanode`           | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1 | 9864       | HDFS Worker        |
| `hive-metastore`     | bde2020/hive:2.3.2-postgresql-metastore   | 9083       | Hive Metadata      |
| `hive-server`        | bde2020/hive:2.3.2-postgresql-metastore   | 10000      | Hive Thrift Server |
| `postgres-metastore` | postgres:9.5                              | 5432       | Metastore DB       |
| `spark-master`       | bde2020/spark-master:3.1.1-hadoop3.2      | 8080, 7077 | Spark Master       |
| `spark-worker`       | bde2020/spark-worker:3.1.1-hadoop3.2      | 8081       | Spark Worker       |
| `mongodb`            | mongo:5.0                                 | 27017      | NoSQL Database     |
| `mongo-express`      | mongo-express:1.0.0-alpha                 | 8082       | MongoDB UI         |
| `hue`                | gethue/hue:4.10.0                         | 8888       | Hadoop Web UI      |
| `jupyter`            | jupyter/pyspark-notebook                  | 8889       | Notebook Server    |

### config/hive-site.xml

Cấu hình quan trọng:

- Kết nối PostgreSQL metastore
- Thrift URI cho remote metastore
- Timeout settings để tránh lỗi khi xử lý dữ liệu lớn
- Authentication và transport mode

### spark-apps/retail_etl_pipeline.py

Pipeline ETL gồm các bước:

1. `create_spark_session()` - Khởi tạo Spark với Hive & MongoDB
2. `load_and_clean_data()` - Load CSV, làm sạch dữ liệu
3. `save_to_hdfs()` - Lưu vào HDFS dạng Parquet
4. `create_hive_tables()` - Tạo bảng trong Hive
5. `analyze_revenue_by_time()` - Phân tích doanh thu
6. `analyze_top_products()` - Top sản phẩm
7. `analyze_customer_behavior()` - Phân tích RFM
8. `save_to_mongodb()` - Lưu kết quả vào MongoDB

### notebooks/simple_retail_analysis.ipynb

Notebook phân tích trực tiếp bằng Pandas (không cần Spark):

- Nhanh hơn, dễ debug
- Trực quan hóa ngay trong notebook
- Phù hợp cho demo và học tập

---

## 🌐 TRUY CẬP SERVICES

| Service                 | URL                   | Dang nhap                |
| ----------------------- | --------------------- | ------------------------ |
| :chart_with_upwards_trend: **Jupyter Notebook** | http://localhost:8889 | Token: xem docker logs jupyter |
| :file_folder: **HDFS NameNode**    | http://localhost:9870 | Khong can                |
| :zap: **Spark Master UI**  | http://localhost:8580 | Khong can                |
| :mag: **Hue**              | http://localhost:8788 | Tao lan dau: admin/admin |
| :leaves: **Mongo Express**    | http://localhost:8290 | admin / admin123         |
| :package: **HDFS DataNode**    | http://localhost:9864 | Khong can                |
| :wrench: **Spark Worker**     | http://localhost:8581 | Khong can                |

---

## 💡 CÁC LỆNH HỮU ÍCH

### Docker

```powershell
# Khởi động tất cả services
docker-compose up -d

# Dừng tất cả services
docker-compose down

# Xem logs của service cụ thể
docker-compose logs -f spark-master

# Restart một service
docker-compose restart hive-server

# Vào container
docker exec -it spark-master bash

# Xem trạng thái
docker-compose ps

# Xóa hoàn toàn (bao gồm data)
docker-compose down -v
```

### HDFS

```powershell
# Liệt kê files
docker exec namenode hdfs dfs -ls /user/retail

# Xem dung lượng
docker exec namenode hdfs dfs -du -h /user

# Upload file
docker exec namenode hdfs dfs -put /data/file.csv /user/retail/

# Download file
docker exec namenode hdfs dfs -get /user/retail/file.csv /data/

# Xóa file
docker exec namenode hdfs dfs -rm -r /user/retail/test
```

### Hive

```powershell
# Vào Hive CLI
docker exec -it hive-server hive

# Chạy query
docker exec hive-server hive -e "SHOW DATABASES;"
docker exec hive-server hive -e "USE retail_db; SHOW TABLES;"
docker exec hive-server hive -e "SELECT COUNT(*) FROM retail_db.transactions;"
```

### Spark

```powershell
# Submit job
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    /spark-apps/retail_etl_pipeline.py

# Spark Shell (Scala)
docker exec -it spark-master /spark/bin/spark-shell

# PySpark Shell
docker exec -it spark-master /spark/bin/pyspark
```

### MongoDB

```powershell
# Vào Mongo Shell
docker exec -it mongodb mongosh -u admin -p admin123

# Xem databases
docker exec mongodb mongosh -u admin -p admin123 --eval "show dbs"

# Query collection
docker exec mongodb mongosh -u admin -p admin123 --eval "use retail_analytics; db.customer_segments.find().limit(5)"
```

---

## ⚠️ XỬ LÝ LỖI THƯỜNG GẶP

### 1. Services không khởi động được

```powershell
# Kiểm tra logs
docker-compose logs

# Restart với clean state
docker-compose down -v
docker-compose up -d
```

### 2. Hive Metastore không kết nối

```powershell
# Chờ metastore khởi động hoàn tất
docker-compose logs hive-metastore

# Restart Hive services
docker-compose restart postgres-metastore hive-metastore hive-server
```

### 3. Spark job fails với "Out of Memory"

Tăng memory trong docker-compose.yml:

```yaml
spark-master:
  environment:
    - SPARK_DRIVER_MEMORY=2g
    - SPARK_EXECUTOR_MEMORY=2g
```

### 4. Jupyter không kết nối được

```powershell
docker-compose restart jupyter
# Chờ 30 giây rồi truy cập lại
```

### 5. Lỗi "hive.metastore.fastpath"

Đảm bảo file `config/hive-site.xml` KHÔNG có property:

```xml
<!-- ĐÃ XÓA - KHÔNG DÙNG PROPERTY NÀY -->
<property>
    <name>hive.metastore.fastpath</name>
    <value>true</value>
</property>
```

### 6. Timeout khi tạo Hive tables

Đảm bảo có các timeout settings trong `hive-site.xml`:

```xml
<property>
    <name>hive.metastore.client.socket.timeout</name>
    <value>600s</value>
</property>
```

---

## 📚 TÀI LIỆU THAM KHẢO

### Công nghệ

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Hive Documentation](https://cwiki.apache.org/confluence/display/Hive/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MongoDB Manual](https://www.mongodb.com/docs/manual/)
- [Hue User Guide](https://docs.gethue.com/)

### Thuật toán

- [RFM Analysis](<https://en.wikipedia.org/wiki/RFM_(market_research)>)
- [K-Means Clustering](https://scikit-learn.org/stable/modules/clustering.html#k-means)

### Dataset

- [Online Retail Dataset - UCI](https://archive.ics.uci.edu/ml/datasets/online+retail)

---

## 👨‍💻 TÁC GIẢ

**Email**: tinhvu2k4@gmail.com

---

## 📄 LICENSE

MIT License - Có thể sử dụng cho mục đích học tập và nghiên cứu.

---

**Last Updated**: December 2025
