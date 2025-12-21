# 🖥️ Hướng dẫn Sử dụng Giao diện Web - Retail Big Data Pipeline

## 📋 Mục lục

- [Tổng quan các Web UI](#tổng-quan-các-web-ui)
- [1. Hadoop HDFS Web UI](#1-hadoop-hdfs-web-ui)
- [2. Apache Spark Web UI](#2-apache-spark-web-ui)
- [3. Hue - Hadoop User Experience](#3-hue---hadoop-user-experience)
- [4. Jupyter Notebook](#4-jupyter-notebook)
- [5. MongoDB Express](#5-mongodb-express)
- [Workflow thực hành](#workflow-thực-hành)

---

## 🌐 Tổng quan các Web UI

Sau khi khởi động hệ thống bằng `start.bat`, bạn có thể truy cập các giao diện web sau:

| Service                 | URL                   | Mô tả                        | Credentials           |
| ----------------------- | --------------------- | ---------------------------- | --------------------- |
| 📂 **HDFS NameNode**    | http://localhost:9870 | Quản lý file system phân tán | Không cần             |
| 📦 **HDFS DataNode**    | http://localhost:9864 | Thông tin node lưu trữ       | Không cần             |
| ⚡ **Spark Master**     | http://localhost:8080 | Quản lý Spark cluster        | Không cần             |
| ⚡ **Spark Worker**     | http://localhost:8081 | Thông tin worker node        | Không cần             |
| 🎨 **Hue**              | http://localhost:8888 | GUI cho Hadoop/Hive          | Tạo tài khoản lần đầu |
| 📓 **Jupyter Notebook** | http://localhost:8889 | Interactive Python/Spark     | Token (xem hướng dẫn) |
| 🍃 **MongoDB Express**  | http://localhost:8082 | Quản lý MongoDB              | admin / admin123      |

---

## 1. Hadoop HDFS Web UI

### 🔗 Truy cập: http://localhost:9870

### Mô tả

Giao diện quản lý Hadoop Distributed File System (HDFS) - hệ thống file phân tán.

### Các tính năng chính

#### 📊 Overview (Trang chủ)

- **Cluster Summary**: Tổng quan về cluster (dung lượng, số node, trạng thái)
- **NameNode Status**: Trạng thái của NameNode
- **Capacity Used**: Phần trăm dung lượng đã sử dụng

![HDFS Overview](docs/hdfs-overview.png)

#### 📁 Utilities > Browse the file system

**Đường dẫn**: Menu **Utilities** → **Browse the file system**

Cho phép bạn:

- Duyệt cấu trúc thư mục trên HDFS
- Xem thông tin file (kích thước, quyền, thời gian tạo)
- Download file về máy local
- Xem nội dung file text

**Các thư mục quan trọng:**

```
/user/retail/          → Dữ liệu raw (online_retail.csv)
/user/hive/warehouse/  → Hive tables
/user/retail/processed_data/ → Dữ liệu đã xử lý
```

#### 📈 Datanodes

Xem danh sách và trạng thái các DataNode trong cluster.

### Cách sử dụng

1. **Xem file đã upload:**

   - Vào **Utilities** → **Browse the file system**
   - Navigate đến `/user/retail/`
   - Click vào `online_retail.csv` để xem thông tin

2. **Download file:**
   - Tìm đến file cần download
   - Click vào tên file
   - Click nút **Download**

---

## 2. Apache Spark Web UI

### 🔗 Truy cập: http://localhost:8080 (Master) | http://localhost:8081 (Worker)

### Mô tả

Giao diện quản lý Apache Spark cluster, theo dõi jobs và resources.

### Spark Master UI (Port 8080)

#### 📊 Trang chủ

- **Workers**: Danh sách worker nodes
- **Running Applications**: Các ứng dụng đang chạy
- **Completed Applications**: Các ứng dụng đã hoàn thành

#### 🔍 Thông tin hiển thị

| Mục           | Mô tả                                        |
| ------------- | -------------------------------------------- |
| URL           | Spark Master URL (spark://spark-master:7077) |
| Alive Workers | Số worker đang hoạt động                     |
| Cores in use  | Số CPU cores đang sử dụng                    |
| Memory in use | RAM đang sử dụng                             |

### Spark Worker UI (Port 8081)

#### 📊 Thông tin Worker

- **Cores**: Số CPU cores của worker
- **Memory**: RAM available
- **Running Executors**: Các executor đang chạy

### Cách sử dụng

1. **Theo dõi Spark Job:**

   - Chạy ETL pipeline: `run-etl.bat`
   - Truy cập http://localhost:8080
   - Xem job trong **Running Applications**
   - Click vào Application ID để xem chi tiết

2. **Xem Spark Application UI:**
   - Khi có job đang chạy, truy cập http://localhost:4040
   - Xem **Jobs**, **Stages**, **Storage**, **Environment**

---

## 3. Hue - Hadoop User Experience

### 🔗 Truy cập: http://localhost:8888

### Mô tả

Giao diện web toàn diện để tương tác với Hadoop ecosystem (HDFS, Hive, Spark).

### Thiết lập lần đầu

1. Truy cập http://localhost:8888
2. Tạo tài khoản admin:
   - **Username**: `admin`
   - **Password**: `admin123` (hoặc tùy chọn)
3. Click **Create Account**

### Các tính năng chính

#### 📝 Editor (Query Editor)

**Đường dẫn**: Menu trái → **Editor** → **Hive**

Viết và thực thi Hive SQL queries:

```sql
-- Xem danh sách databases
SHOW DATABASES;

-- Sử dụng database retail
USE retail_db;

-- Xem danh sách tables
SHOW TABLES;

-- Query dữ liệu
SELECT * FROM transactions LIMIT 10;

-- Phân tích doanh thu theo tháng
SELECT
    Year, Month,
    SUM(TotalAmount) as Revenue
FROM transactions
GROUP BY Year, Month
ORDER BY Year, Month;
```

#### 📁 File Browser

**Đường dẫn**: Menu trái → **Files**

- Duyệt HDFS file system
- Upload/Download files
- Tạo/Xóa thư mục
- Xem nội dung file

**Các thao tác:**

1. Click **New** → **Directory** để tạo thư mục
2. Click **Upload** để upload file từ máy local
3. Right-click file → **Download** để tải về

#### 📊 Table Browser (Metastore)

**Đường dẫn**: Menu trái → **Tables**

- Xem danh sách databases và tables
- Xem schema của tables
- Preview dữ liệu
- Tạo table mới

#### 📈 Dashboard

Tạo các dashboard visualization từ query results.

### Cách sử dụng Hue

#### Bước 1: Kiểm tra kết nối Hive

```sql
SHOW DATABASES;
```

#### Bước 2: Tạo database và table (nếu chưa có)

```sql
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- Xem các tables đã tạo bởi ETL
SHOW TABLES;
```

#### Bước 3: Query phân tích

```sql
-- Top 10 sản phẩm bán chạy
SELECT * FROM top_products_by_revenue LIMIT 10;

-- Phân khúc khách hàng
SELECT * FROM customer_segments;

-- Doanh thu theo tháng
SELECT * FROM monthly_revenue ORDER BY Year, Month;
```

### ⚠️ Lưu ý

Nếu gặp lỗi kết nối Hive, hãy sử dụng Jupyter Notebook thay thế.

---

## 4. Jupyter Notebook

### 🔗 Truy cập: http://localhost:8889

### Lấy Token đăng nhập

**Windows (PowerShell):**

```powershell
docker logs jupyter 2>&1 | Select-String -Pattern "token"
```

**Linux/Mac:**

```bash
docker logs jupyter 2>&1 | grep token
```

Token có dạng: `http://127.0.0.1:8888/lab?token=abc123xyz...`

Copy phần sau `token=` và paste vào ô nhập token.

### Mô tả

Jupyter Notebook với PySpark để phân tích dữ liệu interactive.

### Giao diện JupyterLab

#### 📁 File Browser (Bên trái)

- `work/` - Thư mục làm việc chính
- `data/` - Chứa file dữ liệu
- `spark-apps/` - Spark applications

#### 📓 Notebook

File `retail_analysis.ipynb` đã được tạo sẵn với các phân tích:

1. **Load và Khám phá Dữ liệu**
2. **Làm sạch Dữ liệu**
3. **Phân tích Doanh thu theo Thời gian**
4. **Top Sản phẩm Bán chạy**
5. **Phân tích RFM**
6. **Customer Clustering**
7. **Phân tích theo Quốc gia**

### Cách sử dụng Jupyter

#### Bước 1: Mở Notebook

1. Navigate đến `work/` trong File Browser
2. Double-click vào `retail_analysis.ipynb`

#### Bước 2: Chạy cells

- **Chạy 1 cell**: `Shift + Enter`
- **Chạy tất cả**: Menu **Run** → **Run All Cells**

#### Bước 3: Tạo Notebook mới

1. Click **File** → **New** → **Notebook**
2. Chọn kernel **Python 3**

#### Code mẫu - Kết nối Spark và Hive

```python
from pyspark.sql import SparkSession

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("RetailAnalysis") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Load dữ liệu từ CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/jovyan/data/online_retail.csv")

# Xem dữ liệu
df.show(10)

# Query Hive tables (sau khi chạy ETL)
spark.sql("SHOW DATABASES").show()
spark.sql("USE retail_db")
spark.sql("SELECT * FROM monthly_revenue").show()
```

### Visualization với Matplotlib

```python
import matplotlib.pyplot as plt
import pandas as pd

# Chuyển Spark DataFrame sang Pandas
monthly_pd = spark.sql("SELECT * FROM monthly_revenue").toPandas()

# Vẽ biểu đồ
plt.figure(figsize=(12, 6))
plt.plot(monthly_pd['Month'], monthly_pd['TotalRevenue'], marker='o')
plt.title('Doanh thu theo tháng')
plt.xlabel('Tháng')
plt.ylabel('Doanh thu ($)')
plt.grid(True)
plt.show()
```

---

## 5. MongoDB Express

### 🔗 Truy cập: http://localhost:8082

### Credentials

- **Username**: `admin`
- **Password**: `admin123`

### Mô tả

Giao diện web để quản lý MongoDB - nơi lưu trữ kết quả phân tích.

### Giao diện chính

#### 📊 Databases

Danh sách databases trong MongoDB:

- `admin` - System database
- `config` - Configuration
- `local` - Local data
- `retail_analytics` - **Database chính của project**

#### 📁 Collections trong retail_analytics

Sau khi chạy ETL pipeline, các collections sau sẽ được tạo:

| Collection              | Mô tả                          |
| ----------------------- | ------------------------------ |
| `transactions`          | Dữ liệu giao dịch đã xử lý     |
| `monthly_revenue`       | Doanh thu theo tháng           |
| `daily_revenue`         | Doanh thu theo ngày trong tuần |
| `hourly_revenue`        | Doanh thu theo giờ             |
| `top_products_quantity` | Top sản phẩm theo số lượng     |
| `top_products_revenue`  | Top sản phẩm theo doanh thu    |
| `customer_rfm`          | Phân tích RFM khách hàng       |
| `customer_segments`     | Phân khúc khách hàng           |
| `customer_clusters`     | Kết quả clustering             |
| `country_performance`   | Hiệu suất theo quốc gia        |
| `monthly_trend`         | Xu hướng theo tháng            |

### Cách sử dụng

#### Bước 1: Đăng nhập

1. Truy cập http://localhost:8082
2. Nhập username: `admin`, password: `admin123`

#### Bước 2: Xem dữ liệu

1. Click vào database `retail_analytics`
2. Click vào collection muốn xem (ví dụ: `monthly_revenue`)
3. Xem danh sách documents

#### Bước 3: Query dữ liệu

1. Trong collection, click **New Query**
2. Nhập query JSON:

```json
{ "CustomerSegment": "Champions" }
```

3. Click **Find** để tìm kiếm

#### Bước 4: Export dữ liệu

1. Chọn collection
2. Click **Export**
3. Chọn định dạng (JSON/CSV)

### Query MongoDB từ Python

```python
from pymongo import MongoClient

# Kết nối MongoDB
client = MongoClient('mongodb://admin:admin123@localhost:27017/')
db = client['retail_analytics']

# Lấy dữ liệu từ collection
monthly_revenue = list(db.monthly_revenue.find())
for doc in monthly_revenue:
    print(doc)

# Query với điều kiện
champions = db.customer_rfm.find({"CustomerSegment": "Champions"})
for customer in champions:
    print(customer)
```

---

## 🔄 Workflow thực hành

### Quy trình hoàn chỉnh

```
┌─────────────────────────────────────────────────────────────┐
│  1. KHỞI ĐỘNG HỆ THỐNG                                      │
│     > start.bat                                             │
│     Chờ 2-3 phút để services khởi động                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. KIỂM TRA HDFS                                           │
│     🌐 http://localhost:9870                                │
│     → Utilities → Browse file system                        │
│     → Kiểm tra cluster status                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. UPLOAD DỮ LIỆU                                          │
│     > upload-data.bat                                       │
│     Hoặc: Hue → Files → Upload                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. CHẠY ETL PIPELINE                                       │
│     > run-etl.bat                                           │
│     🌐 http://localhost:8080 → Theo dõi Spark job           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. PHÂN TÍCH DỮ LIỆU                                       │
│     Option A: Hue (http://localhost:8888)                   │
│               → Editor → Hive → Viết SQL queries            │
│                                                             │
│     Option B: Jupyter (http://localhost:8889)               │
│               → Mở retail_analysis.ipynb                    │
│               → Chạy các cells phân tích                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  6. XEM KẾT QUẢ TRONG MONGODB                               │
│     🌐 http://localhost:8082                                │
│     → retail_analytics → Các collections                    │
└─────────────────────────────────────────────────────────────┘
```

### Các bước thực hành chi tiết

#### 🎯 Bài thực hành 1: Khám phá HDFS

1. Truy cập http://localhost:9870
2. Xem **Cluster Summary** - hiểu cấu trúc cluster
3. Vào **Utilities** → **Browse file system**
4. Navigate đến `/user/retail/` và xem file `online_retail.csv`

#### 🎯 Bài thực hành 2: Chạy Spark Job

1. Mở terminal, chạy `run-etl.bat`
2. Truy cập http://localhost:8080 ngay lập tức
3. Quan sát job xuất hiện trong **Running Applications**
4. Click vào Application ID để xem chi tiết stages

#### 🎯 Bài thực hành 3: Query với Hue

1. Truy cập http://localhost:8888, đăng nhập
2. Vào **Editor** → **Hive**
3. Chạy các queries:

```sql
SHOW DATABASES;
USE retail_db;
SHOW TABLES;
SELECT * FROM monthly_revenue;
```

#### 🎯 Bài thực hành 4: Phân tích với Jupyter

1. Truy cập http://localhost:8889 (dùng token)
2. Mở `work/retail_analysis.ipynb`
3. Chạy từng cell (Shift + Enter)
4. Quan sát các biểu đồ visualization

#### 🎯 Bài thực hành 5: Xem kết quả MongoDB

1. Truy cập http://localhost:8082 (admin/admin123)
2. Vào database `retail_analytics`
3. Xem các collections: `customer_segments`, `monthly_revenue`, etc.
4. Thử query: `{ "TotalRevenue": { "$gt": 100000 } }`

---

## 🆘 Xử lý sự cố

### Không truy cập được Web UI

```powershell
# Kiểm tra services đang chạy
docker-compose ps

# Restart service cụ thể
docker-compose restart <service_name>

# Xem logs
docker-compose logs <service_name>
```

### Hue báo lỗi kết nối Hive

1. Đợi 2-3 phút sau khi start
2. Restart Hive services:

```powershell
docker-compose restart hive-metastore hive-server hue
```

3. Sử dụng Jupyter thay thế

### Jupyter không load được

```powershell
# Lấy token mới
docker logs jupyter 2>&1 | Select-String -Pattern "token"

# Restart Jupyter
docker-compose restart jupyter
```

### MongoDB Express không hiển thị data

1. Đảm bảo đã chạy `run-etl.bat`
2. Refresh trang
3. Kiểm tra database `retail_analytics` tồn tại

---

## 📚 Tài liệu bổ sung

- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Spark Web UI Guide](https://spark.apache.org/docs/latest/web-ui.html)
- [Hue User Guide](https://docs.gethue.com/)
- [Jupyter Documentation](https://jupyter.org/documentation)
- [MongoDB Manual](https://www.mongodb.com/docs/manual/)

---

**Happy Learning Big Data! 🚀**
