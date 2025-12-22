# Big Data Retail Analytics Platform

## Huong Dan Su Dung Chi Tiet (Updated)

He thong phan tich du lieu ban le su dung Big Data stack bao gom: Hadoop HDFS, Apache Spark, Hive, MongoDB, Hue va Jupyter Notebook.

---

## 1. Yeu Cau He Thong

- **Docker Desktop** (cho Windows/Mac) hoac **Docker Engine** (cho Linux)
- **Docker Compose** (thuong duoc cai san voi Docker Desktop)
- **RAM**: Toi thieu 8GB, khuyen nghi 16GB
- **Disk Space**: Toi thieu 10GB trong

---

## 2. Cac Dich Vu va Ports

| Dich vu | Container | Port | Mo ta |
|---------|-----------|------|-------|
| **Python Web App** | webapp | **5000** | **Giao dien web phan tich du lieu** |
| HDFS Namenode UI | namenode | **9870** | Quan ly HDFS |
| Spark Master UI | spark-master | **8580** | Quan ly Spark cluster |
| Hue | hue | **8788** | Giao dien SQL va quan ly Hadoop |
| Jupyter Notebook | jupyter | **8889** | Phan tich du lieu tuong tac |
| Mongo Express | mongo-express | **8290** | Quan ly MongoDB (web UI) |
| MongoDB | mongodb | 27017 | Database |
| Hive Server | hive-server | 10000 | Hive JDBC |
| Hive Metastore | hive-metastore | 9083 | Hive Metastore Thrift |
| Spark Master | spark-master | 7777 | Spark cluster port |
| Spark Worker UI | spark-worker | 8581 | Spark Worker UI |

---

## 3. Khoi Dong He Thong

### Buoc 1: Khoi dong tat ca services

```batch
# Windows
start.bat

# Hoac dung docker-compose truc tiep
docker-compose up -d
```

### Buoc 2: Cho he thong san sang (khoang 1-2 phut)

Kiem tra cac containers dang chay:
```batch
docker ps
```

Tat ca 11 containers nen o trang thai "Up" va "healthy".

### Buoc 3: Upload du lieu

```batch
# Windows
upload-data.bat

# Hoac manually
docker exec namenode hdfs dfs -mkdir -p /user/retail
docker exec namenode hdfs dfs -put -f /data/online_retail.csv /user/retail/
```

---

## 4. Chay Cac Quy Trinh Phan Tich

### 4.1 Chay ETL Pipeline

ETL Pipeline se doc du lieu tu HDFS, xu ly va luu vao ca HDFS (parquet) va MongoDB.

```batch
# Windows
run-etl.bat

# Hoac manually
docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/retail_etl_pipeline.py
```

**Ket qua:**
- **HDFS**: `/user/retail/analysis/` - Cac bao cao phan tich (parquet)
- **MongoDB**: Database `retail_analytics` voi cac collections:
  - `transactions` - Du lieu giao dich
  - `customer_rfm` - Phan tich RFM
  - `customer_segments` - Phan khuc khach hang
  - `top_products_quantity` - San pham ban chay theo so luong
  - `top_products_revenue` - San pham ban chay theo doanh thu
  - `daily_revenue`, `monthly_revenue`, `hourly_revenue` - Doanh thu
  - `monthly_trend` - Xu huong theo thang
  - `country_performance` - Hieu suat theo quoc gia
  - `basket_analysis` - Phan tich gio hang
  - `product_associations` - Lien ket san pham
  - `product_recommendations` - Goi y san pham

### 4.2 Chay Customer Clustering/Segmentation

```batch
# Windows
run-clustering.bat

# Hoac manually (su dung script don gian - khong can numpy)
docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/customer_clustering_simple.py
```

**Ket qua:**
- **HDFS**: `/user/retail/analysis/customer_clusters/`
- **HDFS**: `/user/retail/analysis/cluster_statistics/`
- **MongoDB**: Collections `customer_clusters` va `cluster_statistics`

**Cac phan khuc khach hang:**
- **Champions** - Khach hang VIP (R, F, M deu cao)
- **Loyal Customers** - Khach hang trung thanh
- **Big Spenders** - Khach hang chi tieu lon
- **New Customers** - Khach hang moi
- **Regular** - Khach hang binh thuong
- **At Risk** - Co nguy co mat khach
- **Hibernating** - Tam nghi mua hang
- **Lost Customers** - Da mat

---

## 5. Truy Cap Giao Dien Web

### 5.0 Python Web Application (CHINH)
- **URL**: http://localhost:5000
- **Chuc nang**: 
  - Dashboard tong quan
  - Xem danh sach va chi tiet khach hang
  - Phan tich phan khuc khach hang (RFM)
  - Xem san pham ban chay
  - Bieu do doanh thu theo thoi gian
  - Hieu suat theo quoc gia
  - Goi y san pham

**Cac trang co san:**
- `/` - Dashboard tong quan
- `/customers` - Danh sach khach hang
- `/customer/<id>` - Chi tiet khach hang
- `/segments` - Phan tich phan khuc
- `/products` - San pham ban chay
- `/revenue` - Phan tich doanh thu
- `/countries` - Hieu suat theo quoc gia
- `/recommendations` - Goi y san pham

**API Endpoints:**
- `/api/stats` - Thong ke tong quan
- `/api/customers` - Danh sach khach hang (JSON)
- `/api/segments` - Thong ke phan khuc
- `/api/revenue/monthly` - Doanh thu theo thang
- `/api/products/top` - Top san pham
- `/api/countries` - Du lieu theo quoc gia

### 5.1 HDFS Namenode
- **URL**: http://localhost:9870
- **Chuc nang**: Quan ly file system, xem trang thai cluster

### 5.2 Spark Master
- **URL**: http://localhost:8580
- **Chuc nang**: Xem cac jobs dang chay, workers, applications

### 5.3 Hue (SQL & Hadoop UI)
- **URL**: http://localhost:8788
- **Dang nhap**: `admin` / `admin`
- **Chuc nang**: 
  - Chay SQL queries tren Hive
  - Duyet file HDFS
  - Quan ly workflows

### 5.4 Jupyter Notebook
- **URL**: http://localhost:8889
- **Token**: Xem bang lenh `docker logs jupyter`
- **Notebooks co san**:
  - `retail_analysis.ipynb` - Phan tich day du
  - `simple_retail_analysis.ipynb` - Phan tich don gian

### 5.5 Mongo Express
- **URL**: http://localhost:8290
- **Dang nhap**: `admin` / `admin123`
- **Chuc nang**: Quan ly MongoDB, xem collections va documents

---

## 6. Kiem Tra Ket Qua

### Kiem tra HDFS
```batch
docker exec namenode hdfs dfs -ls /user/retail/analysis/
```

### Kiem tra MongoDB
```batch
docker exec mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.getSiblingDB('retail_analytics').getCollectionNames()"
```

### Xem du lieu mau tu MongoDB
```batch
docker exec mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.getSiblingDB('retail_analytics').customer_clusters.find().limit(5)"
```

---

## 7. Dung He Thong

```batch
# Windows
stop.bat

# Hoac
docker-compose down
```

Giu lai data:
```batch
docker-compose down
```

Xoa tat ca data (chu y: mat het du lieu!):
```batch
docker-compose down -v
```

---

## 8. Khoi Dong Lai

```batch
# Windows
restart.bat

# Hoac
docker-compose restart
```

---

## 9. Xu Ly Su Co Thuong Gap

### 9.1 Container khong chay
```batch
# Xem logs
docker logs <ten-container>

# Vi du:
docker logs hive-metastore
docker logs spark-master
```

### 9.2 Port bi chiem
Neu gap loi port, thay doi port trong file `docker-compose.yml` va restart.

### 9.3 Loi ket noi MongoDB tu Spark
Dam bao su dung dung connection URI:
```
mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin
```

### 9.4 Hive Metastore khong khoi dong
Neu Hive Metastore khong hoat dong, chay schema init:
```batch
docker run --rm --network bigdatafinal_bigdata-network ^
    -v "%cd%\config\hive-site.xml:/opt/hive/conf/hive-site.xml" ^
    bde2020/hive:2.3.2-postgresql-metastore ^
    /opt/hive/bin/schematool -dbType postgres -initSchema
```

### 9.5 Khong tim thay du lieu tren HDFS
Chay lai upload data:
```batch
docker exec namenode hdfs dfs -mkdir -p /user/retail
docker exec namenode hdfs dfs -put -f /data/online_retail.csv /user/retail/
```

---

## 10. Cau Truc Thu Muc

```
BigDataFinal/
├── docker-compose.yml       # Cau hinh Docker
├── config/
│   ├── hadoop.env           # Bien moi truong Hadoop
│   ├── hive-site.xml        # Cau hinh Hive
│   └── hue.ini              # Cau hinh Hue
├── data/
│   └── online_retail.csv    # Du lieu dau vao
├── spark-apps/
│   ├── retail_etl_pipeline.py          # ETL chinh
│   ├── customer_clustering.py          # Clustering (can numpy)
│   ├── customer_clustering_simple.py   # Clustering don gian (RFM)
│   └── product_recommendation.py       # Goi y san pham
├── hive-queries/
│   └── retail_analytics.sql  # Cac query Hive
├── notebooks/
│   ├── retail_analysis.ipynb
│   └── simple_retail_analysis.ipynb
├── mongo-init/
│   └── init-mongo.js         # Khoi tao MongoDB
├── start.bat                 # Khoi dong (Windows)
├── stop.bat                  # Dung (Windows)
├── restart.bat               # Khoi dong lai (Windows)
├── run-etl.bat               # Chay ETL (Windows)
├── run-clustering.bat        # Chay Clustering (Windows)
└── upload-data.bat           # Upload data (Windows)
```

---

## 11. Thong Tin Ky Thuat

### Phien ban cac thanh phan:
- **Spark**: 3.3.0 (voi Hadoop 3.3)
- **Hadoop**: 3.3.x (trong Spark images)
- **Hive**: 2.3.2
- **MongoDB**: 6.0
- **PostgreSQL** (Hive Metastore): 11-alpine
- **MongoDB Spark Connector**: 10.2.1

### Thong tin ket noi:
- **MongoDB**: `mongodb://admin:admin123@mongodb:27017/?authSource=admin`
- **Hive Metastore**: `thrift://hive-metastore:9083`
- **HDFS**: `hdfs://namenode:9000`
- **Spark Master**: `spark://spark-master:7077`

---

## 12. Lien He Ho Tro

Neu gap van de, vui long:
1. Kiem tra logs cua container tuong ung
2. Dam bao Docker co du tai nguyen (RAM, CPU)
3. Thu restart toan bo he thong

---

**Cap nhat lan cuoi**: December 2025
