# HƯỚNG DẪN PHÁT TRIỂN DỰ ÁN - Big Data Retail Analytics

## 📁 CÁC FILE KHÔNG SỬ DỤNG / CÓ THỂ XÓA

Các file sau đây không được sử dụng trong quy trình chính hoặc đã bị thay thế:

| File/Thư mục | Lý do không dùng |
|--------------|------------------|
| `hive-site-template.xml` | Template cũ, đã được thay thế bởi `config/hive-site.xml` |
| `hue-template.xml` | Template cũ, đã được thay thế bởi `config/hue.ini` |
| `logs.txt` | File log cũ, có thể xóa |
| `new_readme.md` | Readme draft cũ |
| `README_UI.md` | Readme UI cũ |
| `HUONG_DAN_SU_DUNG.md` | Duplicate với `final_readme.md` |
| `remove_data.py` | Script xóa dữ liệu, ít khi dùng |
| `-p/` | Thư mục rỗng, có thể xóa |
| `hue-data/` | Dữ liệu Hue tự sinh, có thể xóa khi cần reset |
| `hue-logs/` | Logs Hue tự sinh, có thể xóa |
| `spark-apps/retail_etl_pipeline.py` | ETL cũ dùng Hive, đã thay bằng `retail_etl_simple.py` |
| `spark-apps/customer_clustering.py` | Clustering cũ, đã thay bằng `customer_clustering_simple.py` |
| `spark-apps/product_recommendation.py` | Chức năng recommendation chưa hoàn thiện |
| `notebooks/` | Jupyter notebooks cho thử nghiệm, không bắt buộc |
| `hive-queries/` | SQL queries cho Hive, không cần thiết với ETL simple |

---

## 📁 CẤU TRÚC DỰ ÁN CHÍNH

```
BigDataFinal/
├── docker-compose.yml          # [QUAN TRỌNG] Cấu hình Docker services
├── start.bat / start.sh        # [QUAN TRỌNG] Khởi động hệ thống
├── stop.bat / stop.sh          # [QUAN TRỌNG] Dừng hệ thống
├── upload-data.bat             # [QUAN TRỌNG] Upload data lên HDFS
├── run-etl.bat                 # [QUAN TRỌNG] Chạy ETL pipeline
├── run-clustering.bat          # [QUAN TRỌNG] Chạy clustering
├── run-webapp.bat              # [QUAN TRỌNG] Khởi động webapp
├── final_readme.md             # Hướng dẫn sử dụng
│
├── config/                     # Cấu hình các services
│   ├── hadoop.env              # Biến môi trường Hadoop
│   ├── hive-site.xml           # Cấu hình Hive
│   └── hue.ini                 # Cấu hình Hue
│
├── data/
│   └── online_retail.csv       # [QUAN TRỌNG] Dữ liệu gốc
│
├── mongo-init/
│   └── init-mongo.js           # Script khởi tạo MongoDB
│
├── spark-apps/                 # [QUAN TRỌNG] Spark applications
│   ├── retail_etl_simple.py    # ETL chính (đang dùng)
│   └── customer_clustering_simple.py  # Clustering chính (đang dùng)
│
└── webapp/                     # [QUAN TRỌNG] Flask web application
    ├── app.py                  # Main Flask app
    ├── Dockerfile              # Docker build cho webapp
    ├── requirements.txt        # Python dependencies
    └── templates/              # HTML templates
        ├── base.html           # Base template
        ├── index.html          # Dashboard chính
        ├── customers.html      # Danh sách khách hàng
        ├── customer_detail.html # Chi tiết khách hàng
        ├── products.html       # Top sản phẩm
        ├── revenue.html        # Phân tích doanh thu
        ├── segments.html       # Phân khúc khách hàng
        ├── countries.html      # Phân tích theo quốc gia
        └── recommendations.html # Gợi ý sản phẩm
```

---

## 🔧 HƯỚNG DẪN PHÁT TRIỂN CHI TIẾT

### 1. Docker Compose (`docker-compose.yml`)

Định nghĩa 12 services:

| Service | Port | Mô tả |
|---------|------|-------|
| `namenode` | 9870, 9000 | HDFS NameNode |
| `datanode` | 9864 | HDFS DataNode |
| `postgres-metastore` | 5432 | PostgreSQL cho Hive Metastore |
| `hive-metastore` | 9083 | Hive Metastore service |
| `hive-server` | 10000 | HiveServer2 |
| `spark-master` | 8580, 7077 | Spark Master |
| `spark-worker` | 8581 | Spark Worker |
| `mongodb` | 27017 | MongoDB database |
| `mongo-express` | 8290 | MongoDB web UI |
| `hue` | 8788 | Hue web interface |
| `jupyter` | 8889 | Jupyter Notebook |
| `webapp` | 5000 | Flask web application |

**Cách sửa đổi:**
```yaml
# Thay đổi port
ports:
  - "NEW_PORT:INTERNAL_PORT"

# Thay đổi memory
environment:
  - SPARK_WORKER_MEMORY=4g
```

---

### 2. ETL Pipeline (`spark-apps/retail_etl_simple.py`)

**Chức năng chính:**
1. Load dữ liệu từ CSV
2. Làm sạch dữ liệu (loại bỏ null, đơn hàng hủy)
3. Tính toán các metrics (RFM, revenue, top products)
4. Lưu vào HDFS và MongoDB

**Cách sửa đổi:**

```python
# Thay đổi input path
input_path = "/data/your_new_file.csv"

# Thêm collection mới
save_to_mongodb(your_df, "new_collection_name")

# Thêm phân tích mới
def analyze_new_metric(df):
    result = df.groupBy("column").agg(...)
    return result
```

**Collections MongoDB được tạo:**
- `customer_rfm` - RFM analysis
- `customer_segments` - Segment statistics
- `monthly_revenue`, `daily_revenue`, `hourly_revenue`
- `top_products_quantity`, `top_products_revenue`
- `country_performance`
- `monthly_trend`
- `transactions` (sample 10,000 records)

---

### 3. Clustering (`spark-apps/customer_clustering_simple.py`)

**Chức năng:**
1. Đọc dữ liệu từ HDFS
2. Tính RFM cho từng khách hàng
3. Gán điểm RFM (1-5 scale)
4. Phân loại thành 8 phân khúc

**8 Phân khúc khách hàng:**
- `Champions` - R,F,M cao
- `Loyal Customers` - F cao
- `New Customers` - R cao, F thấp
- `At Risk` - R thấp, F cao
- `Big Spenders` - M cao
- `Regular` - Trung bình
- `Hibernating` - R thấp, F trung bình
- `Lost Customers` - R rất thấp

**Cách thêm phân khúc mới:**
```python
def assign_segment(row):
    if condition:
        return "New Segment Name"
    # ...
```

---

### 4. Web Application (`webapp/app.py`)

**Framework:** Flask  
**Template Engine:** Jinja2  
**Database:** MongoDB (pymongo)

**Routes chính:**

| Route | Template | Mô tả |
|-------|----------|-------|
| `/` | `index.html` | Dashboard tổng quan |
| `/customers` | `customers.html` | Danh sách khách hàng |
| `/customer/<id>` | `customer_detail.html` | Chi tiết khách hàng |
| `/products` | `products.html` | Top sản phẩm |
| `/revenue` | `revenue.html` | Phân tích doanh thu |
| `/segments` | `segments.html` | Phân khúc khách hàng |
| `/countries` | `countries.html` | Phân tích theo quốc gia |
| `/recommendations` | `recommendations.html` | Gợi ý sản phẩm |

**Thêm route mới:**
```python
@app.route('/new-page')
def new_page():
    data = list(db.your_collection.find())
    return render_template('new_page.html', data=data)
```

**Thêm template mới:**
1. Tạo file `webapp/templates/new_page.html`
2. Extend base template: `{% extends "base.html" %}`
3. Thêm route trong `app.py`

---

### 5. Kết nối MongoDB

**Connection String:**
```python
MONGO_URI = "mongodb://admin:admin123@mongodb:27017/?authSource=admin"
client = MongoClient(MONGO_URI)
db = client.retail_analytics
```

**Truy vấn dữ liệu:**
```python
# Lấy tất cả documents
data = list(db.collection_name.find())

# Lấy với điều kiện
data = list(db.collection_name.find({"field": "value"}))

# Aggregate
pipeline = [
    {"$group": {"_id": "$field", "count": {"$sum": 1}}}
]
result = list(db.collection_name.aggregate(pipeline))
```

---

## 🔄 QUY TRÌNH PHÁT TRIỂN

### Thêm chức năng phân tích mới:

1. **Sửa ETL** (`retail_etl_simple.py`):
   - Thêm function phân tích
   - Gọi `save_to_mongodb(df, "collection_name")`

2. **Thêm Route** (`webapp/app.py`):
   - Tạo route mới
   - Query từ MongoDB collection

3. **Tạo Template** (`webapp/templates/`):
   - Tạo HTML file mới
   - Hiển thị dữ liệu

4. **Cập nhật Navigation** (`base.html`):
   - Thêm link vào navbar

5. **Deploy**:
   ```bash
   docker-compose up -d --build webapp
   ```

---

## 🛠️ DEBUG & TROUBLESHOOTING

### Xem logs:
```bash
docker logs spark-master --tail 100
docker logs webapp --tail 100
docker logs mongodb --tail 100
```

### Restart service:
```bash
docker restart webapp
docker restart spark-master
```

### Reset toàn bộ:
```bash
docker-compose down -v
docker-compose up -d
```

### Kiểm tra MongoDB:
```bash
docker exec mongodb mongosh -u admin -p admin123 --authenticationDatabase admin
> use retail_analytics
> db.getCollectionNames()
> db.customer_rfm.findOne()
```

### Kiểm tra HDFS:
```bash
docker exec namenode hdfs dfs -ls /user/retail/
docker exec namenode hdfs dfs -cat /user/retail/file.txt
```

---

## 📚 CÔNG NGHỆ SỬ DỤNG

| Công nghệ | Version | Mục đích |
|-----------|---------|----------|
| Apache Spark | 3.3.0 | Xử lý dữ liệu lớn |
| Apache Hadoop | 3.3.5 | Lưu trữ phân tán (HDFS) |
| Apache Hive | 3.1.3 | Data warehouse |
| MongoDB | 6.0 | NoSQL database |
| Flask | 2.3.3 | Web framework |
| Docker | 20.10+ | Containerization |
| Python | 3.9 | Programming language |
