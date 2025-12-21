# 📚 HƯỚNG DẪN SỬ DỤNG PIPELINE BÁN LẺ - RETAIL DATA PIPELINE

## 🚀 CÁCH CHẠY NHANH NHẤT (Không cần Spark ETL)

### Bước 1: Khởi động hệ thống

```powershell
cd D:\BigDataFinal
docker-compose up -d
```

Chờ khoảng 2-3 phút để các services khởi động hoàn tất.

### Bước 2: Mở Jupyter Notebook

1. Mở trình duyệt, vào: **http://localhost:8889**
2. Token: `bigdata2024`
3. Mở file: `simple_retail_analysis.ipynb`

### Bước 3: Chạy từng cell trong notebook

- Click vào cell đầu tiên
- Nhấn **Shift + Enter** để chạy từng cell
- Hoặc vào menu **Run → Run All Cells** để chạy tất cả

### Bước 4: Xem kết quả

Các biểu đồ sẽ hiển thị trực tiếp trong notebook:

- 📊 Doanh thu theo tháng
- 📈 Doanh thu theo ngày/giờ
- 🏆 Top sản phẩm bán chạy
- 👥 Phân khúc khách hàng (RFM)
- 🎯 Phân cụm K-Means
- 🌍 Phân tích theo quốc gia

Kết quả cũng được lưu vào thư mục `data/output/`:

- File CSV: customer_rfm_analysis.csv, top_products.csv, etc.
- File hình ảnh: các biểu đồ .png

---

## 💻 GIAO DIỆN WEB CÓ SẴN

| Service              | URL                   | Mục đích                       |
| -------------------- | --------------------- | ------------------------------ |
| **Jupyter Notebook** | http://localhost:8889 | Phân tích dữ liệu, chạy Python |
| **Hadoop HDFS**      | http://localhost:9870 | Quản lý file HDFS              |
| **Spark Master**     | http://localhost:8080 | Theo dõi Spark jobs            |
| **Hue**              | http://localhost:8888 | Chạy Hive queries              |
| **Mongo Express**    | http://localhost:8082 | Xem dữ liệu MongoDB            |

---

## 📊 HƯỚNG DẪN TRỰC QUAN HÓA

### 1. Trong Jupyter Notebook (Đề xuất)

Notebook `simple_retail_analysis.ipynb` đã có sẵn code trực quan hóa với:

- **matplotlib** - Vẽ biểu đồ cơ bản
- **seaborn** - Biểu đồ đẹp hơn
- **3D plots** - Biểu đồ 3D cho clustering

**Các loại biểu đồ trong notebook:**

1. Bar chart - Doanh thu theo tháng
2. Line chart - Xu hướng theo giờ
3. Horizontal bar - Top sản phẩm
4. Pie chart - Phân khúc khách hàng
5. Scatter plot 3D - Phân cụm khách hàng

### 2. Trong Hue (Hive Queries)

1. Mở http://localhost:8888
2. Login: `admin` / `admin`
3. Vào **Query Editor → Hive**
4. Chạy các queries có sẵn trong `hive-queries/retail_analytics.sql`

### 3. Trong Mongo Express

1. Mở http://localhost:8082
2. Login: `admin` / `admin`
3. Xem collection `customer_analytics` để thấy kết quả phân tích

---

## ⚙️ CHẠY SPARK ETL PIPELINE (Nâng cao)

Nếu muốn chạy ETL với Spark và Hive:

```powershell
# Khởi động lại Hive với config mới
docker-compose restart hive-metastore hive-server

# Chờ 1-2 phút rồi chạy ETL
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.sql.hive.metastore.timeout=600 `
    /spark-apps/retail_etl_pipeline.py
```

**Lưu ý:** ETL có thể mất 10-15 phút tùy cấu hình máy.

---

## 🔧 XỬ LÝ LỖI THƯỜNG GẶP

### Lỗi 1: Jupyter không kết nối được

```powershell
docker-compose restart jupyter
```

### Lỗi 2: Không đọc được file CSV

Đảm bảo file `online_retail.csv` nằm trong thư mục `D:\BigDataFinal`

### Lỗi 3: Biểu đồ không hiển thị

Thêm dòng này vào đầu notebook:

```python
%matplotlib inline
```

### Lỗi 4: Thiếu thư viện

Trong Jupyter, chạy cell:

```python
!pip install pandas matplotlib seaborn scikit-learn
```

---

## 📈 CÁC PHÂN TÍCH CÓ TRONG PIPELINE

### 1. Phân tích Doanh thu theo Thời gian

- Doanh thu theo tháng, quý, năm
- Xu hướng mua hàng theo ngày trong tuần
- Giờ cao điểm bán hàng

### 2. Phân tích Sản phẩm

- Top sản phẩm bán chạy theo doanh thu
- Top sản phẩm theo số lượng
- Sản phẩm theo danh mục

### 3. Phân tích Khách hàng (RFM)

- **Recency**: Khách mua gần đây nhất
- **Frequency**: Tần suất mua hàng
- **Monetary**: Tổng chi tiêu

### 4. Phân cụm Khách hàng (K-Means)

- VIP Customers - Khách hàng chi tiêu cao
- Frequent Buyers - Mua thường xuyên
- Regular Customers - Khách hàng bình thường
- Inactive Customers - Không hoạt động

### 5. Phân tích Địa lý

- Doanh thu theo quốc gia
- Thị trường tiềm năng

---

## 📁 CẤU TRÚC THƯ MỤC OUTPUT

Sau khi chạy notebook, kết quả được lưu tại:

```
data/output/
├── customer_rfm_analysis.csv    # Phân tích RFM cho từng khách hàng
├── top_products.csv             # Top sản phẩm bán chạy
├── revenue_monthly.csv          # Doanh thu theo tháng
├── country_stats.csv            # Thống kê theo quốc gia
├── revenue_monthly.png          # Biểu đồ doanh thu tháng
├── revenue_time_patterns.png    # Biểu đồ xu hướng thời gian
├── top_products.png             # Biểu đồ top sản phẩm
├── customer_segments.png        # Biểu đồ phân khúc
├── customer_clusters.png        # Biểu đồ clustering
└── country_analysis.png         # Biểu đồ quốc gia
```

---

## 💡 TIPS

1. **Bắt đầu với Jupyter** - Đây là cách nhanh nhất để xem kết quả
2. **Chạy từng cell** - Giúp debug dễ hơn
3. **Xem file PNG** - Các biểu đồ được lưu để dùng trong báo cáo
4. **Export CSV** - Dùng cho Excel hoặc Power BI

---

## 📞 HỖ TRỢ

Nếu gặp vấn đề, kiểm tra:

1. Docker đang chạy: `docker ps`
2. Logs của service: `docker-compose logs jupyter`
3. Dữ liệu có đúng vị trí: `D:\BigDataFinal\online_retail.csv`
