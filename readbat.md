# So sánh run-etl.bat và run-clustering.bat

## Tổng quan

Hai file batch này đều sử dụng Apache Spark để xử lý dữ liệu retail, nhưng thực hiện các mục đích khác nhau trong pipeline Big Data.

---

## 1. run-etl.bat - ETL Pipeline

### Mục đích

Chạy quy trình **Extract - Transform - Load (ETL)** để:

- Đọc dữ liệu thô từ file CSV
- Làm sạch và chuyển đổi dữ liệu
- Phân tích cơ bản (revenue, top products, RFM metrics)
- Lưu kết quả vào HDFS và MongoDB

### Lệnh thực thi

```bat
docker exec spark-master /spark/bin/spark-submit ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/retail_etl_simple.py
```

### Đặc điểm kỹ thuật

| Tham số         | Giá trị                 |
| --------------- | ----------------------- |
| Script Python   | `retail_etl_simple.py`  |
| Driver Memory   | Mặc định (1g)           |
| Executor Memory | Mặc định (1g)           |
| Deploy Mode     | Client (mặc định)       |
| Master          | Local (trong container) |

### Kết quả đầu ra

- **HDFS**: `/user/retail/processed_data` - Dữ liệu đã làm sạch
- **MongoDB Collections**:
  - `monthly_revenue` - Doanh thu theo tháng
  - `daily_revenue` - Doanh thu theo ngày trong tuần
  - `hourly_revenue` - Doanh thu theo giờ
  - `top_products_quantity` - Top 20 sản phẩm theo số lượng
  - `top_products_revenue` - Top 20 sản phẩm theo doanh thu
  - `customer_rfm` - Điểm RFM của khách hàng
  - `customer_segments` - Phân khúc khách hàng
  - `country_performance` - Hiệu suất theo quốc gia
  - `monthly_trend` - Xu hướng hàng tháng
  - `transactions` - Mẫu 10,000 giao dịch

---

## 2. run-clustering.bat - Customer Segmentation

### Mục đích

Chạy thuật toán **phân cụm khách hàng (Customer Clustering)** dựa trên RFM:

- Đọc dữ liệu đã xử lý từ HDFS
- Tính toán RFM metrics chi tiết
- Phân khúc khách hàng thành các nhóm
- Lưu kết quả clustering vào HDFS và MongoDB

### Lệnh thực thi

```bat
docker exec spark-master /spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 ^
    /spark-apps/customer_clustering_simple.py
```

### Đặc điểm kỹ thuật

| Tham số         | Giá trị                                       |
| --------------- | --------------------------------------------- |
| Script Python   | `customer_clustering_simple.py`               |
| Driver Memory   | **2g** (tăng gấp đôi)                         |
| Executor Memory | **2g** (tăng gấp đôi)                         |
| Deploy Mode     | Client                                        |
| Master          | **spark://spark-master:7077** (Spark Cluster) |

### Kết quả đầu ra

- **HDFS**:
  - `/user/retail/analysis/customer_clusters` - Chi tiết từng khách hàng với segment
  - `/user/retail/analysis/cluster_statistics` - Thống kê tổng hợp theo segment
- **MongoDB Collections**:
  - `customer_clusters` - Thông tin clustering chi tiết
  - `cluster_statistics` - Thống kê các phân khúc

### Các phân khúc khách hàng

| Segment         | Mô tả                                          |
| --------------- | ---------------------------------------------- |
| Champions       | Khách hàng VIP - mua thường xuyên, giá trị cao |
| Loyal Customers | Khách hàng trung thành                         |
| Big Spenders    | Chi tiêu lớn nhưng không thường xuyên          |
| At Risk         | Có nguy cơ rời bỏ                              |
| New Customers   | Khách hàng mới                                 |
| Regular         | Khách hàng thông thường                        |
| Hibernating     | Khách hàng đang ngủ đông                       |
| Lost Customers  | Khách hàng đã mất                              |

---

## So sánh chi tiết

| Tiêu chí           | run-etl.bat                       | run-clustering.bat                     |
| ------------------ | --------------------------------- | -------------------------------------- |
| **Mục đích**       | ETL - Làm sạch & phân tích cơ bản | ML - Phân cụm khách hàng               |
| **Input**          | File CSV thô                      | Dữ liệu đã xử lý từ HDFS               |
| **Output**         | 10+ collections MongoDB           | 2 collections MongoDB + 2 folders HDFS |
| **Memory**         | 1g (mặc định)                     | 2g (tăng gấp đôi)                      |
| **Spark Mode**     | Local                             | Cluster                                |
| **Độ phức tạp**    | Trung bình                        | Cao (ML algorithms)                    |
| **Thời gian chạy** | ~45 giây                          | ~70 giây                               |
| **Phụ thuộc**      | Không                             | Cần chạy run-etl.bat trước             |

---

## Thứ tự chạy đúng

```
1. start.bat           → Khởi động Docker containers
2. upload-data.bat     → Upload dữ liệu lên HDFS
3. run-etl.bat         → Chạy ETL pipeline (BẮT BUỘC TRƯỚC)
4. run-clustering.bat  → Chạy customer segmentation
5. run-webapp.bat      → Khởi động web application
```

> ⚠️ **Lưu ý quan trọng**: Phải chạy `run-etl.bat` thành công trước khi chạy `run-clustering.bat`, vì clustering cần đọc dữ liệu đã được xử lý từ HDFS.

---

## Tại sao run-clustering.bat cần nhiều memory hơn?

1. **Thuật toán ML phức tạp**: RFM scoring sử dụng Window Functions để tính percentile
2. **Xử lý toàn bộ khách hàng**: Phải load và xử lý dữ liệu của tất cả khách hàng cùng lúc
3. **Multiple passes**: Thuật toán cần nhiều lần duyệt qua dữ liệu để:
   - Tính RFM metrics (Recency, Frequency, Monetary)
   - Gán điểm 1-5 cho mỗi metric
   - Phân loại vào segments
4. **Cluster mode**: Kết nối đến Spark Master để phân tán xử lý

---

## Kết luận

| File                   | Vai trò trong Pipeline                         |
| ---------------------- | ---------------------------------------------- |
| **run-etl.bat**        | Bước chuẩn bị dữ liệu - "Data Preparation"     |
| **run-clustering.bat** | Bước phân tích nâng cao - "Advanced Analytics" |

Cả hai file đều quan trọng và bổ sung cho nhau trong pipeline Big Data hoàn chỉnh.
