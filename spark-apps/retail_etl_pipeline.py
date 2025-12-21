"""
Retail Data ETL Pipeline - Main Spark Application
===================================================
Pipeline xử lý dữ liệu bán lẻ với các chức năng:
1. Load dữ liệu từ CSV vào HDFS
2. Xử lý và làm sạch dữ liệu
3. Tạo các bảng phân tích trên Hive
4. Lưu kết quả phân tích vào MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, 
    month, year, dayofweek, hour, 
    when, lit, round as spark_round,
    desc, asc, to_date, to_timestamp,
    regexp_replace, trim, upper,
    row_number, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session với cấu hình kết nối Hive và MongoDB"""
    
    spark = SparkSession.builder \
        .appName("RetailDataPipeline") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.mongodb.input.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.transactions?authSource=admin") \
        .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.transactions?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.hadoop.hive.metastore.client.socket.timeout", "1800s") \
        .config("spark.sql.hive.metastore.sharedPrefixes", "org.postgresql") \
        .config("spark.sql.broadcastTimeout", "1800") \
        .config("spark.network.timeout", "1800s") \
        .config("spark.executor.heartbeatInterval", "300s") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session created successfully")
    return spark


def define_schema():
    """Định nghĩa schema cho dữ liệu bán lẻ"""
    
    return StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", DoubleType(), True),
        StructField("Country", StringType(), True)
    ])


def load_and_clean_data(spark, input_path):
    """
    Load và làm sạch dữ liệu từ CSV
    - Loại bỏ giá trị null
    - Loại bỏ đơn hàng hủy (InvoiceNo bắt đầu bằng 'C')
    - Chuyển đổi kiểu dữ liệu
    - Tính TotalAmount
    """
    
    logger.info(f"📂 Loading data from: {input_path}")
    
    schema = define_schema()
    
    # Load dữ liệu
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(input_path)
    
    logger.info(f"📊 Raw records: {df.count()}")
    
    # Làm sạch dữ liệu
    df_cleaned = df \
        .filter(col("CustomerID").isNotNull()) \
        .filter(col("Quantity") > 0) \
        .filter(col("UnitPrice") > 0) \
        .filter(~col("InvoiceNo").startswith("C")) \
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
        .withColumn("TotalAmount", spark_round(col("Quantity") * col("UnitPrice"), 2)) \
        .withColumn("Description", trim(upper(col("Description")))) \
        .withColumn("Year", year(col("InvoiceDate"))) \
        .withColumn("Month", month(col("InvoiceDate"))) \
        .withColumn("DayOfWeek", dayofweek(col("InvoiceDate"))) \
        .withColumn("Hour", hour(col("InvoiceDate")))
    
    logger.info(f"✅ Cleaned records: {df_cleaned.count()}")
    
    return df_cleaned


def save_to_hdfs(df, path, format="parquet"):
    """Lưu DataFrame vào HDFS"""
    
    logger.info(f"💾 Saving data to HDFS: {path}")
    
    df.write \
        .mode("overwrite") \
        .format(format) \
        .save(path)
    
    logger.info(f"✅ Data saved to HDFS successfully")


def create_hive_tables(spark, df):
    """Tạo các bảng Hive từ dữ liệu đã xử lý - sử dụng External Table để tránh timeout"""
    
    logger.info("🏗️ Creating Hive database and tables...")
    
    # Đường dẫn HDFS cho dữ liệu
    hdfs_path = "hdfs://namenode:9000/user/retail/transactions_data"
    
    # Bước 1: Lưu dữ liệu vào HDFS dạng Parquet (không cần Hive)
    logger.info(f"💾 Saving data to HDFS: {hdfs_path}")
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(hdfs_path)
    logger.info("✅ Data saved to HDFS successfully")
    
    # Bước 2: Tạo database (đơn giản, ít timeout)
    logger.info("📁 Creating database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS retail_db")
    spark.sql("USE retail_db")
    
    # Bước 3: Drop table cũ nếu có
    spark.sql("DROP TABLE IF EXISTS retail_db.transactions")
    logger.info("🗑️ Dropped existing table if any")
    
    # Bước 4: Tạo EXTERNAL TABLE trỏ đến dữ liệu đã lưu
    # External table chỉ tạo metadata, không copy dữ liệu -> nhanh, không timeout
    logger.info("📋 Creating external table...")
    
    create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS retail_db.transactions (
            InvoiceNo STRING,
            StockCode STRING,
            Description STRING,
            Quantity INT,
            InvoiceDate TIMESTAMP,
            UnitPrice DOUBLE,
            CustomerID INT,
            Country STRING,
            TotalAmount DOUBLE,
            Year INT,
            Month INT,
            DayOfWeek INT,
            Hour INT
        )
        STORED AS PARQUET
        LOCATION '{hdfs_path}'
    """
    
    spark.sql(create_table_sql)
    logger.info("✅ External table retail_db.transactions created")
    
    # Bước 5: Đăng ký df như temporary view để dùng trong phân tích
    df.createOrReplaceTempView("transactions")
    logger.info("✅ Temporary view 'transactions' created for analysis")
    
    return True


def analyze_revenue_by_time(spark):
    """
    Phân tích doanh thu theo thời gian:
    - Theo tháng
    - Theo ngày trong tuần
    - Theo giờ
    """
    
    logger.info("📈 Analyzing revenue by time...")
    
    # Doanh thu theo tháng (dùng temp view 'transactions')
    monthly_revenue = spark.sql("""
        SELECT 
            Year,
            Month,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            COUNT(DISTINCT CustomerID) as TotalCustomers,
            SUM(Quantity) as TotalQuantity,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue,
            ROUND(AVG(TotalAmount), 2) as AvgOrderValue
        FROM transactions
        GROUP BY Year, Month
        ORDER BY Year, Month
    """)
    
    # Doanh thu theo ngày trong tuần
    daily_revenue = spark.sql("""
        SELECT 
            DayOfWeek,
            CASE DayOfWeek
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END as DayName,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue
        FROM transactions
        GROUP BY DayOfWeek
        ORDER BY DayOfWeek
    """)
    
    # Doanh thu theo giờ
    hourly_revenue = spark.sql("""
        SELECT 
            Hour,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue
        FROM transactions
        GROUP BY Hour
        ORDER BY Hour
    """)
    
    # Lưu vào HDFS thay vì Hive (tránh timeout)
    hdfs_base = "hdfs://namenode:9000/user/retail/analysis"
    monthly_revenue.write.mode("overwrite").parquet(f"{hdfs_base}/monthly_revenue")
    daily_revenue.write.mode("overwrite").parquet(f"{hdfs_base}/daily_revenue")
    hourly_revenue.write.mode("overwrite").parquet(f"{hdfs_base}/hourly_revenue")
    
    # Tạo temp views cho các bước sau
    monthly_revenue.createOrReplaceTempView("monthly_revenue")
    daily_revenue.createOrReplaceTempView("daily_revenue")
    hourly_revenue.createOrReplaceTempView("hourly_revenue")
    
    logger.info("✅ Revenue analysis completed and saved to HDFS")
    
    return monthly_revenue, daily_revenue, hourly_revenue


def analyze_top_products(spark, top_n=20):
    """
    Phân tích sản phẩm bán chạy:
    - Top sản phẩm theo số lượng
    - Top sản phẩm theo doanh thu
    """
    
    logger.info(f"🏆 Analyzing top {top_n} products...")
    
    # Top sản phẩm theo số lượng
    top_by_quantity = spark.sql(f"""
        SELECT 
            StockCode,
            Description,
            SUM(Quantity) as TotalQuantity,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            COUNT(DISTINCT CustomerID) as TotalCustomers,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue
        FROM transactions
        GROUP BY StockCode, Description
        ORDER BY TotalQuantity DESC
        LIMIT {top_n}
    """)
    
    # Top sản phẩm theo doanh thu
    top_by_revenue = spark.sql(f"""
        SELECT 
            StockCode,
            Description,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue,
            SUM(Quantity) as TotalQuantity,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            ROUND(AVG(UnitPrice), 2) as AvgPrice
        FROM transactions
        GROUP BY StockCode, Description
        ORDER BY TotalRevenue DESC
        LIMIT {top_n}
    """)
    
    # Lưu vào HDFS
    hdfs_base = "hdfs://namenode:9000/user/retail/analysis"
    top_by_quantity.write.mode("overwrite").parquet(f"{hdfs_base}/top_products_by_quantity")
    top_by_revenue.write.mode("overwrite").parquet(f"{hdfs_base}/top_products_by_revenue")
    
    # Tạo temp views
    top_by_quantity.createOrReplaceTempView("top_products_by_quantity")
    top_by_revenue.createOrReplaceTempView("top_products_by_revenue")
    
    logger.info("✅ Top products analysis completed and saved to HDFS")
    
    return top_by_quantity, top_by_revenue


def analyze_customer_behavior(spark):
    """
    Phân tích hành vi khách hàng:
    - RFM Analysis (Recency, Frequency, Monetary)
    - Phân khúc khách hàng
    """
    
    logger.info("👥 Analyzing customer behavior...")
    
    # Tính RFM cho từng khách hàng
    rfm_analysis = spark.sql("""
        WITH customer_stats AS (
            SELECT 
                CustomerID,
                Country,
                DATEDIFF(
                    (SELECT MAX(InvoiceDate) FROM transactions),
                    MAX(InvoiceDate)
                ) as Recency,
                COUNT(DISTINCT InvoiceNo) as Frequency,
                ROUND(SUM(TotalAmount), 2) as Monetary,
                MIN(InvoiceDate) as FirstPurchase,
                MAX(InvoiceDate) as LastPurchase
            FROM transactions
            GROUP BY CustomerID, Country
        ),
        rfm_scores AS (
            SELECT 
                *,
                NTILE(5) OVER (ORDER BY Recency DESC) as R_Score,
                NTILE(5) OVER (ORDER BY Frequency) as F_Score,
                NTILE(5) OVER (ORDER BY Monetary) as M_Score
            FROM customer_stats
        )
        SELECT 
            *,
            (R_Score + F_Score + M_Score) as RFM_Score,
            CONCAT(R_Score, F_Score, M_Score) as RFM_Segment,
            CASE 
                WHEN R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4 THEN 'Champions'
                WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Loyal Customers'
                WHEN R_Score >= 4 AND F_Score <= 2 THEN 'New Customers'
                WHEN R_Score <= 2 AND F_Score >= 3 THEN 'At Risk'
                WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Lost'
                ELSE 'Regular'
            END as CustomerSegment
        FROM rfm_scores
    """)
    
    # Thống kê theo phân khúc
    segment_stats = spark.sql("""
        SELECT 
            CustomerSegment,
            COUNT(*) as CustomerCount,
            ROUND(AVG(Monetary), 2) as AvgMonetary,
            ROUND(AVG(Frequency), 2) as AvgFrequency,
            ROUND(AVG(Recency), 2) as AvgRecency
        FROM (
            SELECT 
                CustomerID,
                CASE 
                    WHEN R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4 THEN 'Champions'
                    WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Loyal Customers'
                    WHEN R_Score >= 4 AND F_Score <= 2 THEN 'New Customers'
                    WHEN R_Score <= 2 AND F_Score >= 3 THEN 'At Risk'
                    WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Lost'
                    ELSE 'Regular'
                END as CustomerSegment,
                Monetary, Frequency, Recency, R_Score, F_Score, M_Score
            FROM (
                SELECT 
                    CustomerID,
                    DATEDIFF(
                        (SELECT MAX(InvoiceDate) FROM transactions),
                        MAX(InvoiceDate)
                    ) as Recency,
                    COUNT(DISTINCT InvoiceNo) as Frequency,
                    SUM(TotalAmount) as Monetary,
                    NTILE(5) OVER (ORDER BY DATEDIFF(
                        (SELECT MAX(InvoiceDate) FROM transactions),
                        MAX(InvoiceDate)
                    ) DESC) as R_Score,
                    NTILE(5) OVER (ORDER BY COUNT(DISTINCT InvoiceNo)) as F_Score,
                    NTILE(5) OVER (ORDER BY SUM(TotalAmount)) as M_Score
                FROM transactions
                GROUP BY CustomerID
            ) rfm_base
        ) segmented
        GROUP BY CustomerSegment
        ORDER BY CustomerCount DESC
    """)
    
    # Lưu vào HDFS
    hdfs_base = "hdfs://namenode:9000/user/retail/analysis"
    rfm_analysis.write.mode("overwrite").parquet(f"{hdfs_base}/customer_rfm")
    segment_stats.write.mode("overwrite").parquet(f"{hdfs_base}/customer_segments")
    
    # Tạo temp views
    rfm_analysis.createOrReplaceTempView("customer_rfm")
    segment_stats.createOrReplaceTempView("customer_segments")
    
    logger.info("✅ Customer behavior analysis completed and saved to HDFS")
    
    return rfm_analysis, segment_stats


def analyze_country_performance(spark):
    """Phân tích hiệu suất theo quốc gia"""
    
    logger.info("🌍 Analyzing country performance...")
    
    country_stats = spark.sql("""
        SELECT 
            Country,
            COUNT(DISTINCT CustomerID) as TotalCustomers,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            SUM(Quantity) as TotalQuantity,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue,
            ROUND(AVG(TotalAmount), 2) as AvgOrderValue,
            ROUND(SUM(TotalAmount) / COUNT(DISTINCT CustomerID), 2) as RevenuePerCustomer
        FROM transactions
        GROUP BY Country
        ORDER BY TotalRevenue DESC
    """)
    
    # Lưu vào HDFS
    hdfs_base = "hdfs://namenode:9000/user/retail/analysis"
    country_stats.write.mode("overwrite").parquet(f"{hdfs_base}/country_performance")
    
    # Tạo temp view
    country_stats.createOrReplaceTempView("country_performance")
    
    logger.info("✅ Country performance analysis completed and saved to HDFS")
    
    return country_stats


def analyze_purchase_patterns(spark):
    """Phân tích xu hướng mua hàng"""
    
    logger.info("📊 Analyzing purchase patterns...")
    
    # Sản phẩm thường được mua cùng nhau
    basket_analysis = spark.sql("""
        SELECT 
            a.StockCode as Product1,
            b.StockCode as Product2,
            COUNT(*) as Frequency
        FROM transactions a
        JOIN transactions b 
            ON a.InvoiceNo = b.InvoiceNo 
            AND a.StockCode < b.StockCode
        GROUP BY a.StockCode, b.StockCode
        HAVING COUNT(*) > 50
        ORDER BY Frequency DESC
        LIMIT 100
    """)
    
    # Xu hướng mua hàng theo tháng
    monthly_trend = spark.sql("""
        SELECT 
            Year,
            Month,
            COUNT(DISTINCT CustomerID) as UniqueCustomers,
            COUNT(DISTINCT InvoiceNo) as TotalOrders,
            ROUND(SUM(TotalAmount), 2) as TotalRevenue,
            ROUND(SUM(TotalAmount) / COUNT(DISTINCT InvoiceNo), 2) as AvgOrderValue,
            LAG(SUM(TotalAmount)) OVER (ORDER BY Year, Month) as PrevMonthRevenue
        FROM transactions
        GROUP BY Year, Month
        ORDER BY Year, Month
    """)
    
    # Lưu vào HDFS
    hdfs_base = "hdfs://namenode:9000/user/retail/analysis"
    basket_analysis.write.mode("overwrite").parquet(f"{hdfs_base}/basket_analysis")
    monthly_trend.write.mode("overwrite").parquet(f"{hdfs_base}/monthly_trend")
    
    # Tạo temp views
    basket_analysis.createOrReplaceTempView("basket_analysis")
    monthly_trend.createOrReplaceTempView("monthly_trend")
    
    logger.info("✅ Purchase patterns analysis completed and saved to HDFS")
    
    return basket_analysis, monthly_trend


def save_to_mongodb(spark, view_name, collection_name):
    """Lưu kết quả phân tích vào MongoDB từ temp view"""
    
    logger.info(f"📤 Saving {view_name} to MongoDB collection: {collection_name}")
    
    try:
        df = spark.sql(f"SELECT * FROM {view_name}")
        
        df.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("uri", f"mongodb://admin:admin123@mongodb:27017/retail_analytics.{collection_name}?authSource=admin") \
            .save()
        
        logger.info(f"✅ Data saved to MongoDB: {collection_name}")
    except Exception as e:
        logger.warning(f"⚠️ Could not save {view_name} to MongoDB: {e}")


def run_pipeline():
    """Chạy toàn bộ pipeline ETL"""
    
    logger.info("🚀 Starting Retail Data Pipeline...")
    logger.info("=" * 60)
    
    # 1. Tạo Spark Session
    spark = create_spark_session()
    
    try:
        # 2. Load và làm sạch dữ liệu
        input_path = "/data/online_retail.csv"
        df = load_and_clean_data(spark, input_path)
        
        # 3. Lưu dữ liệu đã xử lý vào HDFS
        save_to_hdfs(df, "hdfs://namenode:9000/user/retail/processed_data")
        
        # 4. Tạo bảng Hive (External Table)
        create_hive_tables(spark, df)
        
        # 5. Chạy các phân tích
        logger.info("=" * 60)
        logger.info("🔍 Running Analytics...")
        
        analyze_revenue_by_time(spark)
        analyze_top_products(spark)
        analyze_customer_behavior(spark)
        analyze_country_performance(spark)
        analyze_purchase_patterns(spark)
        
        # 6. Lưu kết quả vào MongoDB (dùng temp views)
        logger.info("=" * 60)
        logger.info("💾 Saving results to MongoDB...")
        
        # Danh sách temp views để lưu vào MongoDB
        views_to_mongo = [
            ("monthly_revenue", "monthly_revenue"),
            ("daily_revenue", "daily_revenue"),
            ("hourly_revenue", "hourly_revenue"),
            ("top_products_by_quantity", "top_products_quantity"),
            ("top_products_by_revenue", "top_products_revenue"),
            ("customer_rfm", "customer_rfm"),
            ("customer_segments", "customer_segments"),
            ("country_performance", "country_performance"),
            ("monthly_trend", "monthly_trend")
        ]
        
        for view, collection in views_to_mongo:
            save_to_mongodb(spark, view, collection)
        
        logger.info("=" * 60)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 60)
        
        # Hiển thị tóm tắt (dùng temp view 'transactions')
        logger.info("\n📋 SUMMARY:")
        logger.info("-" * 40)
        
        total_records = spark.sql("SELECT COUNT(*) as cnt FROM transactions").collect()[0]['cnt']
        total_customers = spark.sql("SELECT COUNT(DISTINCT CustomerID) as cnt FROM transactions").collect()[0]['cnt']
        total_revenue = spark.sql("SELECT ROUND(SUM(TotalAmount), 2) as total FROM transactions").collect()[0]['total']
        
        logger.info(f"📊 Total Records: {total_records:,}")
        logger.info(f"👥 Total Customers: {total_customers:,}")
        logger.info(f"💰 Total Revenue: £{total_revenue:,.2f}")
        
        # Hiển thị đường dẫn HDFS
        logger.info("\n📁 DATA LOCATIONS:")
        logger.info("-" * 40)
        logger.info("📂 Raw transactions: hdfs://namenode:9000/user/retail/transactions_data")
        logger.info("📂 Analysis results: hdfs://namenode:9000/user/retail/analysis/")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("🛑 Spark Session stopped")


if __name__ == "__main__":
    run_pipeline()
