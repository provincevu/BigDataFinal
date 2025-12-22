"""
Retail Data ETL Pipeline - Simple Version (No Hive)
===================================================
Pipeline đơn giản xử lý dữ liệu bán lẻ:
1. Load dữ liệu từ CSV
2. Xử lý và làm sạch dữ liệu
3. Lưu vào HDFS và MongoDB trực tiếp (không cần Hive)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, 
    month, year, dayofweek, hour, 
    when, lit, round as spark_round,
    desc, asc, to_date, to_timestamp,
    regexp_replace, trim, upper,
    row_number, dense_rank, max as spark_max
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
    """Tạo Spark Session - No Hive, MongoDB only"""
    
    spark = SparkSession.builder \
        .appName("RetailDataPipelineSimple") \
        .master("local[*]") \
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session created successfully (No Hive)")
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
    """Load và làm sạch dữ liệu từ CSV"""
    
    logger.info(f"📂 Loading data from: {input_path}")
    
    schema = define_schema()
    
    # Load dữ liệu
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(input_path)
    
    raw_count = df.count()
    logger.info(f"📊 Raw records: {raw_count}")
    
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
    
    clean_count = df_cleaned.count()
    logger.info(f"✅ Cleaned records: {clean_count}")
    
    return df_cleaned


def save_to_hdfs(df, path):
    """Lưu DataFrame vào HDFS"""
    
    logger.info(f"💾 Saving data to HDFS: {path}")
    
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(path)
    
    logger.info(f"✅ Data saved to HDFS successfully")


def save_to_mongodb(df, collection_name):
    """Lưu DataFrame vào MongoDB"""
    
    logger.info(f"📤 Saving to MongoDB: {collection_name}")
    
    try:
        df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", f"mongodb://admin:admin123@mongodb:27017") \
            .option("database", "retail_analytics") \
            .option("collection", collection_name) \
            .option("authSource", "admin") \
            .save()
        
        count = df.count()
        logger.info(f"✅ Saved {count} records to MongoDB: {collection_name}")
    except Exception as e:
        logger.error(f"❌ Failed to save to MongoDB {collection_name}: {e}")
        raise


def analyze_revenue(df):
    """Phân tích doanh thu"""
    
    logger.info("📈 Analyzing revenue...")
    
    # Monthly revenue
    monthly_revenue = df.groupBy("Year", "Month") \
        .agg(
            count("InvoiceNo").alias("TotalOrders"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue"),
            spark_round(avg("TotalAmount"), 2).alias("AvgOrderValue")
        ) \
        .orderBy("Year", "Month")
    
    # Daily revenue (by day of week)
    daily_revenue = df.groupBy("DayOfWeek") \
        .agg(
            count("InvoiceNo").alias("TotalOrders"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue")
        ) \
        .withColumn("DayName", 
            when(col("DayOfWeek") == 1, "Sunday")
            .when(col("DayOfWeek") == 2, "Monday")
            .when(col("DayOfWeek") == 3, "Tuesday")
            .when(col("DayOfWeek") == 4, "Wednesday")
            .when(col("DayOfWeek") == 5, "Thursday")
            .when(col("DayOfWeek") == 6, "Friday")
            .when(col("DayOfWeek") == 7, "Saturday")
            .otherwise("Unknown")
        ) \
        .orderBy("DayOfWeek")
    
    # Hourly revenue
    hourly_revenue = df.groupBy("Hour") \
        .agg(
            count("InvoiceNo").alias("TotalOrders"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue")
        ) \
        .orderBy("Hour")
    
    logger.info("✅ Revenue analysis completed")
    
    return monthly_revenue, daily_revenue, hourly_revenue


def analyze_products(df, top_n=20):
    """Phân tích sản phẩm bán chạy"""
    
    logger.info(f"🏆 Analyzing top {top_n} products...")
    
    # Top by quantity
    top_by_quantity = df.groupBy("StockCode", "Description") \
        .agg(
            spark_sum("Quantity").alias("TotalQuantity"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue"),
            count("InvoiceNo").alias("TotalOrders")
        ) \
        .orderBy(desc("TotalQuantity")) \
        .limit(top_n)
    
    # Top by revenue
    top_by_revenue = df.groupBy("StockCode", "Description") \
        .agg(
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue"),
            spark_sum("Quantity").alias("TotalQuantity"),
            count("InvoiceNo").alias("TotalOrders"),
            spark_round(avg("UnitPrice"), 2).alias("AvgPrice")
        ) \
        .orderBy(desc("TotalRevenue")) \
        .limit(top_n)
    
    logger.info("✅ Product analysis completed")
    
    return top_by_quantity, top_by_revenue


def analyze_customers(df):
    """Phân tích khách hàng RFM"""
    
    logger.info("👥 Analyzing customers (RFM)...")
    
    # Lấy ngày cuối cùng
    max_date = df.agg(spark_max("InvoiceDate")).collect()[0][0]
    
    # Tính RFM
    customer_rfm = df.groupBy("CustomerID", "Country") \
        .agg(
            count("InvoiceNo").alias("Frequency"),
            spark_round(spark_sum("TotalAmount"), 2).alias("Monetary"),
            spark_max("InvoiceDate").alias("LastPurchase")
        )
    
    # Add Recency (ngày từ lần mua cuối)
    customer_rfm = customer_rfm.withColumn(
        "Recency",
        when(col("LastPurchase").isNotNull(), 
             ((lit(max_date).cast("long") - col("LastPurchase").cast("long")) / 86400).cast(IntegerType()))
        .otherwise(0)
    )
    
    # Calculate RFM scores using Window functions
    r_window = Window.orderBy(desc("Recency"))
    f_window = Window.orderBy("Frequency")
    m_window = Window.orderBy("Monetary")
    
    customer_rfm = customer_rfm \
        .withColumn("R_Score", ((row_number().over(r_window) - 1) * 5 / customer_rfm.count() + 1).cast(IntegerType())) \
        .withColumn("F_Score", ((row_number().over(f_window) - 1) * 5 / customer_rfm.count() + 1).cast(IntegerType())) \
        .withColumn("M_Score", ((row_number().over(m_window) - 1) * 5 / customer_rfm.count() + 1).cast(IntegerType()))
    
    # Limit scores to 1-5
    customer_rfm = customer_rfm \
        .withColumn("R_Score", when(col("R_Score") > 5, 5).when(col("R_Score") < 1, 1).otherwise(col("R_Score"))) \
        .withColumn("F_Score", when(col("F_Score") > 5, 5).when(col("F_Score") < 1, 1).otherwise(col("F_Score"))) \
        .withColumn("M_Score", when(col("M_Score") > 5, 5).when(col("M_Score") < 1, 1).otherwise(col("M_Score")))
    
    # Add segment
    customer_rfm = customer_rfm.withColumn(
        "CustomerSegment",
        when((col("R_Score") >= 4) & (col("F_Score") >= 4) & (col("M_Score") >= 4), "Champions")
        .when((col("R_Score") >= 3) & (col("F_Score") >= 3) & (col("M_Score") >= 3), "Loyal Customers")
        .when((col("R_Score") >= 4) & (col("F_Score") <= 2), "New Customers")
        .when((col("R_Score") <= 2) & (col("F_Score") >= 3), "At Risk")
        .when((col("R_Score") <= 2) & (col("F_Score") <= 2) & (col("M_Score") <= 2), "Lost")
        .otherwise("Regular")
    )
    
    # Segment statistics
    segment_stats = customer_rfm.groupBy("CustomerSegment") \
        .agg(
            count("*").alias("CustomerCount"),
            spark_round(avg("Monetary"), 2).alias("AvgMonetary"),
            spark_round(avg("Frequency"), 2).alias("AvgFrequency"),
            spark_round(avg("Recency"), 2).alias("AvgRecency")
        ) \
        .orderBy(desc("CustomerCount"))
    
    logger.info("✅ Customer RFM analysis completed")
    
    return customer_rfm, segment_stats


def analyze_countries(df):
    """Phân tích theo quốc gia"""
    
    logger.info("🌍 Analyzing countries...")
    
    country_stats = df.groupBy("Country") \
        .agg(
            count("*").alias("TotalTransactions"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue"),
            spark_round(avg("TotalAmount"), 2).alias("AvgOrderValue"),
            spark_sum("Quantity").alias("TotalQuantity")
        ) \
        .orderBy(desc("TotalRevenue"))
    
    logger.info("✅ Country analysis completed")
    
    return country_stats


def analyze_monthly_trend(df):
    """Phân tích xu hướng theo tháng"""
    
    logger.info("📊 Analyzing monthly trends...")
    
    monthly_trend = df.groupBy("Year", "Month") \
        .agg(
            count("*").alias("TotalTransactions"),
            spark_round(spark_sum("TotalAmount"), 2).alias("TotalRevenue"),
            spark_sum("Quantity").alias("TotalQuantity")
        ) \
        .orderBy("Year", "Month")
    
    logger.info("✅ Monthly trend analysis completed")
    
    return monthly_trend


def save_transactions_sample(df):
    """Lưu mẫu giao dịch vào MongoDB"""
    
    logger.info("📤 Saving transaction sample to MongoDB...")
    
    # Lấy sample 10000 records để hiển thị
    sample_df = df.limit(10000)
    
    save_to_mongodb(sample_df, "transactions")
    
    logger.info("✅ Transaction sample saved")


def run_pipeline():
    """Chạy toàn bộ pipeline ETL"""
    
    logger.info("🚀 Starting Retail Data Pipeline (Simple Version)...")
    logger.info("=" * 60)
    
    # 1. Tạo Spark Session
    spark = create_spark_session()
    
    try:
        # 2. Load và làm sạch dữ liệu
        input_path = "/data/online_retail.csv"
        df = load_and_clean_data(spark, input_path)
        
        # Cache để tăng tốc
        df.cache()
        
        # 3. Lưu vào HDFS
        logger.info("=" * 60)
        logger.info("💾 Saving to HDFS...")
        save_to_hdfs(df, "hdfs://namenode:9000/user/retail/processed_data")
        
        # 4. Chạy các phân tích
        logger.info("=" * 60)
        logger.info("🔍 Running Analytics...")
        
        monthly_revenue, daily_revenue, hourly_revenue = analyze_revenue(df)
        top_by_quantity, top_by_revenue = analyze_products(df)
        customer_rfm, segment_stats = analyze_customers(df)
        country_stats = analyze_countries(df)
        monthly_trend = analyze_monthly_trend(df)
        
        # 5. Lưu vào MongoDB
        logger.info("=" * 60)
        logger.info("💾 Saving to MongoDB...")
        
        save_to_mongodb(monthly_revenue, "monthly_revenue")
        save_to_mongodb(daily_revenue, "daily_revenue")
        save_to_mongodb(hourly_revenue, "hourly_revenue")
        save_to_mongodb(top_by_quantity, "top_products_quantity")
        save_to_mongodb(top_by_revenue, "top_products_revenue")
        save_to_mongodb(customer_rfm, "customer_rfm")
        save_to_mongodb(segment_stats, "customer_segments")
        save_to_mongodb(country_stats, "country_performance")
        save_to_mongodb(monthly_trend, "monthly_trend")
        
        # Lưu sample transactions
        save_transactions_sample(df)
        
        # 6. Tóm tắt
        logger.info("=" * 60)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 60)
        
        total_records = df.count()
        total_revenue = df.agg(spark_sum("TotalAmount")).collect()[0][0]
        
        logger.info(f"\n📋 SUMMARY:")
        logger.info(f"-" * 40)
        logger.info(f"📊 Total Records: {total_records:,}")
        logger.info(f"💰 Total Revenue: £{total_revenue:,.2f}")
        logger.info(f"\n📁 Data saved to:")
        logger.info(f"  - HDFS: hdfs://namenode:9000/user/retail/processed_data")
        logger.info(f"  - MongoDB: retail_analytics database")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        logger.info("🛑 Spark Session stopped")


if __name__ == "__main__":
    run_pipeline()
