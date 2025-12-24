"""
Customer Segmentation Analysis (SQL-Based)
===========================================
Phân khách hàng dựa trên phân tích RFM
Sử dụng SQL thay vì ML library (không cần numpy)
"""

from pyspark.sql import SparkSession #type: ignore
from pyspark.sql.functions import ( #type: ignore
    col, when, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min,
    percent_rank, lit, round as spark_round
)
from pyspark.sql.window import Window #type: ignore
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session với MongoDB Connector"""
    
    spark = SparkSession.builder \
        .appName("CustomerSegmentation") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_transactions(spark):   
    logger.info("Đọc dữ liệu từ HDFS...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://namenode:9000/user/retail/online_retail.csv")
    
    # Tính tổng tiền cho mỗi giao dịch
    df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
    
    # Chuyển CustomerID null thành "unknown" thay vì xóa
    df = df.withColumn("CustomerID", 
        when(col("CustomerID").isNull(), lit("unknown"))
        .otherwise(col("CustomerID").cast("string"))
    )
    
    # Lọc dữ liệu hợp lệ (không lọc CustomerID null nữa)
    df = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0)
    )
    
    logger.info(f"Đã đọc {df.count()} giao dịch hợp lệ")
    return df


def calculate_rfm(spark, transactions):
    """
    Tính RFM metrics cho mỗi khách hàng:
    - Recency: Số ngày từ lần mua cuối
    - Frequency: Số lần mua hàng
    - Monetary: Tổng chi tiêu
    """
    
    logger.info("Đang tính toán RFM metrics...")
    
    transactions.createOrReplaceTempView("trans")
    
    rfm_df = spark.sql("""
        SELECT 
            CustomerID,
            MAX(Country) as Country,
            DATEDIFF(
                (SELECT MAX(TO_DATE(InvoiceDate, 'M/d/yyyy H:mm')) FROM trans),
                MAX(TO_DATE(InvoiceDate, 'M/d/yyyy H:mm'))
            ) as Recency,
            COUNT(DISTINCT InvoiceNo) as Frequency,
            ROUND(SUM(TotalAmount), 2) as Monetary,
            ROUND(AVG(TotalAmount), 2) as AvgOrderValue,
            COUNT(DISTINCT StockCode) as UniqueProducts
        FROM trans
        GROUP BY CustomerID
        HAVING COUNT(DISTINCT InvoiceNo) >= 2
    """)
    
    logger.info(f"Tính RFM cho {rfm_df.count()} khách hàng")
    return rfm_df


def assign_rfm_scores(rfm_df):
    """
    Gán điểm RFM (1-5) dựa trên percentile
    """
    
    logger.info("Gán điểm RFM (1-5 scale)...")
    
    # Tạo window cho ranking
    window = Window.orderBy(col("Recency").desc())
    rfm_df = rfm_df.withColumn("R_percentile", percent_rank().over(window))
    
    window = Window.orderBy(col("Frequency"))
    rfm_df = rfm_df.withColumn("F_percentile", percent_rank().over(window))
    
    window = Window.orderBy(col("Monetary"))
    rfm_df = rfm_df.withColumn("M_percentile", percent_rank().over(window))
    
    # Gán điểm 1-5 dựa trên percentile
    rfm_df = rfm_df.withColumn(
        "R_Score",
        when(col("R_percentile") >= 0.8, 5)
        .when(col("R_percentile") >= 0.6, 4)
        .when(col("R_percentile") >= 0.4, 3)
        .when(col("R_percentile") >= 0.2, 2)
        .otherwise(1)
    ).withColumn(
        "F_Score",
        when(col("F_percentile") >= 0.8, 5)
        .when(col("F_percentile") >= 0.6, 4)
        .when(col("F_percentile") >= 0.4, 3)
        .when(col("F_percentile") >= 0.2, 2)
        .otherwise(1)
    ).withColumn(
        "M_Score",
        when(col("M_percentile") >= 0.8, 5)
        .when(col("M_percentile") >= 0.6, 4)
        .when(col("M_percentile") >= 0.4, 3)
        .when(col("M_percentile") >= 0.2, 2)
        .otherwise(1)
    )
    
    # Tính RFM Score tổng
    rfm_df = rfm_df.withColumn(
        "RFM_Score",
        col("R_Score") + col("F_Score") + col("M_Score")
    )
    
    return rfm_df


def assign_customer_segments(rfm_df):
    
    # Gán phân khúc khách hàng dựa trên RFM score
    
    
    logger.info("Phân loại khách hàng theo phân khúc...")
    
    segmented_df = rfm_df.withColumn(
        "Segment",
        when(
            (col("R_Score") >= 4) & (col("F_Score") >= 4) & (col("M_Score") >= 4),
            "Champions"  # Khách hàng VIP
        ).when(
            (col("R_Score") >= 4) & (col("F_Score") >= 3),
            "Loyal Customers"  # Khách hàng trung thành
        ).when(
            (col("R_Score") >= 4) & (col("F_Score") <= 2),
            "New Customers"  # Khách hàng mới
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") >= 4) & (col("M_Score") >= 4),
            "At Risk"  # Khách hàng có nguy cơ mất
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") <= 2),
            "Lost Customers"  # Khách hàng đã rời đi
        ).when(
            (col("R_Score") >= 3) & (col("M_Score") >= 4),
            "Big Spenders"  # Khách hàng chi tiêu lớn
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") >= 3),
            "Hibernating"  # Khách hàng tạm nghỉ
        ).otherwise("Regular")  # Khách hàng bình thường
    )
    
    # Thêm giá trị số cho Segment để dùng làm Cluster ID
    segmented_df = segmented_df.withColumn(
        "Cluster",
        when(col("Segment") == "Champions", 0)
        .when(col("Segment") == "Loyal Customers", 1)
        .when(col("Segment") == "Big Spenders", 2)
        .when(col("Segment") == "New Customers", 3)
        .when(col("Segment") == "Regular", 4)
        .when(col("Segment") == "At Risk", 5)
        .when(col("Segment") == "Hibernating", 6)
        .when(col("Segment") == "Lost Customers", 7)
        .otherwise(8)
    )
    
    return segmented_df


def analyze_segments(segmented_df):
    """
    Phân tích thống kê từng phân khúc
    """
    
    logger.info("\n" + "="*60)
    logger.info("PHÂN TÍCH PHÂN KHÚC KHÁCH HÀNG")
    logger.info("="*60)
    
    segment_stats = segmented_df.groupBy("Segment", "Cluster").agg(
        count("*").alias("CustomerCount"),
        spark_round(avg("Recency"), 1).alias("AvgRecency"),
        spark_round(avg("Frequency"), 1).alias("AvgFrequency"),
        spark_round(avg("Monetary"), 2).alias("AvgMonetary"),
        spark_round(spark_sum("Monetary"), 2).alias("TotalRevenue")
    ).orderBy(col("TotalRevenue").desc())
    
    segment_stats.show(truncate=False)
    
    return segment_stats


def save_to_hdfs(segmented_df, segment_stats):
    """Lưu kết quả vào HDFS"""
    
    logger.info("Lưu kết quả vào HDFS...")
    
    # Lưu customer segments
    output_df = segmented_df.select(
        "CustomerID", "Country", "Recency", "Frequency", "Monetary",
        "AvgOrderValue", "UniqueProducts",
        "R_Score", "F_Score", "M_Score", "RFM_Score",
        "Segment", "Cluster"
    )
    
    output_df.write \
        .mode("overwrite") \
        .parquet("hdfs://namenode:9000/user/retail/analysis/customer_clusters")
    
    # Lưu segment statistics
    segment_stats.write \
        .mode("overwrite") \
        .parquet("hdfs://namenode:9000/user/retail/analysis/cluster_statistics")
    
    logger.info("Đã lưu vào HDFS: /user/retail/analysis/customer_clusters")
    logger.info("Đã lưu vào HDFS: /user/retail/analysis/cluster_statistics")


def save_to_mongodb(spark, segmented_df, segment_stats):
    """Lưu kết quả vào MongoDB"""
    
    logger.info("Lưu kết quả vào MongoDB...")
    
    try:
        # Lưu customer clusters
        output_df = segmented_df.select(
            "CustomerID", "Country", "Recency", "Frequency", "Monetary",
            "AvgOrderValue", "UniqueProducts",
            "R_Score", "F_Score", "M_Score", "RFM_Score",
            "Segment", "Cluster"
        )
        
        output_df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.customer_clusters?authSource=admin") \
            .save()
        
        logger.info("Đã lưu customer_clusters vào MongoDB")
        
        # Lưu segment statistics
        segment_stats.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.cluster_statistics?authSource=admin") \
            .save()
        
        logger.info("Đã lưu cluster_statistics vào MongoDB")
        
    except Exception as e:
        logger.warning(f"Không thể lưu vào MongoDB: {e}")


def main():
    """Main function"""
    
    logger.info("")
    logger.info("="*60)
    logger.info("  PHÂN TÍCH PHÂN KHÚC KHÁCH HÀNG (DỰA TRÊN RFM)")
    logger.info("="*60)
    logger.info("")
    
    spark = create_spark_session()
    
    try:
        # 1. Đọc dữ liệu
        transactions = load_transactions(spark)
        
        # 2. Tính RFM
        rfm_df = calculate_rfm(spark, transactions)
        
        # 3. Gán điểm RFM
        rfm_scored = assign_rfm_scores(rfm_df)
        
        # 4. Phân loại phân khúc
        segmented_df = assign_customer_segments(rfm_scored)
        
        # 5. Phân tích
        segment_stats = analyze_segments(segmented_df)
        
        # 6. Lưu kết quả
        save_to_hdfs(segmented_df, segment_stats)
        save_to_mongodb(spark, segmented_df, segment_stats)
        
        logger.info("")
        logger.info("="*60)
        logger.info("  PHÂN TÍCH PHÂN KHÚC HOÀN TẤT!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
