"""
Customer Segmentation Analysis (SQL-Based)
===========================================
Phan khach hang dua tren phan tich RFM
Su dung SQL thay vi ML library (khong can numpy)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min,
    percent_rank, lit, round as spark_round
)
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tao Spark Session voi MongoDB Connector"""
    
    spark = SparkSession.builder \
        .appName("CustomerSegmentation") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_transactions(spark):
    """Doc du lieu tu HDFS"""
    
    logger.info("Doc du lieu tu HDFS...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://namenode:9000/user/retail/online_retail.csv")
    
    # Tinh TotalAmount
    df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
    
    # Loc du lieu hop le
    df = df.filter(
        (col("CustomerID").isNotNull()) &
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0)
    )
    
    logger.info(f"Da doc {df.count()} giao dich hop le")
    return df


def calculate_rfm(spark, transactions):
    """
    Tinh RFM metrics cho moi khach hang:
    - Recency: So ngay tu lan mua cuoi
    - Frequency: So lan mua hang
    - Monetary: Tong chi tieu
    """
    
    logger.info("Tinh toan RFM metrics...")
    
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
    
    logger.info(f"Tinh RFM cho {rfm_df.count()} khach hang")
    return rfm_df


def assign_rfm_scores(rfm_df):
    """
    Gan diem RFM (1-5) dua tren percentile
    """
    
    logger.info("Gan diem RFM (1-5 scale)...")
    
    # Tao window cho ranking
    window = Window.orderBy(col("Recency").desc())
    rfm_df = rfm_df.withColumn("R_percentile", percent_rank().over(window))
    
    window = Window.orderBy(col("Frequency"))
    rfm_df = rfm_df.withColumn("F_percentile", percent_rank().over(window))
    
    window = Window.orderBy(col("Monetary"))
    rfm_df = rfm_df.withColumn("M_percentile", percent_rank().over(window))
    
    # Gan diem 1-5 dua tren percentile
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
    
    # Tinh RFM Score tong
    rfm_df = rfm_df.withColumn(
        "RFM_Score",
        col("R_Score") + col("F_Score") + col("M_Score")
    )
    
    return rfm_df


def assign_customer_segments(rfm_df):
    """
    Gan phan khuc khach hang dua tren RFM score
    """
    
    logger.info("Phan loai khach hang theo phan khuc...")
    
    segmented_df = rfm_df.withColumn(
        "Segment",
        when(
            (col("R_Score") >= 4) & (col("F_Score") >= 4) & (col("M_Score") >= 4),
            "Champions"  # Khach hang VIP
        ).when(
            (col("R_Score") >= 4) & (col("F_Score") >= 3),
            "Loyal Customers"  # Khach hang trung thanh
        ).when(
            (col("R_Score") >= 4) & (col("F_Score") <= 2),
            "New Customers"  # Khach hang moi
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") >= 4) & (col("M_Score") >= 4),
            "At Risk"  # Khach hang co nguy co mat
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") <= 2),
            "Lost Customers"  # Khach hang da mat
        ).when(
            (col("R_Score") >= 3) & (col("M_Score") >= 4),
            "Big Spenders"  # Khach hang chi tieu lon
        ).when(
            (col("R_Score") <= 2) & (col("F_Score") >= 3),
            "Hibernating"  # Khach hang tam nghi
        ).otherwise("Regular")  # Khach hang binh thuong
    )
    
    # Them gia tri so cho Segment de dung lam Cluster ID
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
    Phan tich thong ke tung phan khuc
    """
    
    logger.info("\n" + "="*60)
    logger.info("PHAN TICH PHAN KHUC KHACH HANG")
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
    """Luu ket qua vao HDFS"""
    
    logger.info("Luu ket qua vao HDFS...")
    
    # Luu customer segments
    output_df = segmented_df.select(
        "CustomerID", "Country", "Recency", "Frequency", "Monetary",
        "AvgOrderValue", "UniqueProducts",
        "R_Score", "F_Score", "M_Score", "RFM_Score",
        "Segment", "Cluster"
    )
    
    output_df.write \
        .mode("overwrite") \
        .parquet("hdfs://namenode:9000/user/retail/analysis/customer_clusters")
    
    # Luu segment statistics
    segment_stats.write \
        .mode("overwrite") \
        .parquet("hdfs://namenode:9000/user/retail/analysis/cluster_statistics")
    
    logger.info("Da luu vao HDFS: /user/retail/analysis/customer_clusters")
    logger.info("Da luu vao HDFS: /user/retail/analysis/cluster_statistics")


def save_to_mongodb(spark, segmented_df, segment_stats):
    """Luu ket qua vao MongoDB"""
    
    logger.info("Luu ket qua vao MongoDB...")
    
    try:
        # Luu customer clusters
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
        
        logger.info("Da luu customer_clusters vao MongoDB")
        
        # Luu segment statistics
        segment_stats.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.cluster_statistics?authSource=admin") \
            .save()
        
        logger.info("Da luu cluster_statistics vao MongoDB")
        
    except Exception as e:
        logger.warning(f"Khong the luu vao MongoDB: {e}")


def main():
    """Main function"""
    
    logger.info("")
    logger.info("="*60)
    logger.info("  CUSTOMER SEGMENTATION ANALYSIS (RFM-Based)")
    logger.info("="*60)
    logger.info("")
    
    spark = create_spark_session()
    
    try:
        # 1. Doc du lieu
        transactions = load_transactions(spark)
        
        # 2. Tinh RFM
        rfm_df = calculate_rfm(spark, transactions)
        
        # 3. Gan diem RFM
        rfm_scored = assign_rfm_scores(rfm_df)
        
        # 4. Phan loai phan khuc
        segmented_df = assign_customer_segments(rfm_scored)
        
        # 5. Phan tich
        segment_stats = analyze_segments(segmented_df)
        
        # 6. Luu ket qua
        save_to_hdfs(segmented_df, segment_stats)
        save_to_mongodb(spark, segmented_df, segment_stats)
        
        logger.info("")
        logger.info("="*60)
        logger.info("  CUSTOMER SEGMENTATION HOAN TAT!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Loi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
