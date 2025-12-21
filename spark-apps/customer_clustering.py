"""
Customer Clustering Analysis using K-Means
============================================
Nhóm khách hàng có hành vi mua sắm giống nhau
sử dụng thuật toán K-Means Clustering
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, 
    max as spark_max, min as spark_min,
    datediff, round as spark_round
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session"""
    
    spark = SparkSession.builder \
        .appName("CustomerClustering") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_customer_features(spark):
    """
    Chuẩn bị features cho clustering:
    - Recency: Số ngày từ lần mua cuối
    - Frequency: Số lần mua hàng
    - Monetary: Tổng chi tiêu
    - AvgOrderValue: Giá trị đơn hàng trung bình
    - UniqueProducts: Số sản phẩm unique đã mua
    - AvgQuantity: Số lượng trung bình mỗi đơn
    """
    
    logger.info("📊 Preparing customer features for clustering...")
    
    customer_features = spark.sql("""
        SELECT 
            CustomerID,
            Country,
            DATEDIFF(
                (SELECT MAX(InvoiceDate) FROM retail_db.transactions),
                MAX(InvoiceDate)
            ) as Recency,
            COUNT(DISTINCT InvoiceNo) as Frequency,
            ROUND(SUM(TotalAmount), 2) as Monetary,
            ROUND(AVG(TotalAmount), 2) as AvgOrderValue,
            COUNT(DISTINCT StockCode) as UniqueProducts,
            ROUND(AVG(Quantity), 2) as AvgQuantity,
            ROUND(SUM(Quantity), 0) as TotalQuantity
        FROM retail_db.transactions
        GROUP BY CustomerID, Country
        HAVING COUNT(DISTINCT InvoiceNo) >= 2
    """)
    
    logger.info(f"✅ Customer features prepared: {customer_features.count()} customers")
    
    return customer_features


def find_optimal_k(scaled_data, max_k=10):
    """Tìm số cluster tối ưu bằng Elbow method"""
    
    logger.info("🔍 Finding optimal number of clusters...")
    
    costs = []
    evaluator = ClusteringEvaluator()
    
    for k in range(2, max_k + 1):
        kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("scaled_features")
        model = kmeans.fit(scaled_data)
        
        # Inertia (Within Set Sum of Squared Errors)
        cost = model.summary.trainingCost
        
        # Silhouette score
        predictions = model.transform(scaled_data)
        silhouette = evaluator.evaluate(predictions)
        
        costs.append({
            'k': k,
            'cost': cost,
            'silhouette': silhouette
        })
        
        logger.info(f"  K={k}: Cost={cost:.2f}, Silhouette={silhouette:.4f}")
    
    # Tìm K tốt nhất dựa trên silhouette score
    best_k = max(costs, key=lambda x: x['silhouette'])['k']
    logger.info(f"✅ Optimal K = {best_k}")
    
    return best_k, costs


def run_kmeans_clustering(spark, customer_features, n_clusters=5):
    """
    Chạy K-Means Clustering
    """
    
    logger.info(f"🎯 Running K-Means with {n_clusters} clusters...")
    
    # Chọn các features cho clustering
    feature_cols = ['Recency', 'Frequency', 'Monetary', 'AvgOrderValue', 
                    'UniqueProducts', 'AvgQuantity']
    
    # Vector Assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    df_vector = assembler.transform(customer_features)
    
    # Chuẩn hóa features
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_vector)
    df_scaled = scaler_model.transform(df_vector)
    
    # Train K-Means model
    kmeans = KMeans() \
        .setK(n_clusters) \
        .setSeed(42) \
        .setFeaturesCol("scaled_features") \
        .setPredictionCol("Cluster")
    
    model = kmeans.fit(df_scaled)
    
    # Predict clusters
    predictions = model.transform(df_scaled)
    
    # Evaluate
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    logger.info(f"📈 Silhouette Score: {silhouette:.4f}")
    
    # Tính cluster centers
    centers = model.clusterCenters()
    logger.info("\n📍 Cluster Centers:")
    for i, center in enumerate(centers):
        logger.info(f"  Cluster {i}: {[round(x, 2) for x in center]}")
    
    return predictions, model


def analyze_clusters(spark, predictions):
    """
    Phân tích đặc điểm của từng cluster
    """
    
    logger.info("🔬 Analyzing cluster characteristics...")
    
    # Tạo view tạm thời
    predictions.createOrReplaceTempView("clustered_customers")
    
    # Thống kê theo cluster
    cluster_stats = spark.sql("""
        SELECT 
            Cluster,
            COUNT(*) as CustomerCount,
            ROUND(AVG(Recency), 1) as AvgRecency,
            ROUND(AVG(Frequency), 1) as AvgFrequency,
            ROUND(AVG(Monetary), 2) as AvgMonetary,
            ROUND(AVG(AvgOrderValue), 2) as AvgOrderValue,
            ROUND(AVG(UniqueProducts), 1) as AvgUniqueProducts,
            ROUND(SUM(Monetary), 2) as TotalRevenue
        FROM clustered_customers
        GROUP BY Cluster
        ORDER BY AvgMonetary DESC
    """)
    
    # Gán nhãn cho clusters dựa trên đặc điểm
    cluster_labels = spark.sql("""
        SELECT 
            Cluster,
            CASE 
                WHEN AvgRecency <= 30 AND AvgFrequency >= 10 AND AvgMonetary >= 1000 
                    THEN 'VIP Customers'
                WHEN AvgRecency <= 60 AND AvgFrequency >= 5 AND AvgMonetary >= 500 
                    THEN 'Loyal Customers'
                WHEN AvgRecency <= 30 AND AvgFrequency <= 3 
                    THEN 'New Customers'
                WHEN AvgRecency >= 90 AND AvgFrequency <= 2 
                    THEN 'Lost Customers'
                WHEN AvgRecency >= 60 AND AvgMonetary >= 300 
                    THEN 'At Risk'
                ELSE 'Regular Customers'
            END as ClusterLabel,
            CustomerCount,
            AvgRecency,
            AvgFrequency,
            AvgMonetary,
            TotalRevenue
        FROM (
            SELECT 
                Cluster,
                COUNT(*) as CustomerCount,
                AVG(Recency) as AvgRecency,
                AVG(Frequency) as AvgFrequency,
                AVG(Monetary) as AvgMonetary,
                SUM(Monetary) as TotalRevenue
            FROM clustered_customers
            GROUP BY Cluster
        ) stats
        ORDER BY AvgMonetary DESC
    """)
    
    cluster_stats.show()
    cluster_labels.show()
    
    # Lưu kết quả vào Hive
    result = predictions.select(
        "CustomerID", "Country", "Recency", "Frequency", 
        "Monetary", "AvgOrderValue", "UniqueProducts", "Cluster"
    )
    
    result.write.mode("overwrite").saveAsTable("retail_db.customer_clusters")
    cluster_stats.write.mode("overwrite").saveAsTable("retail_db.cluster_statistics")
    
    logger.info("✅ Cluster analysis saved to Hive")
    
    return cluster_stats


def save_to_mongodb(predictions):
    """Lưu kết quả clustering vào MongoDB"""
    
    logger.info("📤 Saving clustering results to MongoDB...")
    
    result = predictions.select(
        "CustomerID", "Country", "Recency", "Frequency", 
        "Monetary", "AvgOrderValue", "UniqueProducts", "Cluster"
    )
    
    result.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics.customer_clusters?authSource=admin") \
        .save()
    
    logger.info("✅ Clustering results saved to MongoDB")


def main():
    """Main function"""
    
    logger.info("🚀 Starting Customer Clustering Analysis...")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # 1. Chuẩn bị features
        customer_features = prepare_customer_features(spark)
        
        # 2. Chạy K-Means
        predictions, model = run_kmeans_clustering(spark, customer_features, n_clusters=5)
        
        # 3. Phân tích clusters
        cluster_stats = analyze_clusters(spark, predictions)
        
        # 4. Lưu vào MongoDB
        try:
            save_to_mongodb(predictions)
        except Exception as e:
            logger.warning(f"⚠️ Could not save to MongoDB: {e}")
        
        logger.info("=" * 60)
        logger.info("✅ Customer Clustering completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Clustering failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
