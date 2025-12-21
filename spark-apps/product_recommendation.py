"""
Product Recommendation System using Collaborative Filtering
=============================================================
Xây dựng hệ thống gợi ý sản phẩm dựa trên hành vi mua sắm
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, 
    collect_list, explode, array_distinct,
    row_number, desc
)
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session"""
    
    spark = SparkSession.builder \
        .appName("ProductRecommendation") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_interaction_data(spark):
    """
    Chuẩn bị dữ liệu tương tác user-product
    Tính rating dựa trên số lần mua và số lượng
    """
    
    logger.info("📊 Preparing user-product interaction data...")
    
    # Tạo implicit rating từ purchase behavior
    interactions = spark.sql("""
        SELECT 
            CustomerID,
            StockCode,
            SUM(Quantity) as TotalQuantity,
            COUNT(*) as PurchaseCount,
            SUM(TotalAmount) as TotalSpent,
            -- Implicit rating based on purchase frequency and quantity
            LEAST(
                LOG(SUM(Quantity) + 1) * LOG(COUNT(*) + 1) * 2,
                5.0
            ) as ImplicitRating
        FROM retail_db.transactions
        WHERE CustomerID IS NOT NULL
        GROUP BY CustomerID, StockCode
    """)
    
    logger.info(f"✅ Interactions prepared: {interactions.count()} records")
    
    return interactions


def create_product_lookup(spark):
    """Tạo bảng mapping StockCode với ProductIndex"""
    
    products = spark.sql("""
        SELECT DISTINCT 
            StockCode,
            FIRST(Description) as Description
        FROM retail_db.transactions
        GROUP BY StockCode
    """)
    
    # Index StockCode
    indexer = StringIndexer(inputCol="StockCode", outputCol="ProductIndex")
    indexed = indexer.fit(products).transform(products)
    
    return indexed


def train_als_model(interactions, product_lookup):
    """
    Train ALS (Alternating Least Squares) model
    """
    
    logger.info("🎓 Training ALS recommendation model...")
    
    # Join với product index
    indexer = StringIndexer(inputCol="StockCode", outputCol="ProductIndex")
    model_indexer = indexer.fit(interactions)
    data = model_indexer.transform(interactions)
    
    data = data.withColumn("ProductIndex", col("ProductIndex").cast("integer"))
    data = data.withColumn("CustomerID", col("CustomerID").cast("integer"))
    
    # Train/Test split
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    
    # ALS Model
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="CustomerID",
        itemCol="ProductIndex",
        ratingCol="ImplicitRating",
        coldStartStrategy="drop",
        implicitPrefs=True
    )
    
    model = als.fit(train)
    
    # Evaluate
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="ImplicitRating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    logger.info(f"📈 RMSE: {rmse:.4f}")
    
    return model, model_indexer


def generate_recommendations(spark, model, product_indexer, n_recommendations=10):
    """
    Generate product recommendations for all users
    """
    
    logger.info(f"🎁 Generating top {n_recommendations} recommendations per user...")
    
    # Get recommendations for all users
    user_recs = model.recommendForAllUsers(n_recommendations)
    
    # Explode recommendations
    from pyspark.sql.functions import explode, col
    
    exploded = user_recs.select(
        col("CustomerID"),
        explode(col("recommendations")).alias("rec")
    ).select(
        col("CustomerID"),
        col("rec.ProductIndex").alias("ProductIndex"),
        col("rec.rating").alias("Score")
    )
    
    # Lấy thông tin sản phẩm
    products = spark.sql("""
        SELECT DISTINCT 
            StockCode,
            FIRST(Description) as Description,
            ROUND(AVG(UnitPrice), 2) as AvgPrice
        FROM retail_db.transactions
        GROUP BY StockCode
    """)
    
    # Index products
    indexed_products = product_indexer.transform(products)
    indexed_products = indexed_products.withColumn("ProductIndex", col("ProductIndex").cast("integer"))
    
    # Join với product info
    recommendations = exploded.join(
        indexed_products,
        on="ProductIndex",
        how="left"
    ).select(
        "CustomerID",
        "StockCode",
        "Description",
        "AvgPrice",
        "Score"
    )
    
    return recommendations


def create_product_associations(spark):
    """
    Tìm các sản phẩm thường được mua cùng nhau
    (Market Basket Analysis)
    """
    
    logger.info("🛒 Finding frequently bought together products...")
    
    associations = spark.sql("""
        WITH basket AS (
            SELECT 
                InvoiceNo,
                COLLECT_SET(StockCode) as Products
            FROM retail_db.transactions
            GROUP BY InvoiceNo
            HAVING SIZE(COLLECT_SET(StockCode)) >= 2
        ),
        pairs AS (
            SELECT 
                InvoiceNo,
                Products[i] as Product1,
                Products[j] as Product2
            FROM basket
            LATERAL VIEW POSEXPLODE(Products) t1 AS i, p1
            LATERAL VIEW POSEXPLODE(Products) t2 AS j, p2
            WHERE i < j
        )
        SELECT 
            Product1,
            Product2,
            COUNT(*) as CoOccurrence
        FROM pairs
        GROUP BY Product1, Product2
        HAVING COUNT(*) >= 20
        ORDER BY CoOccurrence DESC
        LIMIT 500
    """)
    
    # Thêm thông tin sản phẩm
    product_info = spark.sql("""
        SELECT DISTINCT 
            StockCode,
            FIRST(Description) as Description
        FROM retail_db.transactions
        GROUP BY StockCode
    """)
    
    associations_with_names = associations \
        .join(product_info.alias("p1"), col("Product1") == col("p1.StockCode")) \
        .select(
            col("Product1"),
            col("p1.Description").alias("Product1_Name"),
            col("Product2"),
            col("CoOccurrence")
        ) \
        .join(product_info.alias("p2"), col("Product2") == col("p2.StockCode")) \
        .select(
            col("Product1"),
            col("Product1_Name"),
            col("Product2"),
            col("p2.Description").alias("Product2_Name"),
            col("CoOccurrence")
        )
    
    # Lưu vào Hive
    associations_with_names.write.mode("overwrite").saveAsTable("retail_db.product_associations")
    
    logger.info(f"✅ Found {associations_with_names.count()} product associations")
    
    return associations_with_names


def main():
    """Main function"""
    
    logger.info("🚀 Starting Product Recommendation System...")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # 1. Chuẩn bị dữ liệu tương tác
        interactions = prepare_interaction_data(spark)
        
        # 2. Tạo product lookup
        product_lookup = create_product_lookup(spark)
        
        # 3. Train ALS model
        model, indexer = train_als_model(interactions, product_lookup)
        
        # 4. Generate recommendations
        recommendations = generate_recommendations(spark, model, indexer)
        
        # Lưu vào Hive
        recommendations.write.mode("overwrite").saveAsTable("retail_db.product_recommendations")
        logger.info("✅ Recommendations saved to Hive")
        
        # 5. Market Basket Analysis
        associations = create_product_associations(spark)
        
        # Show sample recommendations
        logger.info("\n📋 Sample Recommendations:")
        recommendations.show(20, truncate=False)
        
        logger.info("=" * 60)
        logger.info("✅ Recommendation System completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Recommendation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
