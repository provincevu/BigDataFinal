"""
Product Recommendations using FP-Growth (MLlib)
================================================
Phân tích liên kết sản phẩm và gợi ý sản phẩm dựa trên:
1. FP-Growth Algorithm - Tìm các itemsets thường xuất hiện cùng nhau
2. Association Rules - Tạo luật gợi ý sản phẩm
"""

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import (  # type: ignore
    col, collect_list, collect_set, count, desc, explode, size,
    lit, round as spark_round, first, array_contains
)
from pyspark.ml.fpm import FPGrowth  # type: ignore
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tạo Spark Session với MongoDB Connector"""
    
    spark = SparkSession.builder \
        .appName("ProductRecommendations") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/retail_analytics?authSource=admin") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session được tạo thành công")
    return spark


def load_transactions(spark):
    """Đọc dữ liệu giao dịch từ HDFS"""
    
    logger.info("Đang đọc dữ liệu từ HDFS...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://namenode:9000/user/retail/online_retail.csv")
    
    # Lọc dữ liệu hợp lệ
    df = df.filter(
        (col("InvoiceNo").isNotNull()) &
        (col("StockCode").isNotNull()) &
        (col("Quantity") > 0) &
        (~col("InvoiceNo").startswith("C"))  # Loại bỏ đơn hàng bị hủy
    )
    
    logger.info(f"Đã đọc {df.count()} giao dịch hợp lệ")
    return df


def prepare_basket_data(df):
    """
    Chuẩn bị dữ liệu giỏ hàng cho FP-Growth
    Mỗi giỏ hàng (InvoiceNo) chứa danh sách các sản phẩm (StockCode)
    FP-Growth yêu cầu các items trong mỗi transaction phải unique
    """
    
    logger.info("Đang chuẩn bị dữ liệu giỏ hàng...")
    
    # Nhóm các sản phẩm theo hóa đơn - sử dụng collect_set để loại bỏ trùng lặp
    baskets = df.groupBy("InvoiceNo") \
        .agg(collect_set("StockCode").alias("items"))
    
    # Lọc các giỏ hàng có ít nhất 2 sản phẩm
    baskets = baskets.filter(size(col("items")) >= 2)
    
    basket_count = baskets.count()
    logger.info(f"Số giỏ hàng có >= 2 sản phẩm: {basket_count}")
    
    return baskets


def get_product_descriptions(df):
    """Lấy mô tả sản phẩm để hiển thị"""
    
    product_desc = df.groupBy("StockCode") \
        .agg(first("Description").alias("Description")) \
        .filter(col("Description").isNotNull())
    
    return product_desc


def run_fpgrowth(baskets, min_support=0.01, min_confidence=0.3):
    """
    Chạy thuật toán FP-Growth để tìm:
    1. Frequent Itemsets - Các tập sản phẩm thường xuất hiện cùng nhau
    2. Association Rules - Luật liên kết để gợi ý sản phẩm
    
    Parameters:
    - min_support: Ngưỡng support tối thiểu (mặc định 1%)
    - min_confidence: Ngưỡng confidence tối thiểu (mặc định 30%)
    """
    
    logger.info(f"Đang chạy FP-Growth với minSupport={min_support}, minConfidence={min_confidence}...")
    
    # Khởi tạo FP-Growth
    fpGrowth = FPGrowth(
        itemsCol="items",
        minSupport=min_support,
        minConfidence=min_confidence
    )
    
    # Huấn luyện mô hình
    model = fpGrowth.fit(baskets)
    
    # Lấy frequent itemsets
    freq_itemsets = model.freqItemsets
    logger.info(f"Tìm thấy {freq_itemsets.count()} frequent itemsets")
    
    # Lấy association rules
    assoc_rules = model.associationRules
    logger.info(f"Tìm thấy {assoc_rules.count()} association rules")
    
    return freq_itemsets, assoc_rules, model


def process_frequent_itemsets(freq_itemsets, product_desc, top_n=100):
    """
    Xử lý frequent itemsets để lưu vào MongoDB
    Chỉ lấy các cặp sản phẩm (2 items)
    """
    
    logger.info("Đang xử lý frequent itemsets...")
    
    # Lọc các itemsets có đúng 2 sản phẩm
    pair_itemsets = freq_itemsets.filter(size(col("items")) == 2) \
        .withColumn("product1", col("items").getItem(0)) \
        .withColumn("product2", col("items").getItem(1)) \
        .select("product1", "product2", "freq") \
        .withColumnRenamed("freq", "CoOccurrence") \
        .orderBy(desc("CoOccurrence")) \
        .limit(top_n)
    
    # Tính support (tỷ lệ xuất hiện)
    total_baskets = freq_itemsets.agg({"freq": "max"}).collect()[0][0]
    pair_itemsets = pair_itemsets.withColumn(
        "Support",
        spark_round(col("CoOccurrence") / lit(total_baskets), 4)
    )
    
    # Join với mô tả sản phẩm
    pair_itemsets = pair_itemsets \
        .join(product_desc.withColumnRenamed("StockCode", "product1")
              .withColumnRenamed("Description", "Description1"), "product1", "left") \
        .join(product_desc.withColumnRenamed("StockCode", "product2")
              .withColumnRenamed("Description", "Description2"), "product2", "left")
    
    # Chuẩn bị output
    result = pair_itemsets.select(
        col("product1").alias("StockCode1"),
        col("Description1"),
        col("product2").alias("StockCode2"),
        col("Description2"),
        "CoOccurrence",
        "Support"
    )
    
    return result


def process_association_rules(assoc_rules, product_desc, top_n=100):
    """
    Xử lý association rules để lưu vào MongoDB
    Format: Nếu mua A -> Gợi ý B (với confidence)
    """
    
    logger.info("Đang xử lý association rules...")
    
    # Lọc các rules có antecedent là 1 sản phẩm
    single_rules = assoc_rules.filter(size(col("antecedent")) == 1) \
        .withColumn("SourceProduct", col("antecedent").getItem(0)) \
        .withColumn("RecommendedProduct", col("consequent").getItem(0)) \
        .select(
            "SourceProduct",
            "RecommendedProduct",
            spark_round(col("confidence"), 4).alias("Confidence"),
            spark_round(col("lift"), 4).alias("Lift"),
            spark_round(col("support"), 4).alias("Support")
        ) \
        .orderBy(desc("Confidence"), desc("Lift")) \
        .limit(top_n)
    
    # Join với mô tả sản phẩm
    single_rules = single_rules \
        .join(product_desc.withColumnRenamed("StockCode", "SourceProduct")
              .withColumnRenamed("Description", "SourceDescription"), "SourceProduct", "left") \
        .join(product_desc.withColumnRenamed("StockCode", "RecommendedProduct")
              .withColumnRenamed("Description", "RecommendedDescription"), "RecommendedProduct", "left")
    
    return single_rules


def save_to_mongodb(df, collection_name):
    """Lưu DataFrame vào MongoDB"""
    
    logger.info(f"Đang lưu vào MongoDB: {collection_name}")
    
    try:
        df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("connection.uri", "mongodb://admin:admin123@mongodb:27017") \
            .option("database", "retail_analytics") \
            .option("collection", collection_name) \
            .option("authSource", "admin") \
            .save()
        
        count = df.count()
        logger.info(f"Đã lưu {count} bản ghi vào MongoDB: {collection_name}")
        
    except Exception as e:
        logger.error(f"Lỗi khi lưu vào MongoDB: {e}")
        raise


def save_to_hdfs(df, path):
    """Lưu DataFrame vào HDFS"""
    
    logger.info(f"Đang lưu vào HDFS: {path}")
    
    df.write \
        .mode("overwrite") \
        .parquet(path)
    
    logger.info(f"Đã lưu vào HDFS: {path}")


def main():
    """Main function"""
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("  PHÂN TÍCH GỢI Ý SẢN PHẨM (FP-GROWTH)")
    logger.info("=" * 60)
    logger.info("")
    
    spark = create_spark_session()
    
    try:
        # 1. Đọc dữ liệu
        df = load_transactions(spark)
        
        # 2. Lấy mô tả sản phẩm
        product_desc = get_product_descriptions(df)
        
        # 3. Chuẩn bị dữ liệu giỏ hàng
        baskets = prepare_basket_data(df)
        baskets.cache()
        
        # 4. Chạy FP-Growth
        freq_itemsets, assoc_rules, model = run_fpgrowth(
            baskets,
            min_support=0.01,    # 1% support
            min_confidence=0.2   # 20% confidence
        )
        
        # 5. Xử lý kết quả
        product_associations = process_frequent_itemsets(freq_itemsets, product_desc, top_n=100)
        product_recommendations = process_association_rules(assoc_rules, product_desc, top_n=100)
        
        # 6. Hiển thị kết quả
        logger.info("\n" + "=" * 60)
        logger.info("TOP 10 CẶP SẢN PHẨM THƯỜNG MUA CÙNG NHAU:")
        logger.info("=" * 60)
        product_associations.show(10, truncate=False)
        
        logger.info("\n" + "=" * 60)
        logger.info("TOP 10 GỢI Ý SẢN PHẨM:")
        logger.info("=" * 60)
        product_recommendations.show(10, truncate=False)
        
        # 7. Lưu vào MongoDB
        logger.info("\n" + "=" * 60)
        logger.info("Đang lưu kết quả...")
        
        save_to_mongodb(product_associations, "product_associations")
        save_to_mongodb(product_recommendations, "product_recommendations")
        
        # 8. Lưu vào HDFS
        save_to_hdfs(product_associations, "hdfs://namenode:9000/user/retail/analysis/product_associations")
        save_to_hdfs(product_recommendations, "hdfs://namenode:9000/user/retail/analysis/product_recommendations")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("  PHÂN TÍCH GỢI Ý SẢN PHẨM HOÀN TẤT!")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Kết quả đã được lưu vào:")
        logger.info("  - MongoDB: product_associations, product_recommendations")
        logger.info("  - HDFS: /user/retail/analysis/product_*")
        
    except Exception as e:
        logger.error(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        logger.info("Spark Session đã dừng")


if __name__ == "__main__":
    main()
