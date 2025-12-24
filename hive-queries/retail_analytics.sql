-- Sử dụng các queries này trong Hue hoặc Hive CLI


-- Tạo database
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- =====================================================
-- 1. TẠO BẢNG TRANSACTIONS TỪ CSV
-- =====================================================

CREATE EXTERNAL TABLE IF NOT EXISTS transactions_raw (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice DOUBLE,
    CustomerID INT,
    Country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/retail_db.db/transactions_raw'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Load dữ liệu từ HDFS
-- LOAD DATA INPATH '/user/retail/online_retail.csv' INTO TABLE transactions_raw;

-- =====================================================
-- 2. TẠO BẢNG ĐÃ XỬ LÝ
-- =====================================================

CREATE TABLE IF NOT EXISTS transactions AS
SELECT 
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country,
    ROUND(Quantity * UnitPrice, 2) as TotalAmount,
    YEAR(InvoiceDate) as Year,
    MONTH(InvoiceDate) as Month,
    DAYOFWEEK(InvoiceDate) as DayOfWeek,
    HOUR(InvoiceDate) as Hour
FROM transactions_raw
WHERE CustomerID IS NOT NULL
  AND Quantity > 0
  AND UnitPrice > 0
  AND InvoiceNo NOT LIKE 'C%';

-- =====================================================
-- 3. PHÂN TÍCH DOANH THU THEO THỜI GIAN
-- =====================================================

-- Doanh thu theo tháng
CREATE TABLE IF NOT EXISTS monthly_revenue AS
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
ORDER BY Year, Month;

-- Doanh thu theo ngày trong tuần
CREATE TABLE IF NOT EXISTS daily_revenue AS
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
ORDER BY DayOfWeek;

-- Doanh thu theo giờ
CREATE TABLE IF NOT EXISTS hourly_revenue AS
SELECT 
    Hour,
    COUNT(DISTINCT InvoiceNo) as TotalOrders,
    ROUND(SUM(TotalAmount), 2) as TotalRevenue
FROM transactions
GROUP BY Hour
ORDER BY Hour;

-- =====================================================
-- 4. TOP SẢN PHẨM BÁN CHẠY
-- =====================================================

-- Top sản phẩm theo số lượng
CREATE TABLE IF NOT EXISTS top_products_by_quantity AS
SELECT 
    StockCode,
    FIRST(Description) as Description,
    SUM(Quantity) as TotalQuantity,
    COUNT(DISTINCT InvoiceNo) as TotalOrders,
    COUNT(DISTINCT CustomerID) as TotalCustomers,
    ROUND(SUM(TotalAmount), 2) as TotalRevenue
FROM transactions
GROUP BY StockCode
ORDER BY TotalQuantity DESC
LIMIT 50;

-- Top sản phẩm theo doanh thu
CREATE TABLE IF NOT EXISTS top_products_by_revenue AS
SELECT 
    StockCode,
    FIRST(Description) as Description,
    ROUND(SUM(TotalAmount), 2) as TotalRevenue,
    SUM(Quantity) as TotalQuantity,
    COUNT(DISTINCT InvoiceNo) as TotalOrders,
    ROUND(AVG(UnitPrice), 2) as AvgPrice
FROM transactions
GROUP BY StockCode
ORDER BY TotalRevenue DESC
LIMIT 50;

-- =====================================================
-- 5. PHÂN TÍCH KHÁCH HÀNG - RFM
-- =====================================================

-- Tính RFM Scores
CREATE TABLE IF NOT EXISTS customer_rfm AS
WITH max_date AS (
    SELECT MAX(InvoiceDate) as MaxDate FROM transactions
),
customer_base AS (
    SELECT 
        CustomerID,
        Country,
        DATEDIFF((SELECT MaxDate FROM max_date), MAX(InvoiceDate)) as Recency,
        COUNT(DISTINCT InvoiceNo) as Frequency,
        ROUND(SUM(TotalAmount), 2) as Monetary
    FROM transactions
    GROUP BY CustomerID, Country
),
rfm_scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY Recency DESC) as R_Score,
        NTILE(5) OVER (ORDER BY Frequency) as F_Score,
        NTILE(5) OVER (ORDER BY Monetary) as M_Score
    FROM customer_base
)
SELECT 
    *,
    (R_Score + F_Score + M_Score) as RFM_Score,
    CONCAT(CAST(R_Score AS STRING), CAST(F_Score AS STRING), CAST(M_Score AS STRING)) as RFM_Segment,
    CASE 
        WHEN R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4 THEN 'Champions'
        WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Loyal Customers'
        WHEN R_Score >= 4 AND F_Score <= 2 THEN 'New Customers'
        WHEN R_Score <= 2 AND F_Score >= 3 THEN 'At Risk'
        WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Lost'
        ELSE 'Regular'
    END as CustomerSegment
FROM rfm_scores;

-- Thống kê theo segment
CREATE TABLE IF NOT EXISTS customer_segments AS
SELECT 
    CustomerSegment,
    COUNT(*) as CustomerCount,
    ROUND(AVG(Monetary), 2) as AvgMonetary,
    ROUND(AVG(Frequency), 2) as AvgFrequency,
    ROUND(AVG(Recency), 2) as AvgRecency,
    ROUND(SUM(Monetary), 2) as TotalMonetary
FROM customer_rfm
GROUP BY CustomerSegment
ORDER BY TotalMonetary DESC;

-- =====================================================
-- 6. PHÂN TÍCH THEO QUỐC GIA
-- =====================================================

CREATE TABLE IF NOT EXISTS country_performance AS
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
ORDER BY TotalRevenue DESC;

-- =====================================================
-- 7. XU HƯỚNG MUA HÀNG
-- =====================================================

-- Trend theo tháng với so sánh
CREATE TABLE IF NOT EXISTS monthly_trend AS
SELECT 
    Year,
    Month,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    COUNT(DISTINCT InvoiceNo) as TotalOrders,
    ROUND(SUM(TotalAmount), 2) as TotalRevenue,
    ROUND(SUM(TotalAmount) / COUNT(DISTINCT InvoiceNo), 2) as AvgOrderValue,
    LAG(ROUND(SUM(TotalAmount), 2)) OVER (ORDER BY Year, Month) as PrevMonthRevenue
FROM transactions
GROUP BY Year, Month
ORDER BY Year, Month;

-- =====================================================
-- 8. SẢN PHẨM THƯỜNG ĐƯỢC MUA CÙNG NHAU
-- =====================================================

CREATE TABLE IF NOT EXISTS basket_analysis AS
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
LIMIT 100;

-- =====================================================
-- USEFUL QUERIES
-- =====================================================

-- Xem tổng quan dữ liệu
SELECT 
    COUNT(*) as TotalRecords,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    COUNT(DISTINCT InvoiceNo) as UniqueOrders,
    COUNT(DISTINCT StockCode) as UniqueProducts,
    ROUND(SUM(TotalAmount), 2) as TotalRevenue,
    MIN(InvoiceDate) as FirstDate,
    MAX(InvoiceDate) as LastDate
FROM transactions;

-- Top 10 khách hàng VIP
SELECT 
    CustomerID,
    Country,
    COUNT(DISTINCT InvoiceNo) as TotalOrders,
    ROUND(SUM(TotalAmount), 2) as TotalSpent,
    COUNT(DISTINCT StockCode) as UniqueProducts
FROM transactions
GROUP BY CustomerID, Country
ORDER BY TotalSpent DESC
LIMIT 10;

-- Sản phẩm có tỷ lệ mua lại cao nhất
SELECT 
    StockCode,
    FIRST(Description) as Description,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    COUNT(*) / COUNT(DISTINCT CustomerID) as AvgPurchasePerCustomer
FROM transactions
GROUP BY StockCode
HAVING COUNT(DISTINCT CustomerID) >= 100
ORDER BY AvgPurchasePerCustomer DESC
LIMIT 20;
