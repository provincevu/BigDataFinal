// MongoDB Initialization Script
// Tạo database và các collections cho phân tích bán lẻ

// chuyển sang database retail_analytics
db = db.getSiblingDB('retail_analytics');

// Tạo các collections với xác thực schema
db.createCollection('transactions', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice', 'CustomerID'],
            properties: {
                InvoiceNo: { bsonType: 'string' },
                StockCode: { bsonType: 'string' },
                Description: { bsonType: 'string' },
                Quantity: { bsonType: 'int' },
                InvoiceDate: { bsonType: 'date' },
                UnitPrice: { bsonType: 'double' },
                CustomerID: { bsonType: 'int' },
                Country: { bsonType: 'string' },
                TotalAmount: { bsonType: 'double' }
            }
        }
    }
});

db.createCollection('monthly_revenue');
db.createCollection('daily_revenue');
db.createCollection('hourly_revenue');
db.createCollection('top_products_quantity');
db.createCollection('top_products_revenue');
db.createCollection('customer_rfm');
db.createCollection('customer_segments');
db.createCollection('customer_clusters');
db.createCollection('country_performance');
db.createCollection('monthly_trend');
db.createCollection('product_recommendations');
db.createCollection('product_associations');

// tạo indexes để hiệu suất truy vấn tốt hơn
db.transactions.createIndex({ 'CustomerID': 1 });
db.transactions.createIndex({ 'InvoiceNo': 1 });
db.transactions.createIndex({ 'StockCode': 1 });
db.transactions.createIndex({ 'InvoiceDate': 1 });
db.transactions.createIndex({ 'Country': 1 });

db.customer_rfm.createIndex({ 'CustomerID': 1 });
db.customer_rfm.createIndex({ 'CustomerSegment': 1 });
db.customer_rfm.createIndex({ 'RFM_Score': -1 });

db.customer_clusters.createIndex({ 'CustomerID': 1 });
db.customer_clusters.createIndex({ 'Cluster': 1 });

db.monthly_revenue.createIndex({ 'Year': 1, 'Month': 1 });
db.country_performance.createIndex({ 'TotalRevenue': -1 });

db.product_recommendations.createIndex({ 'CustomerID': 1 });
db.product_associations.createIndex({ 'CoOccurrence': -1 });

print('MongoDB đã được khởi tạo thành công!');
print('Các collections đã được tạo: transactions, monthly_revenue, daily_revenue, hourly_revenue, top_products_quantity, top_products_revenue, customer_rfm, customer_segments, customer_clusters, country_performance, monthly_trend, product_recommendations, product_associations');