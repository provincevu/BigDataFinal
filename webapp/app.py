"""
Retail Analytics Web Application
================================
Ứng dụng web Flask để đọc và hiển thị dữ liệu
đã xử lý từ Hadoop/Spark lưu trong MongoDB
"""

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient
from bson import json_util
import json
import os

app = Flask(__name__)

# MongoDB Configuration
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://admin:admin123@mongodb:27017/?authSource=admin')
DATABASE_NAME = 'retail_analytics'

def get_db():
    """Kết nối MongoDB"""
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

def parse_json(data):
    """Chuyển đổi MongoDB BSON sang JSON"""
    return json.loads(json_util.dumps(data))


# ==================== TRANG CHU ====================
@app.route('/')
def index():
    """Trang chu - Dashboard tong quan"""
    db = get_db()
    
    # Lay thong ke tong quan
    stats = {
        'total_customers': db.customer_clusters.count_documents({}),
        'total_transactions': db.transactions.count_documents({}) if 'transactions' in db.list_collection_names() else 0,
        'total_products': db.top_products_quantity.count_documents({}),
        'collections': db.list_collection_names()
    }
    
    # Lay top 5 san pham ban chay
    top_products = list(db.top_products_revenue.find().limit(5))
    
    # Lay phan bo phan khuc khach hang
    segments = list(db.cluster_statistics.find())
    
    return render_template('index.html', 
                         stats=stats, 
                         top_products=parse_json(top_products),
                         segments=parse_json(segments))


# ==================== KHACH HANG ====================
@app.route('/customers')
def customers():
    """Trang danh sách khách hàng và phân khúc"""
    db = get_db()
    
    # Phân trang
    page = request.args.get('page', 1, type=int)
    per_page = 20
    skip = (page - 1) * per_page
    
    # Lọc theo segment
    segment_filter = request.args.get('segment', '')
    query = {}
    if segment_filter:
        query['Segment'] = segment_filter
    
    # Lấy dữ liệu khách hàng
    customers = list(db.customer_clusters.find(query).skip(skip).limit(per_page))
    total = db.customer_clusters.count_documents(query)
    
    # Lấy danh sách các phân khúc
    segments = db.customer_clusters.distinct('Segment')
    
    return render_template('customers.html',
                         customers=parse_json(customers),
                         page=page,
                         per_page=per_page,
                         total=total,
                         segments=segments,
                         current_segment=segment_filter)


@app.route('/customer/<customer_id>')
def customer_detail(customer_id):
    """Chi tiết khách hàng"""
    db = get_db()
    
    # Tìm khách hàng
    customer = db.customer_clusters.find_one({'CustomerID': int(customer_id)})
    if not customer:
        customer = db.customer_clusters.find_one({'CustomerID': customer_id})
    
    # Tìm dữ liệu RFM
    rfm = db.customer_rfm.find_one({'CustomerID': int(customer_id)})
    if not rfm:
        rfm = db.customer_rfm.find_one({'CustomerID': customer_id})
    
    return render_template('customer_detail.html',
                         customer=parse_json(customer),
                         rfm=parse_json(rfm) if rfm else None)


# ==================== SAN PHAM ====================
@app.route('/products')
def products():
    """Trang sản phẩm bán chạy"""
    db = get_db()
    
    # Lấy top sản phẩm theo doanh thu
    top_revenue = list(db.top_products_revenue.find().limit(50))
    
    # Lấy top sản phẩm theo số lượng bán
    top_quantity = list(db.top_products_quantity.find().limit(50))
    
    return render_template('products.html',
                         top_revenue=parse_json(top_revenue),
                         top_quantity=parse_json(top_quantity))


# ==================== DOANH THU ====================
@app.route('/revenue')
def revenue():
    """Trang phân tích doanh thu"""
    db = get_db()
    
    # Doanh thu theo tháng
    monthly = list(db.monthly_revenue.find().sort('_id', 1))
    
    # Doanh thu theo ngày (30 ngày gần nhất)
    daily = list(db.daily_revenue.find().sort('_id', -1).limit(30))
    daily.reverse()
    
    # Doanh thu theo giờ
    hourly = list(db.hourly_revenue.find().sort('_id', 1))
    
    # Xu hướng theo tháng
    trend = list(db.monthly_trend.find().sort('_id', 1))
    
    return render_template('revenue.html',
                         monthly=parse_json(monthly),
                         daily=parse_json(daily),
                         hourly=parse_json(hourly),
                         trend=parse_json(trend))


# ==================== QUOC GIA ====================
@app.route('/countries')
def countries():
    """Trang hiệu suất theo quốc gia"""
    db = get_db()
    
    # Lấy dữ liệu theo quốc gia
    country_data = list(db.country_performance.find().sort('TotalRevenue', -1))
    
    return render_template('countries.html',
                         countries=parse_json(country_data))


# ==================== PHÂN KHÚC ====================
@app.route('/segments')
def segments():
    """Trang phân tích phân khúc khách hàng"""
    db = get_db()
    
    # Thống kê phân khúc
    segment_stats = list(db.cluster_statistics.find())
    
    # Đếm số lượng theo phân khúc
    segment_counts = []
    for seg in db.customer_clusters.aggregate([
        {'$group': {'_id': '$Segment', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]):
        segment_counts.append(seg)
    
    return render_template('segments.html',
                         segment_stats=parse_json(segment_stats),
                         segment_counts=parse_json(segment_counts))


# ==================== GOI Y SAN PHAM ====================
@app.route('/recommendations')
def recommendations():
    """Trang gợi ý sản phẩm"""
    db = get_db()
    
    # Lấy product associations
    associations = list(db.product_associations.find().limit(100))
    
    # Lấy gợi ý sản phẩm
    recs = list(db.product_recommendations.find().limit(100))
    
    return render_template('recommendations.html',
                         associations=parse_json(associations),
                         recommendations=parse_json(recs))


# ==================== API ENDPOINTS ====================
@app.route('/api/stats')
def api_stats():
    """API: Thống kê tổng quan"""
    db = get_db()
    stats = {
        'total_customers': db.customer_clusters.count_documents({}),
        'total_transactions': db.transactions.count_documents({}) if 'transactions' in db.list_collection_names() else 0,
        'collections': db.list_collection_names()
    }
    return jsonify(stats)


@app.route('/api/customers')
def api_customers():
    """API: Danh sách khách hàng và phân khúc"""
    db = get_db()
    limit = request.args.get('limit', 100, type=int)
    customers = list(db.customer_clusters.find().limit(limit))
    return jsonify(parse_json(customers))


@app.route('/api/segments')
def api_segments():
    """API: Thống kê phân khúc khách hàng"""
    db = get_db()
    segments = list(db.cluster_statistics.find())
    return jsonify(parse_json(segments))


@app.route('/api/revenue/monthly')
def api_monthly_revenue():
    """API: Doanh thu theo tháng"""
    db = get_db()
    data = list(db.monthly_revenue.find().sort('_id', 1))
    return jsonify(parse_json(data))


@app.route('/api/products/top')
def api_top_products():
    """API: Top sản phẩm theo doanh thu"""
    db = get_db()
    limit = request.args.get('limit', 20, type=int)
    data = list(db.top_products_revenue.find().limit(limit))
    return jsonify(parse_json(data))


@app.route('/api/countries')
def api_countries():
    """API: Dữ liệu theo quốc gia"""
    db = get_db()
    data = list(db.country_performance.find())
    return jsonify(parse_json(data))


# ==================== MAIN ====================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
