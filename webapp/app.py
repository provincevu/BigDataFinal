"""
Retail Analytics Web Application
================================
Ung dung web Flask de doc va hien thi du lieu
da xu ly tu Hadoop/Spark luu trong MongoDB
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
    """Ket noi MongoDB"""
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

def parse_json(data):
    """Convert MongoDB BSON to JSON"""
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
    """Trang danh sach khach hang va phan khuc"""
    db = get_db()
    
    # Phan trang
    page = request.args.get('page', 1, type=int)
    per_page = 20
    skip = (page - 1) * per_page
    
    # Loc theo segment
    segment_filter = request.args.get('segment', '')
    query = {}
    if segment_filter:
        query['Segment'] = segment_filter
    
    # Lay du lieu
    customers = list(db.customer_clusters.find(query).skip(skip).limit(per_page))
    total = db.customer_clusters.count_documents(query)
    
    # Lay danh sach cac segments
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
    """Chi tiet khach hang"""
    db = get_db()
    
    # Tim khach hang
    customer = db.customer_clusters.find_one({'CustomerID': int(customer_id)})
    if not customer:
        customer = db.customer_clusters.find_one({'CustomerID': customer_id})
    
    # Tim RFM data
    rfm = db.customer_rfm.find_one({'CustomerID': int(customer_id)})
    if not rfm:
        rfm = db.customer_rfm.find_one({'CustomerID': customer_id})
    
    return render_template('customer_detail.html',
                         customer=parse_json(customer),
                         rfm=parse_json(rfm) if rfm else None)


# ==================== SAN PHAM ====================
@app.route('/products')
def products():
    """Trang san pham ban chay"""
    db = get_db()
    
    # Lay top san pham theo doanh thu
    top_revenue = list(db.top_products_revenue.find().limit(50))
    
    # Lay top san pham theo so luong
    top_quantity = list(db.top_products_quantity.find().limit(50))
    
    return render_template('products.html',
                         top_revenue=parse_json(top_revenue),
                         top_quantity=parse_json(top_quantity))


# ==================== DOANH THU ====================
@app.route('/revenue')
def revenue():
    """Trang phan tich doanh thu"""
    db = get_db()
    
    # Doanh thu theo thang
    monthly = list(db.monthly_revenue.find().sort('_id', 1))
    
    # Doanh thu theo ngay (30 ngay gan nhat)
    daily = list(db.daily_revenue.find().sort('_id', -1).limit(30))
    daily.reverse()
    
    # Doanh thu theo gio
    hourly = list(db.hourly_revenue.find().sort('_id', 1))
    
    # Xu huong theo thang
    trend = list(db.monthly_trend.find().sort('_id', 1))
    
    return render_template('revenue.html',
                         monthly=parse_json(monthly),
                         daily=parse_json(daily),
                         hourly=parse_json(hourly),
                         trend=parse_json(trend))


# ==================== QUOC GIA ====================
@app.route('/countries')
def countries():
    """Trang hieu suat theo quoc gia"""
    db = get_db()
    
    # Lay du lieu theo quoc gia
    country_data = list(db.country_performance.find().sort('TotalRevenue', -1))
    
    return render_template('countries.html',
                         countries=parse_json(country_data))


# ==================== PHAN KHUC ====================
@app.route('/segments')
def segments():
    """Trang phan tich phan khuc khach hang"""
    db = get_db()
    
    # Thong ke phan khuc
    segment_stats = list(db.cluster_statistics.find())
    
    # Dem so luong theo segment
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
    """Trang goi y san pham"""
    db = get_db()
    
    # Lay product associations
    associations = list(db.product_associations.find().limit(100))
    
    # Lay product recommendations
    recs = list(db.product_recommendations.find().limit(100))
    
    return render_template('recommendations.html',
                         associations=parse_json(associations),
                         recommendations=parse_json(recs))


# ==================== API ENDPOINTS ====================
@app.route('/api/stats')
def api_stats():
    """API: Thong ke tong quan"""
    db = get_db()
    stats = {
        'total_customers': db.customer_clusters.count_documents({}),
        'total_transactions': db.transactions.count_documents({}) if 'transactions' in db.list_collection_names() else 0,
        'collections': db.list_collection_names()
    }
    return jsonify(stats)


@app.route('/api/customers')
def api_customers():
    """API: Danh sach khach hang"""
    db = get_db()
    limit = request.args.get('limit', 100, type=int)
    customers = list(db.customer_clusters.find().limit(limit))
    return jsonify(parse_json(customers))


@app.route('/api/segments')
def api_segments():
    """API: Thong ke phan khuc"""
    db = get_db()
    segments = list(db.cluster_statistics.find())
    return jsonify(parse_json(segments))


@app.route('/api/revenue/monthly')
def api_monthly_revenue():
    """API: Doanh thu theo thang"""
    db = get_db()
    data = list(db.monthly_revenue.find().sort('_id', 1))
    return jsonify(parse_json(data))


@app.route('/api/products/top')
def api_top_products():
    """API: Top san pham"""
    db = get_db()
    limit = request.args.get('limit', 20, type=int)
    data = list(db.top_products_revenue.find().limit(limit))
    return jsonify(parse_json(data))


@app.route('/api/countries')
def api_countries():
    """API: Du lieu theo quoc gia"""
    db = get_db()
    data = list(db.country_performance.find())
    return jsonify(parse_json(data))


# ==================== MAIN ====================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
