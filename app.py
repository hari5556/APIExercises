from flask import Flask, request, jsonify, render_template, abort
import mysql.connector.pooling
from config import DB_CONFIG, API_KEY
from functools import wraps
from datetime import datetime

app = Flask(__name__)

# Create connection pool
db_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)

def get_db_connection():
    return db_pool.get_connection()

def require_api_key(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        if request.path == '/':  # Skip auth for main page
            return view_function(*args, **kwargs)
            
        # Check for API key in headers or query params
        api_key = request.headers.get('X-API-KEY') or request.args.get('api_key')
        if api_key == API_KEY:
            return view_function(*args, **kwargs)
        else:
            abort(401, description="Invalid or missing API key")
    return decorated_function

@app.route('/api/products', methods=['POST'])
@require_api_key
def add_products():
    try:
        data = request.json
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT IGNORE INTO product_inventory 
            (BarcodeNo, SKU, Product, Supplier, Style, Shade, Size, 
             Cost, MRP, MOP, Dept, Fabric, Warehouse, WHLocation, Qty, HSNCODE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['BarcodeNo'], data['SKU'], data['Product'], 
                data['Supplier'], data['Style'], data['Shade'], data['Size'],
                data['Cost'], data['MRP'], data['MOP'], data['Dept'], 
                data['Fabric'], data['Warehouse'], data['WHLocation'],
                data['Qty'], data['HSNCODE']
            ))
        
        conn.commit()
        
        if cursor.rowcount == 0:
            return jsonify({"status": "skipped"}), 200
        else:
            return jsonify({
                "status": "success",
                "id": cursor.lastrowid,
                "data": data
            }), 201
            
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        cursor.close()
        conn.close()

@app.route('/api/products/batch', methods=['POST'])
@require_api_key
def add_products_batch():
    try:
        records = request.json.get('records', [])
        if not records:
            return jsonify({"error": "No records provided"}), 400
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = """INSERT IGNORE INTO product_inventory 
                (BarcodeNo, SKU, Product, Supplier, Style, Shade, Size, 
                 Cost, MRP, MOP, Dept, Fabric, Warehouse, WHLocation, Qty, HSNCODE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        values = [
            (
                r['BarcodeNo'], r['SKU'], r['Product'], r['Supplier'],
                r['Style'], r['Shade'], r['Size'], r['Cost'], r['MRP'],
                r['MOP'], r['Dept'], r['Fabric'], r['Warehouse'],
                r['WHLocation'], r['Qty'], r['HSNCODE']
            ) for r in records
        ]
        
        cursor.executemany(sql, values)
        conn.commit()
        
        return jsonify({
            "status": "success",
            "inserted": cursor.rowcount,
            "skipped": len(records) - cursor.rowcount
        }), 201
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        cursor.close()
        conn.close()

@app.route('/api/products', methods=['GET'])
@require_api_key
def get_products():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM product_inventory")
        products = cursor.fetchall()
        
        return jsonify({
            "status": "success",
            "count": len(products),
            "data": products
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        cursor.close()
        conn.close()

@app.route('/')
def serve_index():
    return render_template('index.html')

@app.errorhandler(401)
def unauthorized(error):
    return jsonify({
        "error": "Unauthorized",
        "message": str(error.description)
    }), 401

if __name__ == '__main__':
    # Changed to run on 0.0.0.0 to allow network access
    app.run(host='0.0.0.0', port=5000, debug=True)