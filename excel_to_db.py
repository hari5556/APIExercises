import openpyxl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from config import API_KEY

# Configuration
EXCEL_FILE = 'itemmaster.xlsx'
API_URL = "http://localhost:5000/api/products/batch"  # Using batch endpoint
BATCH_SIZE = 50  # Optimal for most cases
HEADERS = {
    'Content-Type': 'application/json',
    'X-API-KEY': API_KEY
}

def excel_to_json(row):
    """Convert Excel row to JSON format with NULL handling"""
    return {
        "BarcodeNo": str(row[0]) if row[0] else None,
        "SKU": str(row[1]) if row[1] else None,
        "Product": str(row[2]) if row[2] else None,
        "Supplier": str(row[3]) if row[3] else None,
        "Style": str(row[4]) if row[4] else None,
        "Shade": str(row[5]) if row[5] else None,
        "Size": str(row[6]) if row[6] else None,
        "Cost": float(row[7]) if row[7] else 0.0,
        "MRP": float(row[8]) if row[8] else 0.0,
        "MOP": float(row[9]) if row[9] else None,
        "Dept": str(row[10]) if row[10] else None,
        "Fabric": str(row[11]) if row[11] else None,
        "Warehouse": str(row[12]) if row[12] else None,
        "WHLocation": str(row[13]) if row[13] else None,
        "Qty": int(row[14]) if row[14] else 0,
        "HSNCODE": str(row[15]) if row[15] else None
    }

def process_excel():
    try:
        # Configure retry strategy for HTTP requests
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        
        # Load workbook in read-only mode for better performance
        wb = openpyxl.load_workbook(EXCEL_FILE, read_only=True)
        sheet = wb.active
        
        batch = []
        processed = 0
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            batch.append(excel_to_json(row))
            if len(batch) >= BATCH_SIZE:
                response = session.post(
                    API_URL,
                    json={"records": batch},
                    headers=HEADERS,
                    timeout=10
                )
                
                if response.status_code == 201:
                    result = response.json()
                    processed += result['inserted']
                    print(f"✓ Inserted batch: {result['inserted']}, Skipped: {result['skipped']}")
                else:
                    print(f"✗ Batch error: {response.text}")
                
                batch = []  # Reset batch
        
        # Process remaining records
        if batch:
            response = session.post(API_URL, json={"records": batch}, headers=HEADERS)
            if response.status_code == 201:
                result = response.json()
                processed += result['inserted']
                print(f"✓ Final batch: {result['inserted']}, Skipped: {result['skipped']}")
        
        print(f"\nProcess completed! Total processed: {processed}")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
    finally:
        if 'wb' in locals():
            wb.close()

if __name__ == "__main__":
    print("Starting optimized Excel to Database processing...")
    process_excel()