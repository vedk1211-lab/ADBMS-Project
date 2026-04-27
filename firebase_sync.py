import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# 1. Setup Connection
cred = credentials.Certificate("service-account.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def sync_project_data():
    print("🚀 Starting sync to Cloud Firestore...")

    # 2. Add a Sample Product (Matches your "Top 10 Products" screen)
    product_data = {
        "product_name": "Laptop",
        "category_id": "electronics",
        "cost_price": 45000,
        "selling_price": 55000,
        "stock_quantity": 150
    }
    # Using a specific ID "laptop_001" makes it easy to reference
    db.collection("products").document("laptop_001").set(product_data)

    # 3. Add a Sample Sale (Matches your "Sales Overview" cards)
    sale_data = {
        "product_id": "laptop_001",
        "product_name": "Laptop",
        "quantity": 1,
        "sale_amount": 55000,
        "profit": 10000,
        "is_festival": True,
        "sale_date": datetime.now()
    }
    db.collection("sales").add(sale_data)

    print("✅ Data successfully pushed to Firestore!")

if __name__ == "__main__":
    sync_project_data()