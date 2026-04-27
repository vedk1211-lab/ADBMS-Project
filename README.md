# 🛒 Smart Retail Analytics & Prediction

A full-stack retail analytics system showcasing **Advanced Database Management Systems (ADBMS)** concepts, **Machine Learning models**, and an **interactive dashboard** built using PostgreSQL, FastAPI, and modern web technologies.

---

## 🚀 Key Features

* 📊 Interactive retail analytics dashboard
* 🧠 Machine Learning-based sales & profit prediction
* 🗄️ Implementation of core ADBMS concepts
* ⚡ FastAPI backend with REST APIs
* 🌐 Real-time data visualization

---

## 🏗️ Tech Stack

* **Backend:** FastAPI (Python)
* **Database:** PostgreSQL
* **ML Models:** scikit-learn (Random Forest, Linear Regression)
* **Frontend:** HTML, CSS, JavaScript
* **Tools:** Pandas, NumPy, Uvicorn

---

## 📁 Project Structure

```
ADBMS Project/
├── backend/
│   ├── database.py
│   ├── generate_data.py
│   ├── train_model.py
│   ├── server.py
│   ├── requirements.txt
│   └── models/
├── frontend/
│   └── index.html
├── setup.bat
└── start_server.bat
```

---

## ⚙️ Setup Instructions

```bash
pip install -r backend/requirements.txt
```

```bash
psql -U postgres -c "CREATE DATABASE retail_shop;"
```

```bash
cd backend
python database.py
python generate_data.py
python train_model.py
```

```bash
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

---

## 🌐 Access the Application

* Dashboard → `frontend/index.html`
* Backend → http://localhost:8001
* API Docs → http://localhost:8001/docs

---

# 🗄️ ADBMS Concepts Implemented

## 1. Fragmentation

### 🔹 Horizontal Fragmentation

* Sales data split into:

  * `sales_2023`
  * `sales_2024`
* Improves query performance by reducing scan size

### 🔹 Vertical Fragmentation

* Product data divided into:

  * `products_basic` (identity)
  * `products_pricing` (price + validity)

### 🔹 Reconstruction

* `sales_unified` → UNION of fragmented tables
* `products_reconstructed` → JOIN of vertical fragments

---

## 2. Replication (Master–Replica)

* `products_master` → Handles write operations
* `products_replica` → Handles read queries
* Synchronization using stored procedure:

  * `replicate_products()`

---

## 3. Temporal Database

* **Valid Time:**

  * `valid_from`, `valid_to` (price validity)

* **Transaction Time:**

  * `created_at`, `updated_at` (audit tracking)

---

## 4. Data Warehouse (Star Schema)

```
dim_time
dim_product
dim_category
     ↓
  fact_sales
```

* Enables efficient analytical queries and reporting

---

## 5. OLAP Operations

| Operation  | Endpoint                           |
| ---------- | ---------------------------------- |
| Roll-up    | `/api/adbms/olap/rollup`           |
| Drill-down | `/api/adbms/olap/drilldown/{year}` |
| Slice      | `/api/adbms/olap/slice`            |
| Dice       | `/api/adbms/olap/dice`             |

---

## 6. Query Optimization

* B-tree indexes on:

  * `sale_date`
  * `product_id`
  * `category_id`
* Performance comparison using:

  * `EXPLAIN ANALYZE`
* Endpoint:

  * `/api/adbms/query-optimization`

---

## 7. Stored Procedures & Functions

| Function                      | Purpose                   |
| ----------------------------- | ------------------------- |
| `calculate_profit()`          | Profit calculation        |
| `get_sales_summary()`         | KPI aggregation           |
| `get_product_price_at_date()` | Temporal price lookup     |
| `replicate_products()`        | Data replication          |
| `insert_sale_transaction()`   | ACID transaction handling |

---

# 🤖 Machine Learning Models

| Model             | Target         |
| ----------------- | -------------- |
| Linear Regression | Sales & Profit |
| Random Forest     | Sales & Profit |

### Features Used:

* Category
* Month
* Quantity
* Discount %
* Festival flag
* Historical averages
* Profit margin

---

## 📊 Analytics Features

* Sales trends (monthly/yearly)
* Category-wise analysis
* Top-performing products
* Festival vs regular sales
* Drill-down insights

---

## 🔌 API Endpoints

| Endpoint                       | Description         |
| ------------------------------ | ------------------- |
| `/api/analytics/overview`      | KPIs                |
| `/api/analytics/monthly-sales` | Trends              |
| `/api/analytics/top-products`  | Best sellers        |
| `/api/predict/sales`           | ML prediction       |
| `/api/adbms/fragmentation`     | Fragmentation stats |
| `/api/adbms/replication`       | Replication info    |
| `/api/adbms/star-schema`       | Data warehouse      |

---

## ⚠️ Troubleshooting

* Ensure PostgreSQL is running
* Check `.env` for correct DB credentials
* Train models before starting server
* Verify backend runs on port 8001

---

## 📌 Future Enhancements

* Cloud deployment (Render / AWS)
* PostgreSQL cloud integration
* User authentication system
* Real-time analytics

---

## 👨‍💻 Author

**Vedant Kulkarni**
