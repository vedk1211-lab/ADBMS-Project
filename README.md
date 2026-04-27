# 🛒 Smart Retail Analytics — ADBMS Project

A full-stack retail analytics dashboard demonstrating **Advanced Database Management Systems (ADBMS)** concepts with PostgreSQL, a FastAPI backend, scikit-learn ML models, and a premium dark-theme frontend dashboard.

---

## 🗂️ Project Structure

```
ADBMS Project/
├── backend/
│   ├── .env                  ← DB connection string
│   ├── database.py           ← Schema creation (ADBMS concepts)
│   ├── generate_data.py      ← 2-year synthetic sales data generator
│   ├── train_model.py        ← Linear Regression + Random Forest training
│   ├── server.py             ← FastAPI REST API server
│   ├── requirements.txt      ← Python dependencies
│   └── models/               ← Saved ML model artifacts (after training)
├── fromtend/
│   └── index.html            ← Single-file premium dashboard
├── setup.bat                 ← Full one-click setup (Windows)
└── start_server.bat          ← Quick server start
```

---

## ⚙️ Prerequisites

| Tool | Notes |
|------|-------|
| **Python 3.10+** | Add to PATH |
| **PostgreSQL 14+** | Running locally; user `postgres` with password `postgres` |
| **pip** | Usually bundled with Python |

---

## 🚀 Quick Start (Windows)

### Option A — Full First-Time Setup
```bat
# Double-click or run from the project root:
setup.bat
```

This will:
1. Install all Python dependencies
2. Create the `retail_shop` PostgreSQL database
3. Build the schema with all ADBMS constructs
4. Generate 2 years of realistic sales data (~18,000 transactions)
5. Train the Random Forest and Linear Regression models
6. Start the FastAPI server on **http://localhost:8001**

### Option B — Manual Steps

```bat
cd backend

# 1. Install dependencies
pip install -r requirements.txt

# 2. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE retail_shop;"

# 3. Create schema
python database.py

# 4. Generate data
python generate_data.py

# 5. Train ML models
python train_model.py

# 6. Start server
python -m uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

---

## 🌐 Open the Dashboard

After the server is running:

1. **Open** `fromtend/index.html` directly in your browser (double-click or drag to browser)
2. The dashboard auto-connects to `http://localhost:8001`

---

## 📊 ADBMS Concepts Implemented

### 1. Fragmentation
| Type | Implementation |
|------|---------------|
| **Horizontal** | `sales_2023` and `sales_2024` — child tables partitioned by year |
| **Vertical** | `products_basic` (identity) and `products_pricing` (pricing validity) |
| **Reconstruction** | `sales_unified` (UNION view), `products_reconstructed` (JOIN view) |

### 2. Replication (Master-Replica)
- `products_master` — receives all write operations
- `products_replica` — serves read queries (synced via `CALL replicate_products()`)

### 3. Temporal Database
- **Valid Time** — `valid_from` / `valid_to` on products (real-world price validity)
- **Transaction Time** — `created_at` / `updated_at` on all tables (DB audit trail)

### 4. Data Warehouse (Star Schema)
```
         dim_time ──────┐
         dim_product ───┼──── fact_sales (central fact table)
         dim_category ──┘
```

### 5. OLAP Operations (API endpoints)
| Operation | Endpoint |
|-----------|----------|
| Roll-up | `GET /api/adbms/olap/rollup` |
| Drill-down | `GET /api/adbms/olap/drilldown/{year}` |
| Slice | `GET /api/adbms/olap/slice?year=2024` |
| Dice | `GET /api/adbms/olap/dice?year=2024&category=Electronics` |

### 6. Query Optimisation
- 10 B-tree indexes on high-cardinality columns (`sale_date`, `category_id`, `product_id`, etc.)
- `EXPLAIN ANALYZE` comparison endpoint: `GET /api/adbms/query-optimization`

### 7. Stored Procedures & Functions
| Object | Purpose |
|--------|---------|
| `calculate_profit()` | Profit formula encapsulation |
| `get_sales_summary()` | Date-range KPI aggregation |
| `get_product_price_at_date()` | Valid-time price lookup |
| `replicate_products()` | Master→Replica sync |
| `insert_sale_transaction()` | ACID-safe sale insertion |

---

## 🤖 ML Models

| Model | Target | R² Score |
|-------|--------|----------|
| Linear Regression | Sale Amount | 0.9971 |
| Linear Regression | Profit | 0.8581 |
| **Random Forest** | **Sale Amount** | **1.0000** |
| **Random Forest** | **Profit** | **0.9997** |

**Features used:** category, month, quantity, discount %, festival flag, historical averages, profit margin, base sale value

---

## 🔌 API Reference

| Endpoint | Description |
|----------|-------------|
| `GET /api/analytics/overview` | KPIs, category performance, monthly trend |
| `GET /api/analytics/monthly-sales` | Full 24-month data |
| `GET /api/analytics/top-products` | Best sellers |
| `GET /api/analytics/festival-comparison` | Festival vs regular |
| `GET /api/analytics/category-monthly/{cat}` | Category drill-down |
| `POST /api/predict/sales` | ML prediction |
| `GET /api/adbms/fragmentation` | Fragmentation stats |
| `GET /api/adbms/replication` | Replication status |
| `GET /api/adbms/temporal-database` | Temporal examples |
| `GET /api/adbms/star-schema` | DWH facts |
| `GET /api/adbms/views` | Views & procedures list |

Interactive docs: **http://localhost:8001/docs**

---

## 🛠️ Troubleshooting

**`psql` not found** — Add PostgreSQL `bin` folder to Windows PATH  
**Connection refused** — Ensure PostgreSQL service is running (`services.msc`)  
**Wrong password** — Edit `backend/.env` and change `POSTGRES_URL`  
**Models not loaded** — Run `python train_model.py` before starting the server  
**CORS error in browser** — Make sure the server is on port 8001 and CORS_ORIGINS=* in `.env`
