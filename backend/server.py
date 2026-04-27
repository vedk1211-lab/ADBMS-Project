"""
Smart Retail Sales Analysis & Prediction
SQLite Port.
"""
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configuration
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

DB_PATH = ROOT_DIR / "retail_shop.db"

# ML Models
MODEL_DIR = ROOT_DIR / "models"
try:
    rf_sale    = joblib.load(MODEL_DIR / "rf_sale.pkl")
    rf_profit  = joblib.load(MODEL_DIR / "rf_profit.pkl")
    le         = joblib.load(MODEL_DIR / "label_encoder.pkl")
    with open(MODEL_DIR / "metadata.json") as f:
        META = json.load(f)
    print(" ML models loaded")
except Exception as exc:
    print(f"Warning: ML models unavailable: {exc}")
    rf_sale = rf_profit = le = None
    META = {}

# FastAPI App
app = FastAPI(title="Smart Retail Analytics API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# DB Helper
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def rows(cur) -> list[dict]:
    return [dict(r) for r in cur.fetchall()]


# Pydantic Schemas
class PredictRequest(BaseModel):
    category: str
    month: int
    quantity: int
    discount_percent: float
    store_in_db: bool = False


class PredictResponse(BaseModel):
    predicted_sale_amount: float
    predicted_profit: float
    profit_margin_percent: float
    input_details: dict[str, Any]


# ROOT
@app.get("/api/")
def root():
    return {"message": "Smart Retail Analytics API", "status": "running"}


# ANALYTICS ENDPOINTS
@app.get("/api/analytics/overview")
def overview():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*)          AS total_orders,
                   SUM(sale_amount)  AS total_revenue,
                   SUM(profit)       AS total_profit,
                   AVG(sale_amount)  AS avg_order_value,
                   SUM(quantity)     AS total_items_sold
            FROM sales
            """
        )
        totals = dict(cur.fetchone())

        cur.execute("SELECT * FROM category_performance ORDER BY total_sales DESC")
        categories = rows(cur)

        cur.execute(
            """
            SELECT year, month, total_sales, total_profit
            FROM monthly_sales_summary
            ORDER BY year DESC, month DESC
            LIMIT 24
            """
        )
        monthly = rows(cur)

        cur.execute(
            """
            SELECT is_festival_month,
                   COUNT(*)         AS orders,
                   SUM(sale_amount) AS sales,
                   SUM(profit)      AS profit
            FROM sales GROUP BY is_festival_month
            """
        )
        festival = rows(cur)

        return dict(totals=totals, category_performance=categories,
                    monthly_trend=monthly, festival_comparison=festival)
    finally:
        cur.close()
        conn.close()


@app.get("/api/analytics/monthly-sales")
def monthly_sales():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM monthly_sales_summary ORDER BY year, month")
        return {"monthly_sales": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/analytics/category-performance")
def category_performance():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM category_performance")
        return {"categories": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/analytics/top-products")
def top_products(limit: int = 10):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM top_products LIMIT {int(limit)}")
        return {"top_products": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/analytics/festival-comparison")
def festival_comparison():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT festival_name,
                   COUNT(*)            AS total_orders,
                   SUM(sale_amount)    AS total_sales,
                   SUM(profit)         AS total_profit,
                   AVG(sale_amount)    AS avg_order_value,
                   AVG(discount_percent) AS avg_discount
            FROM sales WHERE is_festival_month = 1
            GROUP BY festival_name ORDER BY total_sales DESC
            """
        )
        fest = rows(cur)
        cur.execute(
            """
            SELECT COUNT(*) AS total_orders, SUM(sale_amount) AS total_sales,
                   SUM(profit) AS total_profit, AVG(sale_amount) AS avg_order_value,
                   AVG(discount_percent) AS avg_discount
            FROM sales WHERE is_festival_month = 0
            """
        )
        non_fest = dict(cur.fetchone())
        return {"festival_sales": fest, "non_festival_sales": non_fest}
    finally:
        cur.close(); conn.close()


@app.get("/api/analytics/category-monthly/{category}")
def category_monthly(category: str):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT s.year, s.month,
                   COUNT(*)               AS total_orders,
                   SUM(s.sale_amount)     AS total_sales,
                   SUM(s.profit)          AS total_profit,
                   AVG(s.discount_percent) AS avg_discount
            FROM sales s
            JOIN categories c ON s.category_id = c.category_id
            WHERE c.category_name = ?
            GROUP BY s.year, s.month ORDER BY s.year, s.month
            """,
            (category,),
        )
        return {"category": category, "monthly_data": rows(cur)}
    finally:
        cur.close(); conn.close()


# ML PREDICTION ENDPOINTS
@app.get("/api/predict/categories")
def pred_categories():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT category_name FROM categories ORDER BY category_name")
        return {"categories": [r["category_name"] for r in cur.fetchall()]}
    finally:
        cur.close(); conn.close()


@app.post("/api/predict/sales", response_model=PredictResponse)
def predict_sales(req: PredictRequest):
    if rf_sale is None:
        raise HTTPException(500, "ML models not loaded")

    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT category_id FROM categories WHERE category_name = ?", (req.category,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Category not found")
        cid = row["category_id"]

        cur.execute(
            "SELECT AVG(cost_price) AS c, AVG(selling_price) AS s FROM products WHERE category_id = ?",
            (cid,),
        )
        pr = cur.fetchone()
        avg_cost = float(pr["c"])
        avg_sell = float(pr["s"])

        cat_df = pd.DataFrame(META["category_avg"])
        cat_avg_sale = float(
            cat_df.loc[cat_df["category_name"] == req.category, "category_avg_sale"].iloc[0]
        )
        m_df = pd.DataFrame(META["month_avg"])
        mon_avg_sale = float(
            m_df.loc[m_df["month"] == req.month, "month_avg_sale"].iloc[0]
        )

        cur.execute("SELECT is_festival FROM dim_time WHERE month = ? LIMIT 1", (req.month,))
        frow = cur.fetchone()
        is_fest = int(frow["is_festival"]) if frow else 0

        cat_enc    = le.transform([req.category])[0]
        pm         = (avg_sell - avg_cost) / avg_sell
        base_val   = req.quantity * avg_sell

        X = np.array([[cat_enc, req.month, req.quantity, req.discount_percent,
                       is_fest, avg_cost, avg_sell, cat_avg_sale, mon_avg_sale,
                       pm, base_val]])

        pred_sale   = float(rf_sale.predict(X)[0])
        pred_profit = float(rf_profit.predict(X)[0])
        margin      = (pred_profit / pred_sale * 100) if pred_sale > 0 else 0

        transaction_msg = None
        if req.store_in_db:
            try:
                cur.execute("BEGIN")
                cur.execute(
                    """
                    INSERT INTO prediction_history
                    (category_name, month, quantity, discount_percent, predicted_sale, predicted_profit)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (req.category, req.month, req.quantity, req.discount_percent, pred_sale, pred_profit)
                )
                cur.execute("COMMIT")
                conn.commit()
                transaction_msg = "Prediction stored successfully (transaction committed)"
            except Exception as e:
                cur.execute("ROLLBACK")
                conn.commit()
                transaction_msg = f"Transaction failed: {str(e)}"

        return PredictResponse(
            predicted_sale_amount=round(pred_sale, 2),
            predicted_profit=round(pred_profit, 2),
            profit_margin_percent=round(margin, 2),
            input_details={
                "category": req.category,
                "month": req.month,
                "quantity": req.quantity,
                "discount_percent": req.discount_percent,
                "is_festival_month": bool(is_fest),
                "transaction_status": transaction_msg,
            },
        )
    finally:
        cur.close(); conn.close()


# ADBMS DEMONSTRATION ENDPOINTS
@app.get("/api/adbms/fragmentation")
def fragmentation():
    conn = db(); cur = conn.cursor()
    try:
        def count(tbl):
            cur.execute(f"SELECT COUNT(*) AS n FROM {tbl}")
            return cur.fetchone()["n"]

        return {
            "horizontal_fragmentation": {
                "concept":    "Sales table partitioned by year into child tables",
                "sales_2023": count("sales_2023"),
                "sales_2024": count("sales_2024"),
                "unified_view": count("sales_unified"),
                "benefit":    "Faster year-specific queries; older data can be archived independently",
                "sql_example": "SELECT * FROM sales_2023 WHERE month = 11",
            },
            "vertical_fragmentation": {
                "concept":      "Products table split by column groups (identity vs. pricing)",
                "products_basic":   count("products_basic"),
                "products_pricing": count("products_pricing"),
                "reconstructed":    count("products_reconstructed"),
                "benefit":     "Rarely-needed pricing columns kept separate, improving cache efficiency",
                "sql_example": "SELECT * FROM products_reconstructed",
            },
        }
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/replication")
def replication():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) AS n FROM products_master")
        mc = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM products_replica")
        rc = cur.fetchone()["n"]
        cur.execute("SELECT MAX(replicated_at) AS ts FROM products_replica")
        ts = cur.fetchone()["ts"]
        return {
            "concept":         "Master-Replica Replication",
            "description":     "Writes go to products_master; reads served from products_replica",
            "master_records":  mc,
            "replica_records": rc,
            "last_replicated": str(ts) if ts else None,
            "benefits": [
                "Read-scalability (replicas absorb read load)",
                "High availability",
                "Geographic distribution",
            ],
            "procedure": "Python replication script replaces CALL replicate_products();",
        }
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/temporal-database")
def temporal_db():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT product_name, cost_price, selling_price, valid_from, valid_to FROM products LIMIT 5"
        )
        valid_time = rows(cur)
        cur.execute("SELECT product_name, created_at, updated_at FROM products LIMIT 5")
        txn_time = rows(cur)
        return {
            "valid_time": {
                "description": "Tracks when data is valid in the real world (price validity window)",
                "columns":     ["valid_from", "valid_to"],
                "samples":     valid_time,
                "query":       "SELECT * FROM products WHERE '2024-06-01' BETWEEN valid_from AND valid_to",
            },
            "transaction_time": {
                "description": "Tracks when data was inserted / last modified in the database",
                "columns":     ["created_at", "updated_at"],
                "samples":     txn_time,
                "use_case":    "Full audit trail; see what the database contained at any past moment",
            },
        }
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/query-optimization")
def query_optimization():
    # SQLite EXPLAIN doesn't execute the query, so we manually time it in Python 
    # to demonstrate the B-Tree index differences.
    conn = db(); cur = conn.cursor()
    try:
        START = time.perf_counter()
        cur.execute("SELECT * FROM sales WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31'")
        cur.fetchall()
        with_idx_time = f"Execution Time: {(time.perf_counter() - START)*1000:.3f} ms (B-Tree Index Scan)"

        # Forcing a full table scan or similar slow search on unindexed column if possible, 
        # but just demonstrating the timing approach for SQLite.
        START2 = time.perf_counter()
        cur.execute("SELECT * FROM sales WHERE created_at > '2023-01-01'")
        cur.fetchall()
        no_idx_time = f"Execution Time: {(time.perf_counter() - START2)*1000:.3f} ms (Full Table Scan)"

        return {
            "concept":      "Index-based Query Optimisation",
            "description":  "B-tree indexes created on high-cardinality, frequently filtered columns",
            "indexes": [
                "idx_sales_date       ON sales(sale_date)",
                "idx_sales_month_year ON sales(month, year)",
                "idx_sales_category   ON sales(category_id)",
                "idx_sales_product    ON sales(product_id)",
                "idx_fact_date        ON fact_sales(date_key)",
            ],
            "indexed_query_plan":     with_idx_time,
            "non_indexed_query_plan": no_idx_time,
        }
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/olap/rollup")
def olap_rollup():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT year, SUM(sale_amount) AS yearly_sales, SUM(profit) AS yearly_profit "
            "FROM sales GROUP BY year ORDER BY year"
        )
        return {"operation": "Roll-Up", "description": "Aggregate monthly -> yearly",
                "data": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/olap/drilldown/{year}")
def olap_drilldown(year: int):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT month, SUM(sale_amount) AS monthly_sales, SUM(profit) AS monthly_profit, "
            "COUNT(*) AS orders FROM sales WHERE year = ? GROUP BY month ORDER BY month",
            (year,),
        )
        return {"operation": "Drill-Down", "year": year,
                "description": f"Expand year {year} -> monthly breakdown",
                "data": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/olap/slice")
def olap_slice(year: Optional[int] = 2024):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT c.category_name, COUNT(*) AS orders, "
            "SUM(s.sale_amount) AS total_sales, SUM(s.profit) AS total_profit "
            "FROM sales s JOIN categories c ON s.category_id = c.category_id "
            "WHERE s.year = ? GROUP BY c.category_name ORDER BY total_sales DESC",
            (year,),
        )
        return {"operation": "Slice", "filter": {"year": year},
                "description": f"Fix year dimension = {year}",
                "data": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/olap/dice")
def olap_dice(year: int = 2024, category: str = "Electronics"):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT s.month, COUNT(*) AS orders, "
            "SUM(s.sale_amount) AS total_sales, SUM(s.profit) AS total_profit, "
            "AVG(s.discount_percent) AS avg_discount "
            "FROM sales s JOIN categories c ON s.category_id = c.category_id "
            "WHERE s.year = ? AND c.category_name = ? "
            "GROUP BY s.month ORDER BY s.month",
            (year, category),
        )
        return {"operation": "Dice", "filters": {"year": year, "category": category},
                "description": "Fix two dimensions: year AND category",
                "data": rows(cur)}
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/star-schema")
def star_schema():
    conn = db(); cur = conn.cursor()
    try:
        def count(t):
            cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
            return cur.fetchone()["n"]
        return {
            "concept":     "Star Schema (Data Warehouse)",
            "description": "Central fact_sales table surrounded by dimension tables",
            "fact_table":  {"name": "fact_sales",    "rows": count("fact_sales")},
            "dimensions":  [
                {"name": "dim_time",     "rows": count("dim_time"),     "role": "Date/festival info"},
                {"name": "dim_product",  "rows": count("dim_product"),  "role": "Product details"},
                {"name": "dim_category", "rows": count("dim_category"), "role": "Category grouping"},
            ],
            "olap_operations": ["Roll-up", "Drill-down", "Slice", "Dice", "Pivot"],
        }
    finally:
        cur.close(); conn.close()


@app.get("/api/adbms/views")
def views():
    return {
        "views": [
            {"name": "monthly_sales_summary",  "description": "Aggregated monthly KPIs"},
            {"name": "category_performance",   "description": "Revenue & profit by category"},
            {"name": "top_products",           "description": "Best-selling products by revenue"},
            {"name": "festival_comparison",    "description": "Festival vs normal sales"},
            {"name": "sales_unified",          "description": "UNION of horizontal fragments"},
            {"name": "products_reconstructed", "description": "Reconstructed from vertical fragments"},
        ],
        "stored_procedures": [
            {"name": "replicate_products (Python)",  "description": "Syncs master to replica"},
            {"name": "insert_sale_transaction", "description": "ACID-safe sale insertion"},
        ],
        "functions": [
            {"name": "Python calculate_profit", "description": "Profit formula"},
            {"name": "Python get_sales_summary", "description": "Date-range KPIs"},
            {"name": "Python get_product_price", "description": "Valid-time price lookup"},
        ],
    }


# Dev server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True)