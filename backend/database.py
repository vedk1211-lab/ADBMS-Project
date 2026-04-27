"""
Database connection and setup for Smart Retail Analytics
SQLite port.
"""
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

DB_PATH = ROOT_DIR / "retail_shop.db"
# Use sqlite string
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

SCHEMA_SQL = """
-- ============================================================
-- 1. CORE TABLES WITH TEMPORAL CONCEPTS
-- ============================================================
CREATE TABLE IF NOT EXISTS categories (
    category_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    description   TEXT,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name   VARCHAR(200) NOT NULL,
    category_id    INTEGER REFERENCES categories(category_id),
    cost_price     DECIMAL(10,2) NOT NULL,
    selling_price  DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    valid_from     DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to       DATE DEFAULT '9999-12-31',
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id       INTEGER REFERENCES products(product_id),
    category_id      INTEGER REFERENCES categories(category_id),
    quantity         INTEGER NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    sale_amount      DECIMAL(12,2) NOT NULL,
    profit           DECIMAL(12,2) NOT NULL,
    sale_date        DATE NOT NULL,
    month            INTEGER NOT NULL,
    year             INTEGER NOT NULL,
    is_festival_month BOOLEAN DEFAULT FALSE,
    festival_name    VARCHAR(50),
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prediction_history (
    prediction_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name    VARCHAR(100) NOT NULL,
    month            INTEGER NOT NULL,
    quantity         INTEGER NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    predicted_sale   DECIMAL(12,2) NOT NULL,
    predicted_profit DECIMAL(12,2) NOT NULL,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. HORIZONTAL FRAGMENTATION (partition by year)
-- ============================================================
CREATE TABLE IF NOT EXISTS sales_2023 (
    sale_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id       INTEGER REFERENCES products(product_id),
    category_id      INTEGER REFERENCES categories(category_id),
    quantity         INTEGER NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    sale_amount      DECIMAL(12,2) NOT NULL,
    profit           DECIMAL(12,2) NOT NULL,
    sale_date        DATE NOT NULL,
    month            INTEGER NOT NULL,
    year             INTEGER NOT NULL CHECK (year = 2023),
    is_festival_month BOOLEAN DEFAULT FALSE,
    festival_name    VARCHAR(50),
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales_2024 (
    sale_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id       INTEGER REFERENCES products(product_id),
    category_id      INTEGER REFERENCES categories(category_id),
    quantity         INTEGER NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    sale_amount      DECIMAL(12,2) NOT NULL,
    profit           DECIMAL(12,2) NOT NULL,
    sale_date        DATE NOT NULL,
    month            INTEGER NOT NULL,
    year             INTEGER NOT NULL CHECK (year = 2024),
    is_festival_month BOOLEAN DEFAULT FALSE,
    festival_name    VARCHAR(50),
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 3. VERTICAL FRAGMENTATION (split columns across tables)
-- ============================================================
CREATE TABLE IF NOT EXISTS products_basic (
    product_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name   VARCHAR(200) NOT NULL,
    category_id    INTEGER,
    stock_quantity INTEGER DEFAULT 0,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products_pricing (
    product_id    INTEGER PRIMARY KEY,
    cost_price    DECIMAL(10,2) NOT NULL,
    selling_price DECIMAL(10,2) NOT NULL,
    valid_from    DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to      DATE DEFAULT '9999-12-31',
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 4. REPLICATION (Master  Replica simulation)
-- ============================================================
CREATE TABLE IF NOT EXISTS products_master (
    product_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name   VARCHAR(200) NOT NULL,
    category_id    INTEGER,
    cost_price     DECIMAL(10,2) NOT NULL,
    selling_price  DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products_replica (
    product_id     INTEGER PRIMARY KEY,
    product_name   VARCHAR(200) NOT NULL,
    category_id    INTEGER,
    cost_price     DECIMAL(10,2) NOT NULL,
    selling_price  DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    replicated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5. DATA WAREHOUSE  STAR SCHEMA
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_time (
    date_key     INTEGER PRIMARY KEY,
    full_date    DATE NOT NULL,
    day          INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   VARCHAR(20),
    quarter      INTEGER NOT NULL,
    year         INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,
    day_name     VARCHAR(20),
    is_weekend   BOOLEAN DEFAULT 0,
    is_festival  BOOLEAN DEFAULT 0,
    festival_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id     INTEGER,
    product_name   VARCHAR(200),
    category_name  VARCHAR(100),
    cost_price     DECIMAL(10,2),
    selling_price  DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id   INTEGER,
    category_name VARCHAR(100),
    description   TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key         INTEGER REFERENCES dim_time(date_key),
    product_key      INTEGER REFERENCES dim_product(product_key),
    category_key     INTEGER REFERENCES dim_category(category_key),
    quantity         INTEGER NOT NULL,
    discount_percent DECIMAL(5,2),
    sale_amount      DECIMAL(12,2) NOT NULL,
    profit           DECIMAL(12,2) NOT NULL,
    cost_amount      DECIMAL(12,2) NOT NULL
);

-- ============================================================
-- 6. INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_sales_date       ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_month_year ON sales(month, year);
CREATE INDEX IF NOT EXISTS idx_sales_category   ON sales(category_id);
CREATE INDEX IF NOT EXISTS idx_sales_product    ON sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_festival   ON sales(is_festival_month);
CREATE INDEX IF NOT EXISTS idx_products_cat     ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_valid   ON products(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_fact_date        ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product     ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_category    ON fact_sales(category_key);

-- ============================================================
-- 7. ANALYTICAL VIEWS
-- ============================================================
DROP VIEW IF EXISTS monthly_sales_summary;
CREATE VIEW monthly_sales_summary AS
SELECT
    year, month,
    COUNT(*)                AS total_orders,
    SUM(quantity)           AS total_quantity,
    SUM(sale_amount)        AS total_sales,
    SUM(profit)             AS total_profit,
    AVG(discount_percent)   AS avg_discount,
    SUM(CASE WHEN is_festival_month THEN sale_amount ELSE 0 END) AS festival_sales
FROM sales
GROUP BY year, month
ORDER BY year, month;

DROP VIEW IF EXISTS category_performance;
CREATE VIEW category_performance AS
SELECT
    c.category_name,
    COUNT(s.sale_id)        AS total_orders,
    SUM(s.quantity)         AS total_quantity,
    SUM(s.sale_amount)      AS total_sales,
    SUM(s.profit)           AS total_profit,
    AVG(s.discount_percent) AS avg_discount,
    ROUND(AVG(s.profit * 1.0 / s.sale_amount * 100), 2) AS profit_margin_percent
FROM sales s
JOIN categories c ON s.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_sales DESC;

DROP VIEW IF EXISTS top_products;
CREATE VIEW top_products AS
SELECT
    p.product_name,
    c.category_name,
    COUNT(s.sale_id)      AS times_sold,
    SUM(s.quantity)       AS total_quantity_sold,
    SUM(s.sale_amount)    AS total_revenue,
    SUM(s.profit)         AS total_profit
FROM sales s
JOIN products   p ON s.product_id  = p.product_id
JOIN categories c ON s.category_id = c.category_id
GROUP BY p.product_name, c.category_name
ORDER BY total_revenue DESC;

DROP VIEW IF EXISTS festival_comparison;
CREATE VIEW festival_comparison AS
SELECT
    is_festival_month,
    festival_name,
    COUNT(*)            AS total_orders,
    SUM(sale_amount)    AS total_sales,
    SUM(profit)         AS total_profit,
    AVG(sale_amount)    AS avg_order_value
FROM sales
GROUP BY is_festival_month, festival_name
ORDER BY total_sales DESC;

DROP VIEW IF EXISTS sales_unified;
CREATE VIEW sales_unified AS
SELECT * FROM sales_2023
UNION ALL
SELECT * FROM sales_2024;

DROP VIEW IF EXISTS products_reconstructed;
CREATE VIEW products_reconstructed AS
SELECT
    pb.product_id, pb.product_name, pb.category_id, pb.stock_quantity,
    pp.cost_price, pp.selling_price, pp.valid_from, pp.valid_to,
    pb.created_at, pp.updated_at
FROM products_basic   pb
JOIN products_pricing pp ON pb.product_id = pp.product_id;
"""

def init_database():
    """Create schema, indexes, views, procedures using SQLite."""
    # SQLAlchemy sqlite3 driver gives access to standard connection with executescript
    raw = engine.raw_connection()
    try:
        raw.executescript(SCHEMA_SQL)
        raw.commit()
        print("OK: Database schema created successfully with all ADBMS concepts adapted for SQLite!")
    except Exception as exc:
        raw.rollback()
        print(f"ERROR: Schema creation failed: {exc}")
        raise
    finally:
        raw.close()

if __name__ == "__main__":
    init_database()