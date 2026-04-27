"""
Generate 2 years (2023-2024) of realistic Indian retail sales data.
Covers seasonal patterns, festival spikes, proper ADBMS table population.
SQLite Port.
"""
import os
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

DB_PATH = ROOT_DIR / "retail_shop.db"

#  Catalogue 
CATEGORIES: dict[str, list[tuple[str, int, int]]] = {
    "Electronics": [
        ("Mobile Phone",      15000, 18000),
        ("Laptop",            40000, 50000),
        ("Headphones",         1500,  2000),
        ("Smart Watch",        8000, 10000),
        ("Tablet",            20000, 25000),
        ("Power Bank",         1000,  1500),
        ("Bluetooth Speaker",  2500,  3500),
        ("USB Cable",           200,   400),
    ],
    "Clothing": [
        ("T-Shirt",   300,  500),
        ("Jeans",     800, 1200),
        ("Shirt",     600,  900),
        ("Dress",    1000, 1500),
        ("Jacket",   1500, 2500),
        ("Saree",    2000, 3000),
        ("Kurta",     700, 1000),
        ("Shoes",    1200, 1800),
    ],
    "Groceries": [
        ("Rice (1kg)",        50,  80),
        ("Wheat Flour (1kg)", 40,  60),
        ("Sugar (1kg)",       45,  70),
        ("Cooking Oil (1L)", 120, 180),
        ("Tea (250g)",       150, 250),
        ("Coffee (200g)",    300, 450),
        ("Milk (1L)",         50,  70),
        ("Biscuits Pack",     40,  80),
    ],
    "Home & Kitchen": [
        ("Pressure Cooker",   1500, 2000),
        ("Mixer Grinder",     2500, 3500),
        ("Dinner Set",        1200, 1800),
        ("Bedsheet",           800, 1200),
        ("Curtains",          1000, 1500),
        ("Towel Set",          500,  800),
        ("Kitchen Knife Set",  600,  900),
        ("Water Bottle",       200,  400),
    ],
    "Sports": [
        ("Cricket Bat",      1500, 2500),
        ("Football",          500,  800),
        ("Badminton Racket", 1000, 1500),
        ("Yoga Mat",          500,  800),
        ("Gym Bag",           800, 1200),
        ("Sports Shoes",     2000, 3000),
        ("Fitness Band",     1500, 2000),
        ("Water Bottle",      300,  500),
    ],
}

# Festival windows: centre date  name
FESTIVALS: dict[str, str] = {
    "2023-03-08": "Holi",
    "2023-04-22": "Eid",
    "2023-11-12": "Diwali",
    "2023-11-13": "Diwali",
    "2023-12-25": "Christmas",
    "2024-03-25": "Holi",
    "2024-04-11": "Eid",
    "2024-11-01": "Diwali",
    "2024-11-02": "Diwali",
    "2024-12-25": "Christmas",
}


def festival_for(date: datetime) -> tuple[bool, str | None]:
    for fdate_str, fname in FESTIVALS.items():
        fdate = datetime.strptime(fdate_str, "%Y-%m-%d")
        if abs((date - fdate).days) <= 5:
            return True, fname
    return False, None


def generate_data() -> None:
    # Ensure DB is created with schema
    import database
    database.init_database()
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        print(" Starting data generation")

        #  1. Categories 
        print(" Inserting categories")
        cat_ids: dict[str, int] = {}
        for cname in CATEGORIES:
            cur.execute(
                """
                INSERT INTO categories (category_name, description)
                VALUES (?, ?)
                ON CONFLICT (category_name) DO UPDATE SET category_name = excluded.category_name
                """,
                (cname, f"{cname} products for retail"),
            )
            cur.execute("SELECT category_id FROM categories WHERE category_name = ?", (cname,))
            cat_ids[cname] = cur.fetchone()[0]
        conn.commit()
        print(f"    {len(cat_ids)} categories")

        #  2. Products 
        print(" Inserting products")
        products: list[dict] = []
        for cname, items in CATEGORIES.items():
            for pname, cost, sell in items:
                cur.execute(
                    """
                    INSERT INTO products
                        (product_name, category_id, cost_price, selling_price,
                         stock_quantity, valid_from, valid_to)
                    VALUES (?, ?, ?, ?, 1000, '2023-01-01', '9999-12-31')
                    """,
                    (pname, cat_ids[cname], cost, sell),
                )
                pid = cur.lastrowid
                products.append(
                    dict(id=pid, name=pname, category_id=cat_ids[cname],
                         category_name=cname, cost=cost, sell=sell)
                )
        conn.commit()
        print(f"    {len(products)} products")

        #  3. Master  Replica (Replication demo) 
        print(" Populating master-replica tables")
        for p in products:
            cur.execute(
                """
                INSERT INTO products_master
                    (product_id, product_name, category_id, cost_price, selling_price, stock_quantity)
                VALUES (?, ?, ?, ?, ?, 1000)
                """,
                (p["id"], p["name"], p["category_id"], p["cost"], p["sell"]),
            )
        # Replicate products (Python equivalent of stored procedure)
        cur.execute("DELETE FROM products_replica")
        cur.execute(
            """
            INSERT INTO products_replica 
            (product_id, product_name, category_id, cost_price, selling_price, stock_quantity, replicated_at)
            SELECT product_id, product_name, category_id, cost_price, selling_price, stock_quantity, CURRENT_TIMESTAMP
            FROM products_master
            """
        )
        conn.commit()
        print("    Master  Replica populated")

        #  4. Vertical fragments 
        print(" Populating vertical fragments")
        for p in products:
            cur.execute(
                "INSERT INTO products_basic (product_id, product_name, category_id, stock_quantity) "
                "VALUES (?, ?, ?, 1000)",
                (p["id"], p["name"], p["category_id"]),
            )
            cur.execute(
                "INSERT INTO products_pricing (product_id, cost_price, selling_price, valid_from, valid_to) "
                "VALUES (?, ?, ?, '2023-01-01', '9999-12-31')",
                (p["id"], p["cost"], p["sell"]),
            )
        conn.commit()
        print("    Vertical fragments populated")

        #  5. Time dimension 
        print(" Building time dimension (2023-2024)")
        d = datetime(2023, 1, 1)
        end = datetime(2024, 12, 31)
        while d <= end:
            is_festival, fname = festival_for(d)
            cur.execute(
                """
                INSERT INTO dim_time
                    (date_key, full_date, day, month, month_name, quarter, year,
                     day_of_week, day_name, is_weekend, is_festival, festival_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO NOTHING
                """,
                (
                    int(d.strftime("%Y%m%d")), d.strftime("%Y-%m-%d"),
                    d.day, d.month, d.strftime("%B"),
                    (d.month - 1) // 3 + 1, d.year,
                    d.weekday(), d.strftime("%A"),
                    int(d.weekday() >= 5), int(is_festival), fname,
                ),
            )
            d += timedelta(days=1)
        conn.commit()
        print("    731 date records inserted")

        #  6. Sales (main + horizontal fragments) 
        print(" Generating sales records")
        sale_count = 0
        d = datetime(2023, 1, 1)
        end = datetime(2024, 12, 31)
        start = d
        while d <= end:
            is_festival, fname = festival_for(d)
            n = random.randint(20, 40) if is_festival else random.randint(5, 15)
            for _ in range(n):
                p = random.choice(products)
                qty = random.randint(1, 5)
                disc = random.uniform(5, 20) if is_festival else random.uniform(0, 10)
                amount = qty * p["sell"] * (1 - disc / 100)
                profit = amount - qty * p["cost"]
                row = (
                    p["id"], p["category_id"], qty, round(disc, 2),
                    round(amount, 2), round(profit, 2),
                    d.strftime("%Y-%m-%d"), d.month, d.year, int(is_festival), fname,
                )
                cur.execute(
                    """INSERT INTO sales
                       (product_id, category_id, quantity, discount_percent,
                        sale_amount, profit, sale_date, month, year,
                        is_festival_month, festival_name)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    row,
                )
                frag_table = "sales_2023" if d.year == 2023 else "sales_2024"
                cur.execute(
                    f"""INSERT INTO {frag_table}
                       (product_id, category_id, quantity, discount_percent,
                        sale_amount, profit, sale_date, month, year,
                        is_festival_month, festival_name)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    row,
                )
                sale_count += 1

            elapsed = (d - start).days
            if elapsed % 100 == 0:
                conn.commit()
                print(f"   Progress {d.strftime('%Y-%m-%d')}: {sale_count} sales")
            d += timedelta(days=1)
        conn.commit()
        print(f"    {sale_count} sales records generated")

        #  7. Data Warehouse 
        print(" Populating data warehouse (Star Schema)")
        for cname, cid in cat_ids.items():
            cur.execute(
                "INSERT INTO dim_category (category_id, category_name, description) VALUES (?,?,?)",
                (cid, cname, f"{cname} products"),
            )
        for p in products:
            cur.execute(
                "INSERT INTO dim_product (product_id, product_name, category_name, cost_price, selling_price) "
                "VALUES (?,?,?,?,?)",
                (p["id"], p["name"], p["category_name"], p["cost"], p["sell"]),
            )
            
        cur.execute(
            """
            INSERT INTO fact_sales
                (date_key, product_key, category_key, quantity, discount_percent,
                 sale_amount, profit, cost_amount)
            SELECT
                CAST(strftime('%Y%m%d', s.sale_date) AS INTEGER),
                dp.product_key, dc.category_key,
                s.quantity, s.discount_percent,
                s.sale_amount, s.profit,
                s.sale_amount - s.profit
            FROM sales s
            JOIN dim_product  dp ON s.product_id  = dp.product_id
            JOIN dim_category dc ON s.category_id = dc.category_id
            """
        )
        conn.commit()
        print("    Data warehouse populated")

        #  Summary 
        print("\n" + "=" * 52)
        print("  DATA GENERATION SUMMARY")
        print("=" * 52)
        for label, sql in [
            ("Categories",            "SELECT COUNT(*) FROM categories"),
            ("Products",              "SELECT COUNT(*) FROM products"),
            ("Total Sales",           "SELECT COUNT(*) FROM sales"),
            ("Festival Sales",        "SELECT COUNT(*) FROM sales WHERE is_festival_month = 1"),
            ("Sales 2023 (fragment)", "SELECT COUNT(*) FROM sales_2023"),
            ("Sales 2024 (fragment)", "SELECT COUNT(*) FROM sales_2024"),
            ("DWH Fact Rows",         "SELECT COUNT(*) FROM fact_sales"),
        ]:
            cur.execute(sql)
            print(f"  {label:28s}: {cur.fetchone()[0]:,}")
        cur.execute("SELECT SUM(sale_amount), SUM(profit) FROM sales")
        rev, prof = cur.fetchone()
        print(f"  {'Total Revenue':28s}: {float(rev):,.2f}")
        print(f"  {'Total Profit':28s}: {float(prof):,.2f}")
        print("=" * 52)
        print(" Data generation complete!")

    except Exception as exc:
        conn.rollback()
        print(f" Error: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    generate_data()
