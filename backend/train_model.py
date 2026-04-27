"""

"""
import json
import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

DB_PATH = ROOT_DIR / "retail_shop.db"

MODEL_DIR = ROOT_DIR / "models"


def load_training_data() -> pd.DataFrame:
    print(" Loading training data")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT
            s.product_id, p.product_name,
            c.category_name,
            s.quantity, s.discount_percent,
            s.sale_amount, s.profit,
            s.month, s.year,
            s.is_festival_month,
            p.cost_price, p.selling_price
        FROM sales s
        JOIN products   p ON s.product_id  = p.product_id
        JOIN categories c ON s.category_id = c.category_id
        """,
        conn,
    )
    conn.close()
    print(f"    {len(df):,} rows loaded")
    return df


def feature_engineer(df: pd.DataFrame):
    # Historical averages per category / month
    cat_avg = (
        df.groupby("category_name")["sale_amount"]
        .mean()
        .reset_index()
        .rename(columns={"sale_amount": "category_avg_sale"})
    )
    month_avg = (
        df.groupby("month")["sale_amount"]
        .mean()
        .reset_index()
        .rename(columns={"sale_amount": "month_avg_sale"})
    )

    df = df.merge(cat_avg,   on="category_name", how="left")
    df = df.merge(month_avg, on="month",          how="left")

    le = LabelEncoder()
    df["category_encoded"]  = le.fit_transform(df["category_name"])
    df["is_festival_int"]   = df["is_festival_month"].astype(int)
    df["profit_margin"]     = (df["selling_price"] - df["cost_price"]) / df["selling_price"]
    df["base_sale_value"]   = df["quantity"] * df["selling_price"]

    feature_cols = [
        "category_encoded", "month", "quantity", "discount_percent",
        "is_festival_int", "cost_price", "selling_price",
        "category_avg_sale", "month_avg_sale", "profit_margin", "base_sale_value",
    ]
    return df, feature_cols, le, cat_avg, month_avg


def train_and_evaluate(X, y, model, label: str) -> dict:
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = {
        "mae":  mean_absolute_error(y_test, preds),
        "rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
        "r2":   r2_score(y_test, preds),
    }
    print(f"   {label}")
    print(f"     MAE={metrics['mae']:,.2f}  RMSE={metrics['rmse']:,.2f}  R={metrics['r2']:.4f}")
    return metrics


def save_artifacts(models: dict, le, feature_cols, cat_avg, month_avg):
    MODEL_DIR.mkdir(exist_ok=True)
    joblib.dump(models["lr_sale"],    MODEL_DIR / "lr_sale.pkl")
    joblib.dump(models["lr_profit"],  MODEL_DIR / "lr_profit.pkl")
    joblib.dump(models["rf_sale"],    MODEL_DIR / "rf_sale.pkl")
    joblib.dump(models["rf_profit"],  MODEL_DIR / "rf_profit.pkl")
    joblib.dump(le,                   MODEL_DIR / "label_encoder.pkl")

    metadata = {
        "feature_columns": feature_cols,
        "categories":      list(le.classes_),
        "category_avg":    cat_avg.to_dict("records"),
        "month_avg":       month_avg.to_dict("records"),
    }
    with open(MODEL_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"    Artifacts saved to {MODEL_DIR}")


def main():
    print("\n ML Training Pipeline\n" + "=" * 50)

    df = load_training_data()
    df, feature_cols, le, cat_avg, month_avg = feature_engineer(df)

    X = df[feature_cols]
    y_sale   = df["sale_amount"]
    y_profit = df["profit"]

    print("\n Training models")
    models = {}
    results = {}

    models["lr_sale"]   = LinearRegression()
    results["lr_sale"]  = train_and_evaluate(X, y_sale,   models["lr_sale"],   "LR  Sale Amount")

    models["lr_profit"] = LinearRegression()
    results["lr_profit"]= train_and_evaluate(X, y_profit, models["lr_profit"], "LR  Profit")

    models["rf_sale"]   = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
    results["rf_sale"]  = train_and_evaluate(X, y_sale,   models["rf_sale"],   "RF  Sale Amount")

    models["rf_profit"] = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
    results["rf_profit"]= train_and_evaluate(X, y_profit, models["rf_profit"], "RF  Profit")

    print("\n Saving artifacts")
    save_artifacts(models, le, feature_cols, cat_avg, month_avg)

    print("\n Training complete!\n" + "=" * 50)


if __name__ == "__main__":
    main()