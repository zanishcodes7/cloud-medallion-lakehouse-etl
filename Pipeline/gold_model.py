import json
import os
import glob
from datetime import datetime
import pandas as pd

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

SILVER_DIR = os.path.join(ROOT_DIR, "data", "silver")
GOLD_DIR = os.path.join(ROOT_DIR, "data", "gold")
LOG_DIR = os.path.join(ROOT_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "audit_log.json")

os.makedirs(GOLD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def log_audit_event(layer: str, status: str, rows_processed: int, anomalies_detected: int, details: str):
    """Appends pipeline telemetry for the monitoring dashboard."""
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "layer": layer,
        "status": status,
        "rows_processed": rows_processed,
        "anomalies_detected": anomalies_detected,
        "details": details,
    }
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    logs = json.loads(content)
        except json.JSONDecodeError:
            logs = []
            
    logs.insert(0, entry)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs[:50], f, indent=2)


def run_gold_modeling() -> str:
    """
    Executes Gold Layer Kimball Star Schema modeling:
    1. Loads the latest clean Silver Parquet dataset.
    2. Builds Dimension tables (dim_product, dim_customer, dim_date).
    3. Builds Fact table (fact_sales) with foreign keys and measures.
    4. Exports all tables to data/gold/ as Parquet.
    """
    # Find the most recent Silver Parquet file
    silver_files = glob.glob(os.path.join(SILVER_DIR, "silver_orders_*.parquet"))
    if not silver_files:
        print("[GOLD] Error: No Silver Parquet files found to process!")
        return ""

    latest_silver_file = max(silver_files, key=os.path.getmtime)
    print(f"[GOLD] Reading latest Silver file: {latest_silver_file}")

    # Load clean data
    df = pd.read_parquet(latest_silver_file)
    initial_count = len(df)

    # Ensure timestamp is parsed properly for date dimension extraction
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], errors="coerce")
    df["order_date"] = df["order_timestamp"].dt.date

    # 1. Build dim_product
    dim_product = df[["product_sku", "product_name", "category", "unit_price"]].drop_duplicates(subset=["product_sku"]).copy()
    dim_product = dim_product.rename(columns={"unit_price": "standard_price"})
    dim_product.to_parquet(os.path.join(GOLD_DIR, "dim_product.parquet"), index=False, engine="pyarrow")

    # 2. Build dim_customer
    dim_customer = df[["customer_id", "customer_email", "region"]].drop_duplicates(subset=["customer_id"]).copy()
    dim_customer.to_parquet(os.path.join(GOLD_DIR, "dim_customer.parquet"), index=False, engine="pyarrow")

    # 3. Build dim_date
    unique_dates = df["order_date"].dropna().unique()
    date_rows = []
    for d in unique_dates:
        dt = pd.to_datetime(d)
        date_rows.append({
            "order_date": d,
            "year": dt.year,
            "quarter": dt.quarter,
            "month": dt.month,
            "month_name": dt.strftime("%B"),
            "day": dt.day,
            "day_of_week": dt.strftime("%A")
        })
    dim_date = pd.DataFrame(date_rows)
    dim_date.to_parquet(os.path.join(GOLD_DIR, "dim_date.parquet"), index=False, engine="pyarrow")

    # 4. Build fact_sales (Central Fact Table)
    df["total_sales_amount"] = df["quantity"] * df["unit_price"]
    
    fact_sales = df[[
        "order_id", "customer_id", "product_sku", "order_date", 
        "quantity", "unit_price", "total_sales_amount", "order_status", "_batch_id"
    ]].copy()
    
    fact_sales.to_parquet(os.path.join(GOLD_DIR, "fact_sales.parquet"), index=False, engine="pyarrow")

    log_audit_event(
        layer="Gold Modeling",
        status="Success",
        rows_processed=initial_count,
        anomalies_detected=0,
        details=f"Built Kimball Star Schema: 1 Fact table ({len(fact_sales)} rows) and 3 Dimension tables."
    )

    print(f"[GOLD] Star Schema successfully built and saved to -> {GOLD_DIR}")
    print(f"       - fact_sales.parquet ({len(fact_sales)} rows)")
    print(f"       - dim_product.parquet ({len(dim_product)} records)")
    print(f"       - dim_customer.parquet ({len(dim_customer)} records)")
    print(f"       - dim_date.parquet ({len(dim_date)} records)")
    
    return GOLD_DIR


if __name__ == "__main__":
    run_gold_modeling()