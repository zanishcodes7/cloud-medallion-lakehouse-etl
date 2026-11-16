import json
import os
import glob
from datetime import datetime
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

SILVER_DIR = os.path.join(ROOT_DIR, "data", "silver")
GOLD_DIR = os.path.join(ROOT_DIR, "data", "gold")
LOG_FILE = os.path.join(ROOT_DIR, "logs", "audit_log.json")

os.makedirs(GOLD_DIR, exist_ok=True)


def log_audit_event(layer: str, status: str, rows_processed: int, anomalies_detected: int, details: str):
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
    Executes Gold Layer modeling:
    1. Reads the latest cleansed Silver Parquet file.
    2. Builds Kimball Star Schema (Fact Sales and Dimension tables).
    """
    silver_files = glob.glob(os.path.join(SILVER_DIR, "silver_orders_*.parquet"))
    if not silver_files:
        print("[GOLD] Error: No cleansed Silver Parquet files found!")
        return ""

    latest_silver = max(silver_files, key=os.path.getmtime)
    print(f"[GOLD] Reading latest Silver file: {latest_silver}")
    
    df = pd.read_parquet(latest_silver)
    row_count = len(df)

    # Ensure required financial metric exists
    if "total_sales_amount" not in df.columns:
        if "quantity" in df.columns and "unit_price" in df.columns:
            df["total_sales_amount"] = df["quantity"] * df["unit_price"]
        else:
            df["total_sales_amount"] = 0.0

    # 1. Build Dimension: Product (safely checking columns)
    dim_cols = [c for c in ["product_sku", "product_name", "category"] if c in df.columns]
    if dim_cols:
        dim_product = df[dim_cols].drop_duplicates().copy()
    else:
        dim_product = pd.DataFrame({"product_sku": ["GENERIC-SKU"], "product_name": ["General Item"], "category": ["General"]})

    dim_product_path = os.path.join(GOLD_DIR, "dim_product.parquet")
    dim_product.to_parquet(dim_product_path, index=False)

    # 2. Build Fact: Sales
    fact_cols = [c for c in ["order_id", "customer_id", "product_sku", "region", "quantity", "unit_price", "total_sales_amount", "order_status", "order_timestamp"] if c in df.columns]
    fact_sales = df[fact_cols].copy()

    fact_sales_path = os.path.join(GOLD_DIR, "fact_sales.parquet")
    fact_sales.to_parquet(fact_sales_path, index=False)

    log_audit_event(
        layer="Gold Modeling",
        status="Success",
        rows_processed=row_count,
        anomalies_detected=0,
        details=f"Built Kimball Star Schema: 1 Fact table ({row_count} rows) and Dimension tables."
    )

    print(f"[GOLD] Successfully built Star Schema -> {fact_sales_path}")
    return fact_sales_path


if __name__ == "__main__":
    run_gold_modeling()