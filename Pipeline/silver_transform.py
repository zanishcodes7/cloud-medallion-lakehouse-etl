import json
import os
import glob
from datetime import datetime
import pandas as pd

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

BRONZE_DIR = os.path.join(ROOT_DIR, "data", "bronze")
SILVER_DIR = os.path.join(ROOT_DIR, "data", "silver")
LOG_DIR = os.path.join(ROOT_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "audit_log.json")

os.makedirs(SILVER_DIR, exist_ok=True)
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


def run_silver_transformation() -> str:
    """
    Executes Silver Layer transformation:
    1. Finds the latest raw file in Bronze (JSON, CSV, or Excel).
    2. Cleanses text, standardizes regions and dates.
    3. Quarantines rows with invalid quantities or missing prices.
    4. Deduplicates and saves as Parquet.
    """
    # Find the most recent file in Bronze directory
    bronze_files = glob.glob(os.path.join(BRONZE_DIR, "*"))
    if not bronze_files:
        print("[SILVER] Error: No Bronze data files found to process!")
        return ""

    latest_bronze_file = max(bronze_files, key=os.path.getmtime)
    print(f"[SILVER] Reading latest Bronze file: {latest_bronze_file}")

    # Load data based on file extension
    try:
        if latest_bronze_file.endswith(".json"):
            df = pd.read_json(latest_bronze_file)
        elif latest_bronze_file.endswith(".csv"):
            df = pd.read_csv(latest_bronze_file)
        elif latest_bronze_file.endswith((".xlsx", ".xls")):
            df = pd.read_excel(latest_bronze_file)
        else:
            print(f"[SILVER] Error: Unsupported file format for {latest_bronze_file}")
            return ""
    except Exception as e:
        print(f"[SILVER] Error reading file: {e}")
        return ""

    initial_count = len(df)

    # 1. Standardize text fields & trim whitespace (if columns exist)
    if "region" in df.columns:
        df["region"] = df["region"].astype(str).str.strip().str.title()
        df["region"] = df["region"].replace("Nan", "Unknown")
    if "order_status" in df.columns:
        df["order_status"] = df["order_status"].astype(str).str.upper()

    # 2. Convert data types safely
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    else:
        df["quantity"] = 1  # Fallback if custom file lacks quantity

    if "unit_price" in df.columns:
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    else:
        df["unit_price"] = 0.0  # Fallback if custom file lacks price

    # 3. Quality Assertions / Quarantine Rules
    is_valid_qty = df["quantity"] > 0
    is_valid_price = df["unit_price"].notnull() & (df["unit_price"] > 0)
    
    valid_mask = is_valid_qty & is_valid_price

    df_valid = df[valid_mask].copy()
    df_quarantine = df[~valid_mask].copy()

    anomalies_count = len(df_quarantine)

    # 4. Deduplicate if order_id exists
    if "order_id" in df_valid.columns:
        df_valid = df_valid.drop_duplicates(subset=["order_id"], keep="last")
    deduped_count = len(df_valid)

    # 5. Save Quarantined records if any exist
    if not df_quarantine.empty:
        quarantine_path = os.path.join(SILVER_DIR, "quarantine_orders.json")
        df_quarantine.to_json(quarantine_path, orient="records", indent=2)

    # 6. Save Cleansed dataset as Parquet
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_parquet_path = os.path.join(SILVER_DIR, f"silver_orders_{timestamp_str}.parquet")
    
    df_valid.to_parquet(output_parquet_path, index=False, engine="pyarrow")

    log_audit_event(
        layer="Silver Transform",
        status="Success",
        rows_processed=deduped_count,
        anomalies_detected=anomalies_count,
        details=f"Cleaned {initial_count} rows. Quarantined {anomalies_count} invalid records. Saved to Parquet."
    )

    print(f"[SILVER] Successfully transformed {deduped_count} valid rows -> {output_parquet_path}")
    print(f"[SILVER] Quarantined {anomalies_count} anomaly records.")
    return output_parquet_path


if __name__ == "__main__":
    run_silver_transformation()