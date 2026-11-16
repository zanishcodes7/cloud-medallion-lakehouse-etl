import json
import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

BRONZE_DIR = os.path.join(ROOT_DIR, "data", "bronze")
LOG_DIR = os.path.join(ROOT_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "audit_log.json")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def generate_messy_erp_orders(num_records: int = 250) -> list[dict]:
    products = [
        {"sku": "PRD-101", "name": "Enterprise Cloud Node", "category": "Infrastructure", "base_price": 450.00},
        {"sku": "PRD-102", "name": "Data Warehouse Seat", "category": "Analytics", "base_price": 120.00},
        {"sku": "PRD-103", "name": "ETL Connector Pack", "category": "Integration", "base_price": 85.50},
        {"sku": "PRD-104", "name": "AI Governance Module", "category": "Security", "base_price": 310.00},
        {"sku": "PRD-105", "name": "BI Semantic License", "category": "Analytics", "base_price": 95.00},
    ]
    regions = ["UK-London", "UK-Manchester", "EU-Frankfurt", "US-East", None, "  uk-london "]
    statuses = ["COMPLETED", "completed", "PENDING", "CANCELLED", "REFUNDED"]

    records = []
    base_date = datetime.now() - timedelta(days=30)

    for i in range(num_records):
        product = random.choice(products)
        
        # Initialize order_id first for this iteration
        order_id = f"ORD-{10000 + i}"

        # Introduce intentional duplicate anomaly (~5% chance if i > 5)
        if random.random() < 0.05 and i > 5:
            order_id = f"ORD-{10000 + i - 1}"

        qty = random.randint(1, 15)
        if random.random() < 0.04:
            qty = -random.randint(1, 5)  # Invalid negative quantity

        unit_price = product["base_price"]
        if random.random() < 0.03:
            unit_price = None  # Missing price anomaly

        raw_date = base_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        order_date_str = (
            raw_date.strftime("%Y-%m-%d %H:%M:%S")
            if random.random() > 0.2
            else raw_date.strftime("%d/%m/%Y %H:%M")
        )

        customer_id = f"CUST-{random.randint(500, 560)}"
        customer_email = (
            f"client_{customer_id.lower()}@enterprise.co.uk"
            if random.random() > 0.08
            else None
        )

        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_email": customer_email,
            "region": random.choice(regions),
            "product_sku": product["sku"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": qty,
            "unit_price": unit_price,
            "order_status": random.choice(statuses),
            "order_timestamp": order_date_str,
        })

    return records


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


def run_bronze_ingestion(num_records: int = 250) -> str:
    batch_id = str(uuid.uuid4())[:8]
    ingest_at = datetime.now().isoformat()

    raw_records = generate_messy_erp_orders(num_records=num_records)
    df = pd.DataFrame(raw_records)
    
    df["_batch_id"] = batch_id
    df["_ingested_at"] = ingest_at
    df["_source_system"] = "ERP_SALES_API_V2"

    df = df.astype(str)
    file_name = f"bronze_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{batch_id}.json"
    output_path = os.path.join(BRONZE_DIR, file_name)

    df.to_json(output_path, orient="records", indent=2)

    log_audit_event(
        layer="Bronze Ingestion",
        status="Success",
        rows_processed=len(df),
        anomalies_detected=0,
        details=f"Ingested batch {batch_id} ({len(df)} raw rows) from ERP_SALES_API_V2"
    )

    print(f"[BRONZE] Successfully landed {len(df)} raw records -> {output_path}")
    return output_path


if __name__ == "__main__":
    run_bronze_ingestion(250)