import json
import os
import glob
from flask import Flask, render_template, request, redirect, url_for
import pandas as pd

from Pipeline.silver_transform import run_silver_transformation
from Pipeline.gold_model import run_gold_modeling

app = Flask(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(ROOT_DIR, "data", "bronze")
SILVER_DIR = os.path.join(ROOT_DIR, "data", "silver")
GOLD_DIR = os.path.join(ROOT_DIR, "data", "gold")
LOG_FILE = os.path.join(ROOT_DIR, "logs", "audit_log.json")

os.makedirs(BRONZE_DIR, exist_ok=True)


@app.route("/", methods=["GET", "POST"])
def dashboard():
    if request.method == "POST":
        if "file" in request.files:
            file = request.files["file"]
            if file and file.filename != "":
                # Clear old raw bronze files so only the new upload is processed
                for f in glob.glob(os.path.join(BRONZE_DIR, "*")):
                    try:
                        os.remove(f)
                    except:
                        pass

                filename = file.filename
                save_path = os.path.join(BRONZE_DIR, filename)
                file.save(save_path)
                print(f"[UPLOAD] Saved new file: {save_path}")
                
                try:
                    run_silver_transformation()
                    run_gold_modeling()
                except Exception as e:
                    print(f"[ERROR] {e}")

                return redirect(url_for("dashboard"))

    # Load logs
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    logs = json.loads(content)
        except:
            logs = []

    # Load Bronze preview
    bronze_data = []
    bronze_files = glob.glob(os.path.join(BRONZE_DIR, "*"))
    if bronze_files:
        latest = max(bronze_files, key=os.path.getmtime)
        try:
            if latest.endswith(".json"):
                bronze_data = pd.read_json(latest).head(10).to_dict(orient="records")
            elif latest.endswith(".csv"):
                bronze_data = pd.read_csv(latest).head(10).to_dict(orient="records")
            elif latest.endswith((".xlsx", ".xls")):
                bronze_data = pd.read_excel(latest).head(10).to_dict(orient="records")
        except Exception as e:
            print(e)

    # Load Silver preview & Quarantine
    silver_data = []
    quarantine_data = []
    silver_files = glob.glob(os.path.join(SILVER_DIR, "silver_orders_*.parquet"))
    if silver_files:
        latest_silver = max(silver_files, key=os.path.getmtime)
        silver_data = pd.read_parquet(latest_silver).head(10).to_dict(orient="records")

    q_path = os.path.join(SILVER_DIR, "quarantine_orders.json")
    if os.path.exists(q_path):
        try:
            with open(q_path, "r", encoding="utf-8") as f:
                quarantine_data = json.load(f)[:10]
        except:
            pass

    # Load Gold preview
    fact_sales = []
    dim_product = []
    fact_path = os.path.join(GOLD_DIR, "fact_sales.parquet")
    if os.path.exists(fact_path):
        fact_sales = pd.read_parquet(fact_path).head(10).to_dict(orient="records")
        
    prod_path = os.path.join(GOLD_DIR, "dim_product.parquet")
    if os.path.exists(prod_path):
        dim_product = pd.read_parquet(prod_path).to_dict(orient="records")

    return render_template(
        "index.html", 
        logs=logs, 
        bronze_data=bronze_data, 
        silver_data=silver_data, 
        quarantine_data=quarantine_data,
        fact_sales=fact_sales,
        dim_product=dim_product
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)