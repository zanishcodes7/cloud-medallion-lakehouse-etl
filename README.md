# ☁️ Cloud-Scale Medallion Lakehouse & ETL Pipeline

An enterprise-grade, end-to-end data engineering pipeline and monitoring dashboard built to demonstrate modern data lakehouse architecture, automated data quality governance, and dimensional modeling.


https://github.com/user-attachments/assets/32b9caae-1ae7-40ab-aa21-cf081da9da1a



---

## 🏗️ Architecture Overview

The pipeline processes raw data through the classic **Medallion Architecture**, ensuring high data reliability and readiness for downstream business analytics:

1. **🥉 Bronze Layer (Raw Ingestion):** 
   * Self-service upload portal accepts multi-format files (CSV, Excel, JSON).
   * Acts as an immutable raw landing zone preserving original file lineage and structure.
2. **🥈 Silver Layer (Cleansed & Quarantine):** 
   * Automated Python validation gates clean text, standardize regional naming conventions, and cast data types.
   * **Quarantine Engine:** Intercepts and isolates bad records (e.g., negative/zero quantities, missing prices) into a separate audit store to prevent data corruption.
   * Stores valid records as high-performance **Apache Parquet**.
3. **🥇 Gold Layer (Kimball Star Schema):** 
   * Transforms cleansed records into business-ready relational models.
   * Contains central **Fact Sales** tables and descriptive **Dimension Product** tables optimized for BI tools (Power BI, Tableau).

---

## 📊 Key Features
* **Interactive Web Dashboard:** Built with Flask and Tailwind CSS (styled in a warm espresso and bronze theme).
* **Self-Service Ingestion Portal:** Allows users to upload custom datasets and instantly trigger end-to-end pipeline execution.
* **Real-Time Telemetry & Audit Logs:** Tracks every pipeline run, row count, and anomaly isolation timestamp in `logs/audit_log.json`.
* **Dynamic Table Rendering:** Automatically adapts table schemas to any uploaded custom dataset.

---

## 🛠️ Tech Stack
* **Language:** Python 3.10+
* **Data Processing:** Pandas, NumPy, PyArrow (Parquet), OpenPyXL
* **Web Framework:** Flask
* **Frontend Styling:** Tailwind CSS
* **Version Control:** Git & GitHub

---

## 🚀 Getting Started & Running Locally

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/zanishcodes7/cloud-medallion-pipeline.git](https://github.com/zanishcodes7/cloud-medallion-pipeline.git)
   cd Cloud-Medallion-Pipeline
