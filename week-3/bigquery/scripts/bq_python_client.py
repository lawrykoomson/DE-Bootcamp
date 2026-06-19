# ============================================
# WEEK 3 — BIGQUERY MODULE 2: Python Client
# Load Data, Query, Export Results
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import (
    LoadJobConfig,
    SourceFormat,
    WriteDisposition)

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/module2.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

PROJECT_ID = "de-bootcamp-sandbox"
DATASET_ID = "telco_de_bootcamp"
TABLE_NAME = "telco_customers"
TABLE_ID = (
    f"{PROJECT_ID}.{DATASET_ID}"
    f".{TABLE_NAME}")

client = bigquery.Client(
    project=PROJECT_ID)

print("=" * 55)
print("   MODULE 2 — BIGQUERY PYTHON CLIENT")
print("   LOAD DATA | QUERY | EXPORT")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN SCHEMA & AUTODETECT
# ─────────────────────────────────────────────
print("\n📌 1. SCHEMA — AUTODETECT VS MANUAL")
print("""
   BigQuery has two ways to define schema:

   1. Manual schema → You define every column
      Best for: production pipelines
      Pro: strict, no surprises
      Con: must match CSV exactly

   2. Autodetect → BQ reads your CSV header
      Best for: development & learning
      Pro: fast, no column count mismatch
      Con: may guess types wrong

   We use autodetect=True for this module
   so BigQuery detects all 23 columns from
   the telco CSV automatically!
""")

# ─────────────────────────────────────────────
# STEP 2: DELETE + RECREATE TABLE
# ─────────────────────────────────────────────
print("\n📌 2. PREPARING TABLE")
logger.info("Preparing BigQuery table")

# Delete existing table if it exists
try:
    client.delete_table(
        TABLE_ID, not_found_ok=True)
    print(f"   ✅ Old table cleared")
    logger.info("Old table deleted")
except Exception as e:
    print(f"   ℹ️  {e}")

# ─────────────────────────────────────────────
# STEP 3: LOAD CSV WITH AUTODETECT
# ─────────────────────────────────────────────
print("\n📌 3. LOADING CSV WITH AUTODETECT")
logger.info("Loading CSV to BigQuery")

CLEAN_CSV = Path(
    "../../../week-2/data/processed/"
    "telco_clean.csv")

print(f"   Source: {CLEAN_CSV}")
print(f"   Target: {TABLE_ID}")
print(f"   Schema: autodetect")

job_config = LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=SourceFormat.CSV,
    write_disposition=(
        WriteDisposition.WRITE_TRUNCATE),
    allow_quoted_newlines=True,
    allow_jagged_rows=True,
)

try:
    with open(CLEAN_CSV, "rb") as f:
        load_job = (
            client.load_table_from_file(
                f,
                TABLE_ID,
                job_config=job_config))

    print(f"   ⏳ Loading to BigQuery...")
    load_job.result()

    table = client.get_table(TABLE_ID)
    print(f"   ✅ Load complete!")
    print(f"   Rows:     {table.num_rows:,}")
    print(f"   Columns:  "
          f"{len(table.schema)}")
    print(f"   Size:     "
          f"{table.num_bytes:,} bytes")

    print(f"\n   Detected schema (first 8):")
    for field in table.schema[:8]:
        print(f"   {field.name:<25} "
              f"{field.field_type}")
    print(f"   ... and "
          f"{len(table.schema)-8} more")

    logger.info(
        f"Loaded {table.num_rows:,} rows, "
        f"{len(table.schema)} columns")

except Exception as e:
    print(f"   ❌ Load error: {e}")
    logger.error(f"Load error: {e}")
    exit(1)

# ─────────────────────────────────────────────
# STEP 4: QUERY THE LOADED TABLE
# ─────────────────────────────────────────────
print("\n📌 4. QUERYING YOUR OWN TABLE")
logger.info("Querying loaded table")

print("\n   🔍 Total customers and churn:")
query = f"""
    SELECT
        COUNT(*) as total_customers,
        COUNTIF(Churn = TRUE) as churned,
        ROUND(
            COUNTIF(Churn = TRUE) * 100.0 /
            COUNT(*), 2)
            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2)
            as total_revenue
    FROM `{TABLE_ID}`
"""

try:
    job = client.query(query)
    result = job.result()
    for row in result:
        print(f"   Total customers: "
              f"{row.total_customers:,}")
        print(f"   Churned:         "
              f"{row.churned:,}")
        print(f"   Churn rate:      "
              f"{row.churn_rate_pct}%")
        print(f"   Avg monthly:     "
              f"${row.avg_monthly:,.2f}")
        print(f"   Total revenue:   "
              f"${row.total_revenue:,.2f}")
    logger.info("KPI query complete")

except Exception as e:
    print(f"   ❌ Query error: {e}")
    logger.error(f"Query error: {e}")

print("\n   🔍 Churn by contract type:")
query2 = f"""
    SELECT
        Contract,
        COUNT(*) as total,
        COUNTIF(Churn = TRUE) as churned,
        ROUND(
            COUNTIF(Churn = TRUE) * 100.0 /
            COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly
    FROM `{TABLE_ID}`
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
"""

try:
    job2 = client.query(query2)
    rows = list(job2.result())
    print(f"   {'Contract':<20} "
          f"{'Total':>6} "
          f"{'Churned':>8} "
          f"{'Rate':>6} "
          f"{'Avg $':>8}")
    print("   " + "-" * 55)
    for row in rows:
        print(
            f"   {row.Contract:<20} "
            f"{row.total:>6,} "
            f"{row.churned:>8,} "
            f"{row.churn_rate_pct:>5.1f}% "
            f"${row.avg_monthly:>7.2f}")
    logger.info(
        f"Contract query: {len(rows)} rows")

except Exception as e:
    print(f"   ❌ Query error: {e}")
    logger.error(f"Query error: {e}")

# ─────────────────────────────────────────────
# STEP 5: EXPORT RESULTS TO PANDAS
# ─────────────────────────────────────────────
print("\n📌 5. EXPORTING RESULTS TO PANDAS")
logger.info("Exporting to Pandas")

query3 = f"""
    SELECT
        Contract,
        InternetService,
        COUNT(*) as customers,
        COUNTIF(Churn = TRUE) as churned,
        ROUND(
            COUNTIF(Churn = TRUE) * 100.0 /
            COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
            as total_revenue
    FROM `{TABLE_ID}`
    GROUP BY Contract, InternetService
    ORDER BY churn_rate_pct DESC
"""

try:
    job3 = client.query(query3)
    df = job3.to_dataframe()

    print(f"   ✅ Exported to DataFrame!")
    print(f"   Shape: {df.shape}")
    print(f"\n{df.to_string(index=False)}")

    output_path = (
        "../data/bq_churn_analysis.csv")
    df.to_csv(output_path, index=False)
    print(f"\n   ✅ Saved: {output_path}")
    logger.info(
        f"Exported: {df.shape}")

except Exception as e:
    print(f"   ❌ Export error: {e}")
    logger.error(f"Export error: {e}")

# ─────────────────────────────────────────────
# STEP 6: TABLE INFORMATION
# ─────────────────────────────────────────────
print("\n📌 6. TABLE INFORMATION")

try:
    table_info = client.get_table(TABLE_ID)
    print(f"   Table:    "
          f"{table_info.table_id}")
    print(f"   Rows:     "
          f"{table_info.num_rows:,}")
    print(f"   Columns:  "
          f"{len(table_info.schema)}")
    print(f"   Size:     "
          f"{table_info.num_bytes:,} bytes")
    print(f"   Created:  "
          f"{table_info.created.strftime('%Y-%m-%d %H:%M')}")
    logger.info(
        f"Table: {table_info.num_rows:,} rows")

except Exception as e:
    print(f"   ❌ Error: {e}")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   autodetect=True     → Auto schema")
print("   load_table_from_file→ Upload CSV")
print("   WRITE_TRUNCATE      → Overwrite")
print("   client.query()      → Run SQL")
print("   COUNTIF()           → BQ's SUM(CASE)")
print("   job.to_dataframe()  → BQ → Pandas")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 complete")