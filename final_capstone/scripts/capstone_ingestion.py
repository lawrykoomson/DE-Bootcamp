# ============================================
# FINAL CAPSTONE — MODULE 1: DATA INGESTION
# Extract from multiple sources into one
# unified data lake
# GetSkills Network DE Bootcamp
# Lawrence Koomson
# ============================================

import os
import json
import logging
import requests
import pandas as pd
import boto3
import io
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError
from google.cloud import bigquery

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = (
    "Telco Churn Intelligence Platform")
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"
RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
RUN_TS = datetime.now().strftime(
    "%Y%m%d_%H%M%S")

# AWS Config
AWS_REGION = "eu-north-1"
S3_BUCKET = (
    "lawrence-de-bootcamp-20260611")

# GCP Config
GCP_PROJECT = "de-bootcamp-sandbox"
BQ_DATASET = "telco_de_bootcamp"
BQ_TABLE = (
    f"{GCP_PROJECT}.{BQ_DATASET}"
    f".telco_customers")

# Local paths
CLEAN_CSV = Path(
    "../../week-2/data/processed/"
    "telco_clean.csv")

os.makedirs("../logs", exist_ok=True)
os.makedirs("../data", exist_ok=True)
os.makedirs("../reports", exist_ok=True)
os.makedirs("../charts", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/ingestion.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   MODULE 1 — DATA INGESTION")
print(f"   Version: {PIPELINE_VERSION}")
print(f"   Author:  {AUTHOR}")
print("=" * 55)

logger.info("=" * 40)
logger.info(
    f"CAPSTONE STARTED: "
    f"{PIPELINE_NAME}")
logger.info("=" * 40)

ingestion_stats = {
    "sources": {},
    "errors": []
}

# ─────────────────────────────────────────────
# SOURCE 1: LOCAL CSV — TELCO DATA
# ─────────────────────────────────────────────
print("\n📌 SOURCE 1: LOCAL TELCO CSV")
logger.info("Source 1: Local CSV")

try:
    df_telco = pd.read_csv(CLEAN_CSV)
    print(f"   ✅ Loaded: "
          f"{len(df_telco):,} rows x "
          f"{df_telco.shape[1]} columns")
    print(f"   Churn rate: "
          f"{df_telco['Churn'].mean()*100:.2f}%")
    logger.info(
        f"CSV loaded: "
        f"{len(df_telco):,} rows")
    ingestion_stats["sources"][
        "local_csv"] = {
        "rows": len(df_telco),
        "status": "SUCCESS"
    }

except Exception as e:
    print(f"   ❌ Error: {e}")
    logger.error(f"CSV error: {e}")
    ingestion_stats["errors"].append(
        f"CSV: {e}")

# ─────────────────────────────────────────────
# SOURCE 2: AWS S3
# ─────────────────────────────────────────────
print("\n📌 SOURCE 2: AWS S3")
logger.info("Source 2: AWS S3")

try:
    s3 = boto3.client(
        's3', region_name=AWS_REGION)
    s3.head_bucket(Bucket=S3_BUCKET)

    paginator = s3.get_paginator(
        'list_objects_v2')
    pages = paginator.paginate(
        Bucket=S3_BUCKET)
    all_objects = []
    total_size = 0
    for page in pages:
        objs = page.get('Contents', [])
        all_objects.extend(objs)
        total_size += sum(
            o['Size'] for o in objs)

    print(f"   ✅ S3 bucket connected!")
    print(f"   Objects: "
          f"{len(all_objects)}")
    print(f"   Size:    "
          f"{total_size/1024:.1f} KB")
    logger.info(
        f"S3: {len(all_objects)} objects")
    ingestion_stats["sources"]["s3"] = {
        "objects": len(all_objects),
        "size_kb": round(
            total_size/1024, 1),
        "status": "SUCCESS"
    }

    # Upload telco data to S3
    buf = io.StringIO()
    df_telco.to_csv(buf, index=False)
    s3_key = (
        f"capstone/raw/"
        f"telco_raw_{RUN_TS}.csv")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buf.getvalue().encode(
            'utf-8'),
        ContentType='text/csv',
        Metadata={
            'pipeline': 'capstone',
            'version': PIPELINE_VERSION,
            'author': AUTHOR
        })
    print(f"   ✅ Uploaded to S3: "
          f"{s3_key}")
    logger.info(
        f"Uploaded to S3: {s3_key}")

except Exception as e:
    print(f"   ⚠️ S3: {e}")
    logger.warning(f"S3 warning: {e}")
    ingestion_stats["sources"]["s3"] = {
        "status": "WARNING",
        "message": str(e)
    }

# ─────────────────────────────────────────────
# SOURCE 3: BIGQUERY
# ─────────────────────────────────────────────
print("\n📌 SOURCE 3: BIGQUERY")
logger.info("Source 3: BigQuery")

try:
    bq_client = bigquery.Client(
        project=GCP_PROJECT)

    job = bq_client.query(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(Churn = TRUE)
                as churned,
            ROUND(SUM(MonthlyCharges), 2)
                as total_revenue
        FROM `{BQ_TABLE}`
    """)
    bq_result = list(job.result())[0]

    print(f"   ✅ BigQuery connected!")
    print(f"   Rows:    "
          f"{bq_result.total_rows:,}")
    print(f"   Churned: "
          f"{bq_result.churned:,}")
    print(f"   Revenue: "
          f"${bq_result.total_revenue:,.2f}")
    logger.info(
        f"BQ: {bq_result.total_rows:,} "
        f"rows")
    ingestion_stats["sources"][
        "bigquery"] = {
        "rows": int(
            bq_result.total_rows),
        "status": "SUCCESS"
    }

except Exception as e:
    print(f"   ⚠️ BigQuery: {e}")
    logger.warning(f"BQ warning: {e}")
    ingestion_stats["sources"][
        "bigquery"] = {
        "status": "WARNING",
        "message": str(e)
    }

# ─────────────────────────────────────────────
# SOURCE 4: REST APIS
# ─────────────────────────────────────────────
print("\n📌 SOURCE 4: REST APIS")
logger.info("Source 4: REST APIs")

api_data = {}

# Weather for Accra (Ghana HQ)
try:
    response = requests.get(
        "https://api.open-meteo.com"
        "/v1/forecast",
        params={
            "latitude": 5.6037,
            "longitude": -0.1870,
            "current": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "wind_speed_10m"),
            "timezone": "Africa/Accra"
        },
        timeout=10)
    response.raise_for_status()
    weather = response.json()
    current = weather.get('current', {})
    api_data['weather'] = {
        'city': 'Accra',
        'temperature_c': current.get(
            'temperature_2m'),
        'humidity_pct': current.get(
            'relative_humidity_2m'),
        'wind_kmh': current.get(
            'wind_speed_10m')
    }
    print(f"   ✅ Weather API: "
          f"Accra {api_data['weather']['temperature_c']}°C")
    logger.info(
        f"Weather: "
        f"{api_data['weather']['temperature_c']}°C")

except Exception as e:
    print(f"   ⚠️ Weather API: {e}")
    logger.warning(f"Weather API: {e}")
    api_data['weather'] = {
        'city': 'Accra',
        'status': 'unavailable'}

# Ghana country data
try:
    response = requests.get(
        "https://restcountries.com"
        "/v3.1/alpha/GHA",
        timeout=10)
    response.raise_for_status()
    data = response.json()
    if data:
        c = data[0]
        api_data['ghana'] = {
            'name': c['name']['common'],
            'population': c.get(
                'population', 0),
            'region': c.get(
                'region', 'Africa'),
            'capital': c.get(
                'capital', ['Accra'])[0]
        }
        print(f"   ✅ Country API: "
              f"Ghana — "
              f"{api_data['ghana']['population']/1e6:.1f}M people")
        logger.info(
            "Ghana country data fetched")

except Exception as e:
    print(f"   ⚠️ Country API: {e}")
    logger.warning(f"Country API: {e}")
    api_data['ghana'] = {
        'name': 'Ghana',
        'status': 'unavailable'}

ingestion_stats["sources"]["apis"] = {
    "weather": api_data.get(
        'weather', {}).get(
        'temperature_c', 'N/A'),
    "country": api_data.get(
        'ghana', {}).get(
        'name', 'N/A'),
    "status": "SUCCESS"
}

# ─────────────────────────────────────────────
# SAVE INGESTION RESULTS
# ─────────────────────────────────────────────
print("\n📌 SAVING INGESTION RESULTS")
logger.info("Saving ingestion results")

# Save clean telco data locally
df_telco.to_csv(
    "../data/telco_ingested.csv",
    index=False)

# Save API data
with open(
        "../data/api_data.json",
        'w') as f:
    json.dump(api_data, f, indent=4)

# Save ingestion report
ingestion_report = {
    "module": "Data Ingestion",
    "pipeline": PIPELINE_NAME,
    "author": AUTHOR,
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "sources": ingestion_stats[
        "sources"],
    "errors": ingestion_stats["errors"],
    "telco_data": {
        "rows": len(df_telco),
        "columns": df_telco.shape[1],
        "churn_rate": round(
            df_telco['Churn'].mean()*100,
            2)
    }
}

with open(
        "../reports/ingestion_report.json",
        'w') as f:
    json.dump(
        ingestion_report, f, indent=4)

print("   ✅ telco_ingested.csv")
print("   ✅ api_data.json")
print("   ✅ ingestion_report.json")
logger.info("Ingestion outputs saved")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
print(f"\n{'='*55}")
print("   MODULE 1 INGESTION SUMMARY")
print(f"{'='*55}")
sources_ok = sum(
    1 for s in ingestion_stats[
        "sources"].values()
    if s.get("status") in [
        "SUCCESS", "WARNING"])
print(f"   Sources connected: "
      f"{sources_ok}/4")
for src, info in ingestion_stats[
        "sources"].items():
    status = info.get("status", "N/A")
    icon = "✅" if status == "SUCCESS" \
        else "⚠️"
    print(f"   {icon} {src:<15} {status}")

print(f"\n   Telco rows: "
      f"{len(df_telco):,}")
print(f"   Churn rate: "
      f"{df_telco['Churn'].mean()*100:.2f}%")
print(f"\n✅ Module 1 — Ingestion Complete!")
logger.info("Module 1 ingestion complete")