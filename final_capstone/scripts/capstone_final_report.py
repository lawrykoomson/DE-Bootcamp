# ============================================
# FINAL CAPSTONE — MODULE 5: FINAL REPORT
# Master pipeline summary tying everything
# together
# GetSkills Network DE Bootcamp
# Lawrence Koomson
# ============================================

import os
import json
import logging
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../reports", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/final_report.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

PIPELINE_NAME = (
    "Telco Churn Intelligence Platform")
AUTHOR = "Lawrence Koomson"
start_time = datetime.now()

print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   MODULE 5 — FINAL REPORT")
print("=" * 55)

logger.info(
    "Module 5: Final report started")

# ─────────────────────────────────────────────
# STEP 1: LOAD ALL PREVIOUS REPORTS
# ─────────────────────────────────────────────
print("\n📌 1. LOADING ALL PIPELINE REPORTS")
logger.info("Loading all previous reports")

with open(
        "../reports/ingestion_report.json"
        ) as f:
    ingestion = json.load(f)

with open(
        "../reports/transform_report.json"
        ) as f:
    transform = json.load(f)

with open(
        "../reports/spark_report.json"
        ) as f:
    spark_report = json.load(f)

print(f"   ✅ Ingestion report loaded")
print(f"   ✅ Transform report loaded")
print(f"   ✅ Spark report loaded")
logger.info("All reports loaded")

# ─────────────────────────────────────────────
# STEP 2: BUILD MASTER SUMMARY
# ─────────────────────────────────────────────
print("\n📌 2. BUILDING MASTER SUMMARY")
logger.info("Building master summary")

duration = (
    datetime.now() - start_time
).total_seconds()

master_report = {
    "pipeline_name": PIPELINE_NAME,
    "author": AUTHOR,
    "bootcamp": (
        "GetSkills Network & "
        "Thrive Africa"),
    "generated_at": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "modules_completed": [
        "1. Data Ingestion "
        "(CSV + S3 + BigQuery + APIs)",
        "2. Transformation & Risk Scoring",
        "3. Spark Distributed Processing",
        "4. Visualizations",
        "5. Final Reporting"
    ],
    "technologies_used": {
        "languages": ["Python", "SQL"],
        "libraries": [
            "Pandas", "NumPy",
            "Matplotlib", "PySpark",
            "boto3", "requests"],
        "cloud_aws": ["S3"],
        "cloud_gcp": ["BigQuery"],
        "big_data": ["Apache Spark"],
        "apis": [
            "Open-Meteo",
            "REST Countries"]
    },
    "data_sources": (
        ingestion.get("sources", {})),
    "data_quality": transform.get(
        "data_quality"),
    "executive_summary": (
        spark_report.get(
            "executive_summary", {})),
    "risk_distribution": transform.get(
        "risk_distribution", {}),
    "intervention_strategy": transform.get(
        "intervention_summary", []),
    "top_risk_segment": transform.get(
        "top_risk_segment", {}),
    "performance": {
        "spark_duration_seconds":
            spark_report.get(
                "duration_seconds"),
        "spark_cores_used":
            spark_report.get(
                "cores_used"),
        "spark_version":
            spark_report.get(
                "spark_version")
    }
}

with open(
        "../reports/MASTER_REPORT.json",
        'w') as f:
    json.dump(
        master_report, f, indent=4)

print(f"   ✅ Master report built!")
logger.info("Master report saved")

# ─────────────────────────────────────────────
# STEP 3: PRINT EXECUTIVE BRIEFING
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   EXECUTIVE BRIEFING")
print("=" * 55)

exec_sum = master_report[
    "executive_summary"]

print(f"""
   Pipeline:  {PIPELINE_NAME}
   Author:    {AUTHOR}
   Bootcamp:  GetSkills Network &
              Thrive Africa

   ──────────────────────────────────
   BUSINESS METRICS
   ──────────────────────────────────
   Total customers:    {exec_sum.get('total_customers', 0):,}
   Churn rate:          {exec_sum.get('churn_rate_pct', 0)}%
   Annual revenue:      ${exec_sum.get('annual_revenue', 0):,.2f}
   Revenue at risk:     ${exec_sum.get('revenue_at_risk', 0):,.2f}

   ──────────────────────────────────
   RISK DISTRIBUTION
   ──────────────────────────────────""")

for label, count in master_report[
        "risk_distribution"].items():
    print(f"   {label:<12} {count:>6,} "
          f"customers")

print(f"""
   ──────────────────────────────────
   TECHNOLOGY STACK
   ──────────────────────────────────
   Languages:  Python, SQL
   Cloud AWS:  S3 (data lake)
   Cloud GCP:  BigQuery (warehouse)
   Big Data:   Apache Spark
                ({master_report['performance']['spark_cores_used']} cores,
                 {master_report['performance']['spark_duration_seconds']}s)
   APIs:       Open-Meteo, REST Countries

   ──────────────────────────────────
   PIPELINE MODULES
   ──────────────────────────────────""")

for module in master_report[
        "modules_completed"]:
    print(f"   ✅ {module}")

print(f"\n{'='*55}")
print("   🏆 FINAL CAPSTONE COMPLETE!")
print(f"{'='*55}")
print(f"\n   This pipeline demonstrates the")
print(f"   complete Data Engineering")
print(f"   lifecycle — from raw multi-")
print(f"   source ingestion through")
print(f"   distributed processing to")
print(f"   executive-ready insights.")
print(f"\n✅ GetSkills Network & Thrive Africa")
print(f"   Data Engineering Bootcamp")
print(f"   — COMPLETE —")
logger.info(
    "FINAL CAPSTONE COMPLETE!")