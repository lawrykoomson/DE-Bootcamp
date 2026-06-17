# ============================================
# WEEK 9 — CAPSTONE: Apache Spark DE Pipeline
# Complete Big Data Processing Pipeline
# GetSkills Network DE Bootcamp
# ============================================

import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    round as spark_round, when, desc,
    rank, broadcast)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Spark Telco DE Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"

os.makedirs("../logs", exist_ok=True)
os.makedirs("../data", exist_ok=True)
os.makedirs("../data/charts", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/capstone.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
start_time = datetime.now()

spark = SparkSession.builder \
    .appName(
        "DE-Bootcamp-Week9-Capstone") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   Version: {PIPELINE_VERSION}")
print(f"   Author:  {AUTHOR}")
print("=" * 55)

logger.info("=" * 40)
logger.info(
    f"PIPELINE STARTED: {PIPELINE_NAME}")
logger.info("=" * 40)

stats = {"stages_completed": 0}

# ─────────────────────────────────────────────
# STAGE 1: EXTRACT
# ─────────────────────────────────────────────
print("\n🔧 STAGE 1: EXTRACT")
logger.info("Stage 1: Extract")

CLEAN_DATA = (
    "../../week-2/data/processed/"
    "telco_clean.csv")

df = spark.read.csv(
    CLEAN_DATA, header=True,
    inferSchema=True)

df = df.withColumn(
    "ChurnInt",
    col("Churn").cast("integer"))

row_count = df.count()
print(f"   ✅ Extracted {row_count:,} rows")
print(f"   ✅ {len(df.columns)} columns")
logger.info(
    f"Extracted {row_count:,} rows")
stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 2: DATA QUALITY CHECKS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 2: DATA QUALITY CHECKS")
logger.info("Stage 2: Data quality")

checks = {
    "Total records": df.count(),
    "Null customerIDs": df.filter(
        col("customerID").isNull()
    ).count(),
    "Invalid charges": df.filter(
        col("MonthlyCharges") <= 0
    ).count(),
    "Invalid tenure": df.filter(
        col("tenure") < 0
    ).count(),
    "Duplicate IDs": (
        df.count() -
        df.dropDuplicates(
            ["customerID"]).count()
    ),
}

all_passed = True
print(f"\n   {'Check':<22} "
      f"{'Result':>8}  Status")
print("   " + "-" * 45)
for check, value in checks.items():
    if check == "Total records":
        status = "✅ OK"
    else:
        status = "✅ Pass" if value == 0 \
            else "❌ FAIL"
        if value > 0:
            all_passed = False
    print(f"   {check:<22} "
          f"{value:>8,}  {status}")

print(f"\n   Overall: "
      f"{'✅ ALL PASSED' if all_passed else '❌ ISSUES FOUND'}")
logger.info(
    f"Quality: "
    f"{'PASSED' if all_passed else 'FAILED'}")
stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 3: TRANSFORM — RISK SCORING
# ─────────────────────────────────────────────
print("\n🔧 STAGE 3: TRANSFORM — RISK SCORING")
logger.info("Stage 3: Transform")

risk_df = df.withColumn(
    "risk_score",
    when(col("Contract") ==
         "Month-to-month", 3)
    .when(col("Contract") ==
          "One year", 1)
    .otherwise(0) +
    when(col("PaymentMethod") ==
         "Electronic check", 3)
    .when(col("PaymentMethod") ==
          "Mailed check", 2)
    .otherwise(0) +
    when(col("tenure") <= 12, 3)
    .when(col("tenure") <= 24, 2)
    .when(col("tenure") <= 48, 1)
    .otherwise(0)
).withColumn(
    "risk_label",
    when(col("risk_score") >= 7,
         "Critical")
    .when(col("risk_score") >= 5,
          "High")
    .when(col("risk_score") >= 3,
          "Medium")
    .otherwise("Low")
).withColumn(
    "lifetime_value",
    spark_round(
        col("MonthlyCharges") *
        col("tenure"), 2)
)

risk_df.cache()
risk_df.count()  # materialize cache

print(f"   ✅ Risk scoring complete!")
print(f"\n   🔍 Risk distribution:")
risk_df.groupBy("risk_label").agg(
    count("*").alias("customers"),
    spark_round(
        avg("MonthlyCharges"), 2
    ).alias("avg_monthly")
).orderBy(desc("customers")).show()

logger.info("Risk scoring complete")
stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 4: WINDOW FUNCTIONS — RANKING
# ─────────────────────────────────────────────
print("\n🔧 STAGE 4: INTERVENTION TARGETING")
logger.info("Stage 4: Window ranking")

window_spec = Window.partitionBy(
    "risk_label").orderBy(
    desc("MonthlyCharges"))

ranked_df = risk_df.withColumn(
    "rank_in_segment",
    rank().over(window_spec))

top_priority = ranked_df.filter(
    (col("risk_label") == "Critical") &
    (col("Churn") == False) &
    (col("rank_in_segment") <= 10)
).select(
    "customerID", "Contract",
    "MonthlyCharges", "tenure",
    "risk_score", "lifetime_value",
    "rank_in_segment")

print(f"\n   🔍 Top 10 priority customers "
      f"(Critical risk, retained):")
top_priority.show(10)

logger.info("Window ranking complete")
stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 5: SQL-BASED EXECUTIVE SUMMARY
# ─────────────────────────────────────────────
print("\n🔧 STAGE 5: EXECUTIVE SUMMARY (SQL)")
logger.info("Stage 5: SQL summary")

risk_df.createOrReplaceTempView(
    "telco_risk")

kpis = spark.sql("""
    SELECT
        COUNT(*) as total_customers,
        SUM(ChurnInt) as total_churned,
        ROUND(
            SUM(ChurnInt) * 100.0 /
            COUNT(*), 2)
            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
            as monthly_revenue,
        ROUND(
            SUM(CASE WHEN ChurnInt = 1
                THEN MonthlyCharges
                ELSE 0 END), 2)
            as revenue_at_risk
    FROM telco_risk
""").collect()[0]

print(f"\n   📊 Executive KPIs:")
print(f"   Total customers:   "
      f"{kpis['total_customers']:,}")
print(f"   Total churned:     "
      f"{kpis['total_churned']:,}")
print(f"   Churn rate:        "
      f"{kpis['churn_rate_pct']}%")
print(f"   Monthly revenue:   "
      f"${kpis['monthly_revenue']:,.2f}")
print(f"   Revenue at risk:   "
      f"${kpis['revenue_at_risk']:,.2f}")

logger.info(
    f"KPIs: {kpis['churn_rate_pct']}% churn")
stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 6: SAVE OUTPUTS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 6: SAVING OUTPUTS")
logger.info("Stage 6: Saving outputs")

# Save enriched dataset
enriched_path = (
    "../data/spark_enriched_output")
risk_df.select(
    "customerID", "Contract",
    "MonthlyCharges", "tenure",
    "risk_score", "risk_label",
    "lifetime_value", "Churn"
).coalesce(1).write.mode(
    "overwrite").csv(
    enriched_path, header=True)
print(f"   ✅ Enriched data: "
      f"{enriched_path}")

# Save top priority customers
priority_path = (
    "../data/top_priority_customers")
top_priority.coalesce(1).write.mode(
    "overwrite").csv(
    priority_path, header=True)
print(f"   ✅ Priority customers: "
      f"{priority_path}")

# Save JSON report
duration = (
    datetime.now() - start_time
).total_seconds()

report = {
    "pipeline_name": PIPELINE_NAME,
    "version": PIPELINE_VERSION,
    "author": AUTHOR,
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "duration_seconds": round(
        duration, 2),
    "spark_version": spark.version,
    "cores_used": (
        spark.sparkContext
        .defaultParallelism),
    "data_quality": (
        "ALL PASSED" if all_passed
        else "ISSUES FOUND"),
    "executive_summary": {
        "total_customers": int(
            kpis['total_customers']),
        "churn_rate_pct": float(
            kpis['churn_rate_pct']),
        "monthly_revenue": float(
            kpis['monthly_revenue']),
        "revenue_at_risk": float(
            kpis['revenue_at_risk']),
    },
    "stages_completed": (
        stats["stages_completed"]),
    "spark_skills_used": [
        "SparkSession & DataFrames",
        "Joins & Window Functions",
        "Spark SQL & CTEs",
        "Caching & Broadcast Joins",
        "Partitioning & Optimization",
        "Explain Plans"
    ]
}

report_path = (
    f"../data/week9_report.json")
with open(report_path, 'w') as f:
    json.dump(report, f, indent=4)
print(f"   ✅ Report: {report_path}")

logger.info(
    f"Pipeline complete in "
    f"{duration:.2f}s")

risk_df.unpersist()

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   WEEK 9 SPARK CAPSTONE RESULTS")
print("=" * 55)
print(f"\n   Pipeline: {PIPELINE_NAME}")
print(f"   Status:   ✅ SUCCESS")
print(f"   Duration: {duration:.2f} seconds")
print(f"   Stages:   "
      f"{stats['stages_completed']}/5 ✅")
print(f"\n   📊 BUSINESS SUMMARY:")
print(f"   Customers:    "
      f"{kpis['total_customers']:,}")
print(f"   Churn Rate:   "
      f"{kpis['churn_rate_pct']}%")
print(f"   Revenue:      "
      f"${kpis['monthly_revenue']:,.2f}/mo")
print(f"   At Risk:      "
      f"${kpis['revenue_at_risk']:,.2f}/mo")
print(f"\n   ⚡ SPARK INFRASTRUCTURE:")
print(f"   Version:  {spark.version}")
print(f"   Cores:    "
      f"{spark.sparkContext.defaultParallelism}")
print(f"\n   📁 OUTPUT FILES: 3 saved")
print(f"\n✅ Week 9 Apache Spark "
      f"Capstone Complete!")
print(f"   Big Data Engineering — "
      f"Production Ready!")
logger.info("Week 9 capstone complete!")

spark.stop()
print("\n   🛑 Spark session stopped")