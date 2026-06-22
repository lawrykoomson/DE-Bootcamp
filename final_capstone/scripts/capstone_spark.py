# ============================================
# FINAL CAPSTONE — MODULE 3: SPARK PROCESSING
# Distributed processing of enriched data
# GetSkills Network DE Bootcamp
# Lawrence Koomson
# ============================================

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    round as spark_round, when, desc,
    rank)
from pyspark.sql.window import Window

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
            '../logs/spark_module.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName(
        "Capstone-Spark-Processing") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 55)
print("   FINAL CAPSTONE")
print("   MODULE 3 — SPARK PROCESSING")
print("=" * 55)

start_time = datetime.now()
logger.info(
    "Module 3: Spark processing started")

# ─────────────────────────────────────────────
# STEP 1: LOAD TRANSFORMED DATA INTO SPARK
# ─────────────────────────────────────────────
print("\n📌 1. LOADING DATA INTO SPARK")
logger.info("Loading transformed CSV")

df = spark.read.csv(
    "../data/telco_transformed.csv",
    header=True, inferSchema=True)

df = df.withColumn(
    "ChurnInt",
    col("Churn").cast("integer"))

row_count = df.count()
print(f"   ✅ Loaded {row_count:,} rows")
print(f"   Columns: {len(df.columns)}")
print(f"   Cores used: "
      f"{spark.sparkContext.defaultParallelism}")
logger.info(
    f"Loaded {row_count:,} rows into "
    f"Spark")

# ─────────────────────────────────────────────
# STEP 2: DISTRIBUTED RISK ANALYSIS
# ─────────────────────────────────────────────
print("\n📌 2. DISTRIBUTED RISK ANALYSIS")
logger.info("Running distributed analysis")

risk_summary = df.groupBy(
    "risk_label").agg(
    count("*").alias("customers"),
    spark_sum("ChurnInt").alias(
        "churned"),
    spark_round(
        spark_sum("ChurnInt") * 100.0 /
        count("*"), 1
    ).alias("churn_rate_pct"),
    spark_round(
        avg("MonthlyCharges"), 2
    ).alias("avg_monthly"),
    spark_round(
        spark_sum("lifetime_value"), 2
    ).alias("total_ltv")
).orderBy(desc("customers"))

print(f"\n   🔍 Risk distribution "
      f"(Spark-processed):")
risk_summary.show()
logger.info("Risk analysis complete")

# ─────────────────────────────────────────────
# STEP 3: WINDOW FUNCTIONS — TOP CUSTOMERS
# ─────────────────────────────────────────────
print("\n📌 3. TOP CUSTOMERS PER SEGMENT")
logger.info("Running window functions")

window_spec = Window.partitionBy(
    "Contract").orderBy(
    desc("lifetime_value"))

ranked = df.withColumn(
    "ltv_rank",
    rank().over(window_spec))

top_customers = ranked.filter(
    col("ltv_rank") <= 3
).select(
    "customerID", "Contract",
    "lifetime_value", "risk_label",
    "ltv_rank")

print(f"\n   🔍 Top 3 lifetime-value "
      f"customers per contract:")
top_customers.orderBy(
    "Contract", "ltv_rank").show(9)
logger.info("Window ranking complete")

# ─────────────────────────────────────────────
# STEP 4: SQL-BASED EXECUTIVE QUERY
# ─────────────────────────────────────────────
print("\n📌 4. SQL EXECUTIVE SUMMARY")
logger.info("Running SQL summary")

df.createOrReplaceTempView(
    "capstone_data")

exec_summary = spark.sql("""
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
            SUM(annual_value), 2)
            as annual_revenue,
        ROUND(
            SUM(CASE WHEN ChurnInt = 1
                THEN MonthlyCharges
                ELSE 0 END), 2)
            as revenue_at_risk
    FROM capstone_data
""").collect()[0]

print(f"\n   📊 Executive KPIs:")
print(f"   Total customers: "
      f"{exec_summary['total_customers']:,}")
print(f"   Churn rate:      "
      f"{exec_summary['churn_rate_pct']}%")
print(f"   Monthly revenue: "
      f"${exec_summary['monthly_revenue']:,.2f}")
print(f"   Annual revenue:  "
      f"${exec_summary['annual_revenue']:,.2f}")
print(f"   Revenue at risk: "
      f"${exec_summary['revenue_at_risk']:,.2f}")
logger.info(
    f"SQL summary: "
    f"{exec_summary['churn_rate_pct']}% "
    f"churn")

# ─────────────────────────────────────────────
# STEP 5: SAVE SPARK OUTPUTS
# ─────────────────────────────────────────────
print("\n📌 5. SAVING SPARK OUTPUTS")
logger.info("Saving Spark outputs")

risk_summary.coalesce(1).write.mode(
    "overwrite").csv(
    "../data/spark_risk_summary",
    header=True)

top_customers.coalesce(1).write.mode(
    "overwrite").csv(
    "../data/spark_top_customers",
    header=True)

duration = (
    datetime.now() - start_time
).total_seconds()

spark_report = {
    "module": "Spark Processing",
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "duration_seconds": round(
        duration, 2),
    "spark_version": spark.version,
    "cores_used": (
        spark.sparkContext
        .defaultParallelism),
    "rows_processed": row_count,
    "executive_summary": {
        "total_customers": int(
            exec_summary[
                'total_customers']),
        "churn_rate_pct": float(
            exec_summary[
                'churn_rate_pct']),
        "annual_revenue": float(
            exec_summary[
                'annual_revenue']),
        "revenue_at_risk": float(
            exec_summary[
                'revenue_at_risk'])
    }
}

with open(
        "../reports/spark_report.json",
        'w') as f:
    json.dump(
        spark_report, f, indent=4)

print("   ✅ spark_risk_summary/")
print("   ✅ spark_top_customers/")
print("   ✅ spark_report.json")
logger.info("Spark outputs saved")

print(f"\n{'='*55}")
print("   MODULE 3 SPARK SUMMARY")
print(f"{'='*55}")
print(f"   Duration:    {duration:.2f}s")
print(f"   Rows:        {row_count:,}")
print(f"   Cores:       "
      f"{spark.sparkContext.defaultParallelism}")
print(f"   Churn rate:  "
      f"{exec_summary['churn_rate_pct']}%")
print(f"   Revenue risk: "
      f"${exec_summary['revenue_at_risk']:,.2f}")
print(f"\n✅ Module 3 — Spark Processing Complete!")
logger.info("Module 3 Spark processing complete")

spark.stop()
print("\n   🛑 Spark session stopped")