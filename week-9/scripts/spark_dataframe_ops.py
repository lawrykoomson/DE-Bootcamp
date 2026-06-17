# ============================================
# WEEK 9 — MODULE 2: Spark DataFrame Operations
# Real Telco Data — Joins, Window Functions
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    round as spark_round, when, desc,
    rank, dense_rank)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)

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

spark = SparkSession.builder \
    .appName("DE-Bootcamp-Week9-M2") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 55)
print("   MODULE 2 — SPARK DATAFRAME OPERATIONS")
print("   REAL DATA | JOINS | WINDOW FUNCTIONS")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD REAL TELCO DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING REAL TELCO DATA")
logger.info("Loading Telco CSV into Spark")

CLEAN_DATA = (
    "../../week-2/data/processed/"
    "telco_clean.csv")

df = spark.read.csv(
    CLEAN_DATA, header=True,
    inferSchema=True)

# Cast Churn to integer immediately since
# Spark infers it as boolean from this CSV
# (true/false), and SQL sum() needs numeric
df = df.withColumn(
    "ChurnInt",
    col("Churn").cast("integer"))

print(f"   ✅ Loaded into Spark!")
print(f"   Rows: {df.count():,}")
print(f"   Columns: {len(df.columns)}")
print(f"   Partitions: "
      f"{df.rdd.getNumPartitions()}")
logger.info(
    f"Loaded {df.count():,} rows")

df.printSchema()

# ─────────────────────────────────────────────
# STEP 2: BASIC EXPLORATION
# ─────────────────────────────────────────────
print("\n📌 2. BASIC EXPLORATION")

print("\n   🔍 First 5 rows:")
df.select(
    "customerID", "Contract",
    "MonthlyCharges", "Churn").show(5)

print("\n   🔍 Churn value counts:")
df.groupBy("Churn").count().show()

# ─────────────────────────────────────────────
# STEP 3: CREATE A SECOND DATAFRAME TO JOIN
# ─────────────────────────────────────────────
print("\n📌 3. CREATING LOOKUP TABLE FOR JOIN")
logger.info("Creating contract pricing lookup")

contract_pricing = spark.createDataFrame([
    ("Month-to-month", "Flexible", 0),
    ("One year", "Discounted", 10),
    ("Two year", "Best Value", 20),
], ["Contract", "PlanType",
    "DiscountPct"])

print("   ✅ Lookup table created:")
contract_pricing.show()

# ─────────────────────────────────────────────
# STEP 4: JOIN DATAFRAMES
# ─────────────────────────────────────────────
print("\n📌 4. JOINING DATAFRAMES")
print("   Joining customer data with")
print("   contract pricing lookup!")
logger.info("Joining DataFrames")

joined_df = df.join(
    contract_pricing,
    on="Contract",
    how="left")

print(f"   ✅ Joined! Rows: "
      f"{joined_df.count():,}")
joined_df.select(
    "customerID", "Contract",
    "PlanType", "DiscountPct",
    "MonthlyCharges").show(5)

# ─────────────────────────────────────────────
# STEP 5: WINDOW FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 5. WINDOW FUNCTIONS")
print("   Rank customers by spending")
print("   WITHIN each contract type!")
logger.info("Running window functions")

window_spec = Window.partitionBy(
    "Contract").orderBy(
    desc("MonthlyCharges"))

ranked_df = df.withColumn(
    "spend_rank",
    rank().over(window_spec)
).withColumn(
    "spend_dense_rank",
    dense_rank().over(window_spec)
)

print("\n   🔍 Top 3 spenders per contract:")
ranked_df.filter(
    col("spend_rank") <= 3
).select(
    "Contract", "customerID",
    "MonthlyCharges", "spend_rank"
).orderBy(
    "Contract", "spend_rank"
).show(15)

# ─────────────────────────────────────────────
# STEP 6: COMPLEX TRANSFORMATIONS
# ─────────────────────────────────────────────
print("\n📌 6. COMPLEX TRANSFORMATIONS")
print("   Using when/otherwise for")
print("   business logic — like CASE WHEN!")
logger.info("Running complex transformations")

risk_df = df.withColumn(
    "risk_score",
    when(col("Contract") ==
         "Month-to-month", 3)
    .when(col("Contract") ==
          "One year", 1)
    .otherwise(0) +
    when(col("tenure") <= 12, 3)
    .when(col("tenure") <= 24, 2)
    .when(col("tenure") <= 48, 1)
    .otherwise(0)
).withColumn(
    "risk_label",
    when(col("risk_score") >= 5,
         "Critical")
    .when(col("risk_score") >= 3,
          "High")
    .when(col("risk_score") >= 1,
          "Medium")
    .otherwise("Low")
)

print("\n   🔍 Risk distribution:")
risk_df.groupBy("risk_label").agg(
    count("*").alias("customers"),
    spark_round(
        avg("MonthlyCharges"), 2
    ).alias("avg_monthly")
).orderBy(
    desc("customers")).show()

# ─────────────────────────────────────────────
# STEP 7: MULTI-LEVEL AGGREGATIONS
# ─────────────────────────────────────────────
print("\n📌 7. MULTI-LEVEL AGGREGATIONS")
logger.info("Running multi-level aggregations")

print("\n   🔍 Churn analysis by "
      "Contract + InternetService:")
df.groupBy(
    "Contract", "InternetService"
).agg(
    count("*").alias("total"),
    spark_sum("ChurnInt").alias("churned"),
    spark_round(
        spark_sum("ChurnInt") * 100.0 /
        count("*"), 1
    ).alias("churn_rate_pct"),
    spark_round(
        spark_sum("MonthlyCharges"), 2
    ).alias("total_revenue")
).orderBy(
    desc("churn_rate_pct")
).show(10)

# ─────────────────────────────────────────────
# STEP 8: SAVE RESULTS
# ─────────────────────────────────────────────
print("\n📌 8. SAVING RESULTS")
logger.info("Saving results to disk")

output_path = (
    "../data/risk_analysis")
risk_df.select(
    "customerID", "Contract",
    "tenure", "MonthlyCharges",
    "risk_score", "risk_label",
    "Churn"
).write.mode("overwrite").csv(
    output_path, header=True)

print(f"   ✅ Saved to: {output_path}")
logger.info(
    f"Results saved to {output_path}")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   spark.read.csv() → Load real data")
print("   join()           → Combine DataFrames")
print("   Window functions → Rank within groups")
print("   when/otherwise   → CASE WHEN logic")
print("   Multi-level groupBy → Deep aggregation")
print("   cast()           → Convert data types")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 complete")

spark.stop()
print("\n   🛑 Spark session stopped")