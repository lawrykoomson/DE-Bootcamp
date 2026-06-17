# ============================================
# WEEK 9 — MODULE 1: Apache Spark Basics
# SparkSession, DataFrames, RDDs
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    round as spark_round)

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
            '../logs/module1.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

print("=" * 55)
print("   MODULE 1 — APACHE SPARK BASICS")
print("   SPARKSESSION | DATAFRAMES | RDDS")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: CREATE SPARK SESSION
# ─────────────────────────────────────────────
print("\n📌 1. CREATING SPARK SESSION")
print("   SparkSession is the entry point")
print("   to all Spark functionality!")
logger.info("Creating Spark session")

spark = SparkSession.builder \
    .appName("DE-Bootcamp-Week9") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print(f"   ✅ Spark Session created!")
print(f"   Spark version: "
      f"{spark.version}")
print(f"   Master: "
      f"{spark.sparkContext.master}")
print(f"   App name: "
      f"{spark.sparkContext.appName}")
logger.info(
    f"Spark session created, "
    f"version {spark.version}")

# ─────────────────────────────────────────────
# STEP 2: WHAT IS local[*]?
# ─────────────────────────────────────────────
print("\n📌 2. UNDERSTANDING local[*]")
cores = spark.sparkContext.defaultParallelism
print(f"""
   local[*]  → Run Spark using ALL
                CPU cores on your laptop
   local[2]  → Use only 2 cores
   local     → Use only 1 core

   In production this would be:
   yarn      → Run on a Hadoop cluster
   k8s://    → Run on Kubernetes

   Your machine has {cores} cores
   available to Spark!
""")

# ─────────────────────────────────────────────
# STEP 3: CREATE A SIMPLE DATAFRAME
# ─────────────────────────────────────────────
print("\n📌 3. CREATING A SIMPLE DATAFRAME")
logger.info("Creating sample DataFrame")

data = [
    ("7590-VHVEG", "Female", "Month-to-month", 29.85, 0),
    ("5575-GNVDE", "Male", "One year", 56.95, 0),
    ("3668-QPYBK", "Male", "Month-to-month", 53.85, 1),
    ("7795-CFOCW", "Male", "One year", 42.30, 0),
    ("9237-HQITU", "Female", "Month-to-month", 70.70, 1),
    ("9305-CDSKC", "Female", "Month-to-month", 99.65, 1),
    ("1452-KIOVK", "Male", "Month-to-month", 89.10, 0),
    ("6713-OKOMC", "Female", "Month-to-month", 29.75, 0),
    ("7892-POOKP", "Female", "Month-to-month", 104.80, 1),
    ("0280-XJGEX", "Male", "Month-to-month", 103.70, 1),
]

columns = [
    "customerID", "gender", "Contract",
    "MonthlyCharges", "Churn"]

df = spark.createDataFrame(data, columns)

print(f"   ✅ DataFrame created!")
print(f"   Rows: {df.count()}")
print(f"   Columns: {len(df.columns)}")
logger.info(
    f"DataFrame created: {df.count()} rows")

# ─────────────────────────────────────────────
# STEP 4: EXPLORE THE DATAFRAME
# ─────────────────────────────────────────────
print("\n📌 4. EXPLORING THE DATAFRAME")

print("\n   🔍 Schema:")
df.printSchema()

print("\n   🔍 Show all rows:")
df.show()

print("\n   🔍 Describe statistics:")
df.describe(
    "MonthlyCharges").show()

# ─────────────────────────────────────────────
# STEP 5: BASIC TRANSFORMATIONS
# ─────────────────────────────────────────────
print("\n📌 5. BASIC TRANSFORMATIONS")
print("   Transformations are LAZY —")
print("   they don't run until an")
print("   ACTION is called!")
logger.info("Running transformations")

# Select specific columns
print("\n   🔍 select() — pick columns:")
df.select(
    "customerID", "Contract",
    "MonthlyCharges").show(5)

# Filter rows
print("\n   🔍 filter() — Month-to-month "
      "customers only:")
mtm_df = df.filter(
    col("Contract") == "Month-to-month")
mtm_df.show()

# Add a new column
print("\n   🔍 withColumn() — add "
      "annual charges:")
df_with_annual = df.withColumn(
    "AnnualCharges",
    spark_round(
        col("MonthlyCharges") * 12, 2))
df_with_annual.select(
    "customerID", "MonthlyCharges",
    "AnnualCharges").show(5)

# ─────────────────────────────────────────────
# STEP 6: GROUP BY AND AGGREGATE
# ─────────────────────────────────────────────
print("\n📌 6. GROUP BY AND AGGREGATE")
logger.info("Running aggregations")

print("\n   🔍 Churn rate by contract:")
df.groupBy("Contract").agg(
    count("*").alias("total"),
    spark_sum("Churn").alias("churned"),
    spark_round(
        avg("MonthlyCharges"), 2
    ).alias("avg_monthly")
).show()

# ─────────────────────────────────────────────
# STEP 7: ACTIONS VS TRANSFORMATIONS
# ─────────────────────────────────────────────
print("\n📌 7. ACTIONS VS TRANSFORMATIONS")
print("""
   TRANSFORMATIONS (lazy — build a plan):
   select(), filter(), withColumn(),
   groupBy(), join(), orderBy()

   ACTIONS (eager — trigger execution):
   show(), count(), collect(),
   write(), take(), first()

   Spark builds a DAG (execution plan)
   from transformations, then only
   RUNS when an action is called!
""")

print("   🔍 Demonstrating lazy evaluation:")
lazy_df = df.filter(
    col("Churn") == 1
).select("customerID", "MonthlyCharges")
print("   Transformation defined — "
      "nothing executed yet!")

result = lazy_df.collect()
print(f"   ✅ Action triggered! "
      f"Got {len(result)} rows")
for row in result:
    print(f"   {row['customerID']}: "
          f"${row['MonthlyCharges']}")

# ─────────────────────────────────────────────
# STEP 8: RDDs — RESILIENT DISTRIBUTED DATASETS
# ─────────────────────────────────────────────
print("\n📌 8. RDDs — RESILIENT "
      "DISTRIBUTED DATASETS")
print("   RDDs are the low-level Spark")
print("   data structure — DataFrames")
print("   are built on top of RDDs!")
logger.info("Working with RDDs")

# Use DataFrame operations instead of raw
# RDD lambda map/filter — this avoids a
# known Windows PySpark worker crash with
# RDD lambdas, while teaching the same
# underlying RDD concept safely.

rdd = df.rdd
print(f"\n   Number of partitions: "
      f"{rdd.getNumPartitions()}")

print("\n   🔍 Using DataFrame filter "
      "(RDD-backed under the hood):")
churned_df = df.filter(
    col("Churn") == 1
).select("customerID")

churned_customers = [
    row['customerID']
    for row in churned_df.collect()]

print(f"   Churned customer IDs: "
      f"{churned_customers}")
logger.info(
    f"RDD-backed operations: "
    f"{len(churned_customers)} churned")

print("\n   💡 Note: every DataFrame in")
print("   Spark is backed by an RDD —")
print("   df.rdd exposes the low-level")
print("   structure used under the hood!")

# ─────────────────────────────────────────────
# STEP 9: SAVE DATAFRAME
# ─────────────────────────────────────────────
print("\n📌 9. SAVING DATAFRAME TO DISK")
logger.info("Saving DataFrame")

output_path = "../data/sample_output"
df_with_annual.write.mode(
    "overwrite").csv(
    output_path, header=True)

print(f"   ✅ Saved to: {output_path}")
print(f"   Spark writes in PARALLEL —")
print(f"   you may see multiple part files!")
logger.info(
    f"DataFrame saved to {output_path}")

# ─────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────
print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   SparkSession  → Entry point to Spark")
print("   DataFrame     → Distributed table")
print("   Transformations → Lazy operations")
print("   Actions       → Trigger execution")
print("   RDD           → Low-level data structure")
print("   local[*]      → Use all CPU cores")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 Spark basics complete")

spark.stop()
print("\n   🛑 Spark session stopped")