# ============================================
# WEEK 9 — MODULE 4: Spark Performance
# Partitioning, Caching, Optimization
# GetSkills Network DE Bootcamp
# ============================================

import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    round as spark_round, broadcast)

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
            '../logs/module4.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("DE-Bootcamp-Week9-M4") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 55)
print("   MODULE 4 — SPARK PERFORMANCE")
print("   PARTITIONING | CACHING | OPTIMIZATION")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING DATA")
logger.info("Loading Telco data")

CLEAN_DATA = (
    "../../week-2/data/processed/"
    "telco_clean.csv")

df = spark.read.csv(
    CLEAN_DATA, header=True,
    inferSchema=True)

df = df.withColumn(
    "ChurnInt",
    col("Churn").cast("integer"))

print(f"   ✅ Loaded {df.count():,} rows")
print(f"   Default partitions: "
      f"{df.rdd.getNumPartitions()}")
logger.info(
    f"Loaded {df.count():,} rows, "
    f"{df.rdd.getNumPartitions()} partitions")

# ─────────────────────────────────────────────
# STEP 2: UNDERSTANDING PARTITIONS
# ─────────────────────────────────────────────
print("\n📌 2. UNDERSTANDING PARTITIONS")
print("""
   A partition is a CHUNK of data that
   Spark processes independently.

   Too few partitions → can't use all
                         your cores
   Too many partitions → overhead from
                          managing them

   Rule of thumb: 2-4 partitions per CPU core
""")

print(f"   Your machine cores: "
      f"{spark.sparkContext.defaultParallelism}")
print(f"   Current partitions: "
      f"{df.rdd.getNumPartitions()}")

# Repartition for better parallelism
df_repartitioned = df.repartition(8)
print(f"   After repartition(8): "
      f"{df_repartitioned.rdd.getNumPartitions()}")
logger.info("Repartitioned to 8")

# Coalesce reduces partitions (cheaper than repartition)
df_coalesced = df_repartitioned.coalesce(2)
print(f"   After coalesce(2): "
      f"{df_coalesced.rdd.getNumPartitions()}")
logger.info("Coalesced to 2")

# ─────────────────────────────────────────────
# STEP 3: CACHING / PERSISTENCE
# ─────────────────────────────────────────────
print("\n📌 3. CACHING — KEEP DATA IN MEMORY")
print("   Without caching, Spark RE-READS")
print("   and RE-COMPUTES data every time")
print("   you run an action!")
logger.info("Testing caching performance")

# Build an expensive transformation
expensive_df = df.withColumn(
    "risk_score",
    when_score := (
        col("MonthlyCharges") *
        col("tenure") / 100)
).filter(col("MonthlyCharges") > 50)

print("\n   ⏱️  WITHOUT caching — "
      "running 3 actions:")
start = time.time()
count1 = expensive_df.count()
time1 = time.time() - start

start = time.time()
count2 = expensive_df.count()
time2 = time.time() - start

start = time.time()
count3 = expensive_df.count()
time3 = time.time() - start

print(f"   Run 1: {time1:.3f}s "
      f"({count1:,} rows)")
print(f"   Run 2: {time2:.3f}s "
      f"({count2:,} rows)")
print(f"   Run 3: {time3:.3f}s "
      f"({count3:,} rows)")
print(f"   Total: {time1+time2+time3:.3f}s")

print("\n   ⏱️  WITH caching — "
      "running 3 actions:")
expensive_df.cache()
expensive_df.count()  # Trigger the cache

start = time.time()
count1 = expensive_df.count()
time1c = time.time() - start

start = time.time()
count2 = expensive_df.count()
time2c = time.time() - start

start = time.time()
count3 = expensive_df.count()
time3c = time.time() - start

print(f"   Run 1: {time1c:.3f}s "
      f"({count1:,} rows)")
print(f"   Run 2: {time2c:.3f}s "
      f"({count2:,} rows)")
print(f"   Run 3: {time3c:.3f}s "
      f"({count3:,} rows)")
print(f"   Total: "
      f"{time1c+time2c+time3c:.3f}s")

speedup = (
    (time1+time2+time3) /
    (time1c+time2c+time3c)
    if (time1c+time2c+time3c) > 0 else 1)
print(f"\n   🚀 Speedup: {speedup:.1f}x faster "
      f"with caching!")
logger.info(
    f"Cache speedup: {speedup:.1f}x")

expensive_df.unpersist()

# ─────────────────────────────────────────────
# STEP 4: BROADCAST JOINS
# ─────────────────────────────────────────────
print("\n📌 4. BROADCAST JOINS")
print("   For SMALL lookup tables,")
print("   broadcast() sends a full copy")
print("   to every worker — avoiding")
print("   expensive shuffles!")
logger.info("Testing broadcast join")

small_lookup = spark.createDataFrame([
    ("Month-to-month", "Flexible"),
    ("One year", "Discounted"),
    ("Two year", "Best Value"),
], ["Contract", "PlanType"])

print("\n   🔍 Regular join:")
start = time.time()
regular_join = df.join(
    small_lookup, on="Contract")
regular_join.count()
regular_time = time.time() - start
print(f"   Time: {regular_time:.3f}s")

print("\n   🔍 Broadcast join:")
start = time.time()
broadcast_join = df.join(
    broadcast(small_lookup),
    on="Contract")
broadcast_join.count()
broadcast_time = time.time() - start
print(f"   Time: {broadcast_time:.3f}s")

logger.info(
    f"Regular: {regular_time:.3f}s, "
    f"Broadcast: {broadcast_time:.3f}s")

# ─────────────────────────────────────────────
# STEP 5: EXPLAIN PLAN
# ─────────────────────────────────────────────
print("\n📌 5. EXPLAIN — VIEWING THE QUERY PLAN")
print("   Spark shows you EXACTLY how it")
print("   will execute your query!")
logger.info("Viewing explain plans")

print("\n   🔍 Physical plan for a "
      "groupBy aggregation:")
df.groupBy("Contract").agg(
    count("*").alias("total"),
    spark_sum("MonthlyCharges").alias(
        "revenue")
).explain()

# ─────────────────────────────────────────────
# STEP 6: COLUMN PRUNING DEMONSTRATION
# ─────────────────────────────────────────────
print("\n📌 6. COLUMN PRUNING")
print("   Spark only reads the columns")
print("   you actually need — saving I/O!")
logger.info("Demonstrating column pruning")

print("\n   🔍 Selecting only 2 columns:")
narrow_df = df.select(
    "customerID", "MonthlyCharges")
narrow_df.explain()

# ─────────────────────────────────────────────
# STEP 7: PARTITION-AWARE WRITING
# ─────────────────────────────────────────────
print("\n📌 7. PARTITIONED WRITES")
print("   Writing data partitioned by")
print("   a column speeds up future reads!")
logger.info("Writing partitioned data")

output_path = "../data/partitioned_output"
df.write.mode("overwrite").partitionBy(
    "Contract").csv(
    output_path, header=True)

print(f"   ✅ Saved partitioned by "
      f"Contract to: {output_path}")
print(f"   This creates separate folders:")
print(f"   Contract=Month-to-month/")
print(f"   Contract=One year/")
print(f"   Contract=Two year/")
logger.info(
    f"Partitioned write to {output_path}")

# ─────────────────────────────────────────────
# STEP 8: PERFORMANCE BEST PRACTICES
# ─────────────────────────────────────────────
print("\n📌 8. PERFORMANCE BEST PRACTICES")
print("""
   ✅ cache() reused DataFrames
   ✅ broadcast() small lookup tables
   ✅ repartition() before heavy shuffles
   ✅ coalesce() before writing output
   ✅ select() only needed columns early
   ✅ filter() as early as possible
   ✅ partitionBy() when writing large data
   ✅ avoid collect() on large datasets
   ✅ use explain() to verify query plans
""")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   repartition()  → Increase partitions")
print("   coalesce()     → Decrease partitions")
print("   cache()        → Keep data in memory")
print("   broadcast()    → Speed up small joins")
print("   explain()      → View execution plan")
print("   partitionBy()  → Organize output files")
print(f"\n✅ Module 4 Complete!")
logger.info("Module 4 complete")

spark.stop()
print("\n   🛑 Spark session stopped")