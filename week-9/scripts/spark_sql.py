# ============================================
# WEEK 9 — MODULE 3: Spark SQL
# Running SQL Queries on Spark DataFrames
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
            '../logs/module3.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("DE-Bootcamp-Week9-M3") \
    .master("local[*]") \
    .config(
        "spark.sql.shuffle.partitions",
        "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 55)
print("   MODULE 3 — SPARK SQL")
print("   SQL QUERIES ON DISTRIBUTED DATA")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD DATA AND REGISTER AS TABLE
# ─────────────────────────────────────────────
print("\n📌 1. REGISTERING DATAFRAME AS "
      "SQL TABLE")
logger.info("Loading data and creating view")

CLEAN_DATA = (
    "../../week-2/data/processed/"
    "telco_clean.csv")

df = spark.read.csv(
    CLEAN_DATA, header=True,
    inferSchema=True)

df = df.withColumn(
    "ChurnInt",
    col("Churn").cast("integer"))

# Register as a temporary SQL view
df.createOrReplaceTempView("customers")

print(f"   ✅ Loaded {df.count():,} rows")
print(f"   ✅ Registered as SQL view: "
      f"'customers'")
print(f"   Now you can run plain SQL "
      f"on this DataFrame!")
logger.info(
    "DataFrame registered as 'customers'")

# ─────────────────────────────────────────────
# STEP 2: BASIC SQL QUERIES
# ─────────────────────────────────────────────
print("\n📌 2. BASIC SQL QUERIES")
logger.info("Running basic SQL queries")

print("\n   🔍 SELECT COUNT(*):")
spark.sql("""
    SELECT COUNT(*) as total_customers
    FROM customers
""").show()

print("\n   🔍 Filter with WHERE:")
spark.sql("""
    SELECT customerID, Contract,
           MonthlyCharges
    FROM customers
    WHERE Contract = 'Month-to-month'
    LIMIT 5
""").show()

# ─────────────────────────────────────────────
# STEP 3: AGGREGATIONS IN SQL
# ─────────────────────────────────────────────
print("\n📌 3. AGGREGATIONS IN SQL")
logger.info("Running SQL aggregations")

print("\n   🔍 Churn rate by contract:")
spark.sql("""
    SELECT
        Contract,
        COUNT(*) as total,
        SUM(ChurnInt) as churned,
        ROUND(
            SUM(ChurnInt) * 100.0 /
            COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly
    FROM customers
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
""").show()

# ─────────────────────────────────────────────
# STEP 4: CTEs (Common Table Expressions)
# ─────────────────────────────────────────────
print("\n📌 4. CTEs — COMMON TABLE EXPRESSIONS")
print("   Build complex queries step "
      "by step using WITH!")
logger.info("Running CTE query")

print("\n   🔍 High-value at-risk customers:")
spark.sql("""
    WITH high_value AS (
        SELECT *
        FROM customers
        WHERE MonthlyCharges > 80
    ),
    at_risk AS (
        SELECT *
        FROM high_value
        WHERE Contract = 'Month-to-month'
        AND ChurnInt = 0
    )
    SELECT
        customerID, Contract,
        MonthlyCharges, tenure
    FROM at_risk
    ORDER BY MonthlyCharges DESC
    LIMIT 10
""").show()

# ─────────────────────────────────────────────
# STEP 5: WINDOW FUNCTIONS IN SQL
# ─────────────────────────────────────────────
print("\n📌 5. WINDOW FUNCTIONS IN SQL")
logger.info("Running SQL window functions")

print("\n   🔍 Rank customers by spend "
      "within contract type:")
spark.sql("""
    SELECT *
    FROM (
        SELECT
            Contract, customerID,
            MonthlyCharges,
            RANK() OVER (
                PARTITION BY Contract
                ORDER BY MonthlyCharges DESC
            ) as spend_rank
        FROM customers
    )
    WHERE spend_rank <= 3
    ORDER BY Contract, spend_rank
""").show()

# ─────────────────────────────────────────────
# STEP 6: SUBQUERIES
# ─────────────────────────────────────────────
print("\n📌 6. SUBQUERIES")
logger.info("Running subquery")

print("\n   🔍 Customers paying above "
      "average:")
spark.sql("""
    SELECT
        customerID, Contract,
        MonthlyCharges
    FROM customers
    WHERE MonthlyCharges > (
        SELECT AVG(MonthlyCharges)
        FROM customers
    )
    ORDER BY MonthlyCharges DESC
    LIMIT 5
""").show()

# ─────────────────────────────────────────────
# STEP 7: CREATE A PERMANENT-STYLE VIEW
# ─────────────────────────────────────────────
print("\n📌 7. CREATING REUSABLE SQL VIEWS")
logger.info("Creating SQL views")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW
    churn_summary AS
    SELECT
        Contract,
        InternetService,
        COUNT(*) as total,
        SUM(ChurnInt) as churned,
        ROUND(
            SUM(ChurnInt) * 100.0 /
            COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
            as total_revenue
    FROM customers
    GROUP BY Contract, InternetService
""")

print("   ✅ View 'churn_summary' created!")
print("\n   🔍 Query the view like a table:")
spark.sql("""
    SELECT *
    FROM churn_summary
    ORDER BY churn_rate_pct DESC
    LIMIT 5
""").show()

# ─────────────────────────────────────────────
# STEP 8: COMPARE PYTHON API vs SQL
# ─────────────────────────────────────────────
print("\n📌 8. PYTHON API vs SQL — SAME RESULT")
print("   Both produce IDENTICAL plans —")
print("   Spark optimises them the same way!")
logger.info(
    "Comparing Python API vs SQL")

print("\n   🔍 Using Python DataFrame API:")
df.filter(
    col("Contract") == "Two year"
).select(
    "customerID", "MonthlyCharges"
).orderBy(
    col("MonthlyCharges").desc()
).show(3)

print("\n   🔍 Using SQL — same result:")
spark.sql("""
    SELECT customerID, MonthlyCharges
    FROM customers
    WHERE Contract = 'Two year'
    ORDER BY MonthlyCharges DESC
    LIMIT 3
""").show()

# ─────────────────────────────────────────────
# STEP 9: SAVE SQL RESULTS
# ─────────────────────────────────────────────
print("\n📌 9. SAVING SQL QUERY RESULTS")
logger.info("Saving SQL results")

final_report = spark.sql("""
    SELECT
        Contract,
        InternetService,
        COUNT(*) as customers,
        SUM(ChurnInt) as churned,
        ROUND(
            SUM(ChurnInt) * 100.0 /
            COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
            as total_revenue,
        ROUND(
            SUM(CASE WHEN ChurnInt = 1
                THEN MonthlyCharges
                ELSE 0 END), 2)
            as revenue_at_risk
    FROM customers
    GROUP BY Contract, InternetService
    ORDER BY churn_rate_pct DESC
""")

output_path = "../data/spark_sql_report"
final_report.write.mode(
    "overwrite").csv(
    output_path, header=True)

print(f"   ✅ Saved to: {output_path}")
logger.info(
    f"SQL report saved to {output_path}")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   createOrReplaceTempView() → SQL access")
print("   spark.sql()      → Run any SQL query")
print("   CTEs (WITH)      → Step-by-step queries")
print("   Window functions → RANK in subquery")
print("   Subqueries       → Nested SELECT")
print("   CREATE VIEW      → Reusable SQL views")
print("   Python API = SQL → Same execution plan!")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 complete")

spark.stop()
print("\n   🛑 Spark session stopped")