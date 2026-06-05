# ============================================
# WEEK 4 — MODULE 1: SQL Basics
# SELECT, WHERE, ORDER BY, LIMIT
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import logging
import os
from pathlib import Path

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
            '../logs/module1.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Path to Week 2 clean data
CLEAN_DATA = Path(
    "../../week-2/data/processed/telco_clean.csv")
DB_PATH = "../data/telco.db"

print("=" * 55)
print("   MODULE 1 — SQL BASICS")
print("   SELECT | WHERE | ORDER BY | LIMIT")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: CREATE DATABASE FROM CSV
# ─────────────────────────────────────────────
print("\n📌 1. CREATING SQLITE DATABASE")

logger.info("Loading Telco clean data")
df = pd.read_csv(CLEAN_DATA)
logger.info(
    f"Loaded {df.shape[0]:,} rows x "
    f"{df.shape[1]} columns")

# Connect to SQLite database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Load DataFrame into SQLite table
df.to_sql(
    'customers',
    conn,
    if_exists='replace',
    index=False)

print(f"   ✅ Database created: {DB_PATH}")
print(f"   ✅ Table 'customers' loaded")
print(f"   Rows: {df.shape[0]:,}")
print(f"   Columns: {df.shape[1]}")
logger.info(
    f"SQLite DB created with "
    f"{df.shape[0]:,} rows")

# ─────────────────────────────────────────────
# HELPER FUNCTION
# ─────────────────────────────────────────────
def run_query(sql, description):
    """Run SQL and display results."""
    print(f"\n   🔍 {description}")
    print(f"   SQL: {sql.strip()}")
    result = pd.read_sql_query(sql, conn)
    print(f"   Rows returned: {len(result)}")
    print(result.to_string(index=False))
    logger.info(
        f"Query '{description}': "
        f"{len(result)} rows")
    return result

# ─────────────────────────────────────────────
# STEP 2: BASIC SELECT
# ─────────────────────────────────────────────
print("\n📌 2. BASIC SELECT STATEMENTS")

run_query("""
    SELECT customerID, tenure,
           MonthlyCharges, Contract, Churn
    FROM customers
    LIMIT 5
""", "First 5 customers")

run_query("""
    SELECT COUNT(*) as total_customers
    FROM customers
""", "Total customer count")

run_query("""
    SELECT DISTINCT Contract
    FROM customers
""", "Distinct contract types")

# ─────────────────────────────────────────────
# STEP 3: WHERE CLAUSE
# ─────────────────────────────────────────────
print("\n📌 3. WHERE CLAUSE — FILTERING")

run_query("""
    SELECT customerID, tenure,
           MonthlyCharges, Contract
    FROM customers
    WHERE Churn = 1
    LIMIT 5
""", "Churned customers (first 5)")

run_query("""
    SELECT COUNT(*) as churned_count
    FROM customers
    WHERE Churn = 1
""", "Total churned customers")

run_query("""
    SELECT COUNT(*) as high_value_churned
    FROM customers
    WHERE Churn = 1
    AND MonthlyCharges >= 90
""", "High value churned customers (≥$90)")

run_query("""
    SELECT COUNT(*) as mtm_churned
    FROM customers
    WHERE Churn = 1
    AND Contract = 'Month-to-month'
""", "Month-to-month churned customers")

run_query("""
    SELECT customerID, tenure,
           MonthlyCharges, Contract
    FROM customers
    WHERE tenure <= 12
    AND Churn = 1
    LIMIT 5
""", "New customers who churned (tenure ≤12)")

# ─────────────────────────────────────────────
# STEP 4: ORDER BY
# ─────────────────────────────────────────────
print("\n📌 4. ORDER BY — SORTING RESULTS")

run_query("""
    SELECT customerID, tenure,
           MonthlyCharges, TotalCharges
    FROM customers
    ORDER BY MonthlyCharges DESC
    LIMIT 5
""", "Top 5 highest paying customers")

run_query("""
    SELECT customerID, tenure,
           MonthlyCharges, Contract
    FROM customers
    WHERE Churn = 1
    ORDER BY tenure ASC
    LIMIT 5
""", "Churned customers with shortest tenure")

# ─────────────────────────────────────────────
# STEP 5: BETWEEN AND IN
# ─────────────────────────────────────────────
print("\n📌 5. BETWEEN AND IN")

run_query("""
    SELECT COUNT(*) as mid_tenure_count
    FROM customers
    WHERE tenure BETWEEN 13 AND 24
""", "Customers with tenure 13-24 months")

run_query("""
    SELECT COUNT(*) as long_contract_count
    FROM customers
    WHERE Contract IN ('One year', 'Two year')
""", "Customers on annual contracts")

run_query("""
    SELECT COUNT(*) as loyal_non_churned
    FROM customers
    WHERE tenure >= 48
    AND Churn = 0
""", "Loyal customers (48+ months) not churned")

# ─────────────────────────────────────────────
# STEP 6: CALCULATED COLUMNS
# ─────────────────────────────────────────────
print("\n📌 6. CALCULATED COLUMNS")

run_query("""
    SELECT customerID,
           tenure,
           MonthlyCharges,
           ROUND(MonthlyCharges * tenure, 2)
               as estimated_lifetime_value,
           Contract
    FROM customers
    ORDER BY estimated_lifetime_value DESC
    LIMIT 5
""", "Top 5 customers by lifetime value")

# ─────────────────────────────────────────────
# STEP 7: LIKE — PATTERN MATCHING
# ─────────────────────────────────────────────
print("\n📌 7. LIKE — PATTERN MATCHING")

run_query("""
    SELECT COUNT(*) as auto_pay_count
    FROM customers
    WHERE PaymentMethod LIKE '%automatic%'
""", "Customers on automatic payments")

run_query("""
    SELECT COUNT(*) as fiber_count
    FROM customers
    WHERE InternetService LIKE '%Fiber%'
""", "Fiber optic customers")

# Close connection
conn.close()
logger.info("Database connection closed")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   SELECT    → Choose columns")
print("   FROM      → Choose table")
print("   WHERE     → Filter rows")
print("   ORDER BY  → Sort results")
print("   LIMIT     → Restrict row count")
print("   BETWEEN   → Range filter")
print("   IN        → Multiple values")
print("   LIKE      → Pattern matching")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 SQL basics complete")