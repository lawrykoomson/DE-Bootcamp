# ============================================
# WEEK 5 — MODULE 1: Advanced SQL Patterns
# UNION, INTERSECT, EXCEPT, PIVOT, UNPIVOT
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

# Copy Week 4 database
import shutil
W4_DB = Path(
    "../../week-4/data/telco.db")
W5_DB = Path("../data/telco_w5.db")

if not W5_DB.exists():
    shutil.copy(W4_DB, W5_DB)
    logger.info(f"Copied DB to {W5_DB}")

conn = sqlite3.connect(str(W5_DB))

print("=" * 55)
print("   MODULE 1 — ADVANCED SQL PATTERNS")
print("   UNION | INTERSECT | EXCEPT | CASE")
print("=" * 55)

def run_query(sql, description):
    """Run SQL and display results."""
    print(f"\n   🔍 {description}")
    result = pd.read_sql_query(sql, conn)
    print(result.to_string(index=False))
    logger.info(
        f"'{description}': {len(result)} rows")
    return result

# ─────────────────────────────────────────────
# STEP 1: UNION — Combine result sets
# ─────────────────────────────────────────────
print("\n📌 1. UNION — COMBINING RESULT SETS")
print("   UNION removes duplicates")
print("   UNION ALL keeps duplicates")

run_query("""
    SELECT
        'High Risk Churned' as segment,
        customerID,
        Contract,
        MonthlyCharges,
        tenure
    FROM customers
    WHERE Churn = 1
    AND MonthlyCharges >= 90
    AND tenure <= 12

    UNION ALL

    SELECT
        'Low Risk Not Churned' as segment,
        customerID,
        Contract,
        MonthlyCharges,
        tenure
    FROM customers
    WHERE Churn = 0
    AND Contract = 'Two year'
    AND tenure >= 60

    ORDER BY segment, MonthlyCharges DESC
    LIMIT 10
""", "UNION ALL — two customer segments")

run_query("""
    SELECT COUNT(*) as high_risk_churned
    FROM (
        SELECT customerID
        FROM customers
        WHERE Churn = 1
        AND MonthlyCharges >= 90

        UNION ALL

        SELECT customerID
        FROM customers
        WHERE Churn = 1
        AND tenure <= 6
    ) combined
""", "UNION ALL count — high value + new churned")

# ─────────────────────────────────────────────
# STEP 2: CASE WHEN — Advanced conditional
# ─────────────────────────────────────────────
print("\n📌 2. ADVANCED CASE WHEN")

run_query("""
    SELECT
        CASE
            WHEN tenure <= 6   THEN '1. New (0-6m)'
            WHEN tenure <= 12  THEN '2. Early (7-12m)'
            WHEN tenure <= 24  THEN '3. Growing (13-24m)'
            WHEN tenure <= 48  THEN '4. Established (25-48m)'
            ELSE                    '5. Loyal (49m+)'
        END                         as lifecycle_stage,
        COUNT(*)                    as customers,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2) as total_revenue
    FROM customers
    GROUP BY lifecycle_stage
    ORDER BY lifecycle_stage
""", "Customer lifecycle analysis with CASE WHEN")

# ─────────────────────────────────────────────
# STEP 3: PIVOT — Reshape data
# ─────────────────────────────────────────────
print("\n📌 3. PIVOT — RESHAPING DATA")
print("   Turn rows into columns!")

run_query("""
    SELECT
        InternetService,
        SUM(CASE WHEN Contract = 'Month-to-month'
            THEN 1 ELSE 0 END)  as month_to_month,
        SUM(CASE WHEN Contract = 'One year'
            THEN 1 ELSE 0 END)  as one_year,
        SUM(CASE WHEN Contract = 'Two year'
            THEN 1 ELSE 0 END)  as two_year,
        COUNT(*)                as total
    FROM customers
    GROUP BY InternetService
    ORDER BY total DESC
""", "PIVOT — customers by internet x contract")

run_query("""
    SELECT
        InternetService,
        ROUND(
            SUM(CASE WHEN Contract = 'Month-to-month'
                AND Churn = 1
                THEN 1.0 ELSE 0 END) /
            SUM(CASE WHEN Contract = 'Month-to-month'
                THEN 1.0 ELSE 0 END) * 100,
            1)                  as mtm_churn_pct,
        ROUND(
            SUM(CASE WHEN Contract = 'One year'
                AND Churn = 1
                THEN 1.0 ELSE 0 END) /
            NULLIF(SUM(CASE
                WHEN Contract = 'One year'
                THEN 1.0 ELSE 0 END), 0) * 100,
            1)                  as one_yr_churn_pct,
        ROUND(
            SUM(CASE WHEN Contract = 'Two year'
                AND Churn = 1
                THEN 1.0 ELSE 0 END) /
            NULLIF(SUM(CASE
                WHEN Contract = 'Two year'
                THEN 1.0 ELSE 0 END), 0) * 100,
            1)                  as two_yr_churn_pct
    FROM customers
    GROUP BY InternetService
""", "PIVOT — churn rate by internet x contract")

# ─────────────────────────────────────────────
# STEP 4: COALESCE & NULLIF
# ─────────────────────────────────────────────
print("\n📌 4. COALESCE & NULLIF")
print("   Essential for handling NULLs in DE!")

run_query("""
    SELECT
        customerID,
        COALESCE(
            NULLIF(TotalCharges, 0),
            MonthlyCharges)     as adjusted_total,
        TotalCharges            as original_total,
        MonthlyCharges,
        tenure
    FROM customers
    WHERE tenure = 0
    OR TotalCharges = 0
    LIMIT 8
""", "COALESCE to handle zero/null charges")

# ─────────────────────────────────────────────
# STEP 5: STRING FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 5. STRING FUNCTIONS")

run_query("""
    SELECT
        customerID,
        UPPER(Contract)         as contract_upper,
        LOWER(InternetService)  as internet_lower,
        LENGTH(customerID)      as id_length,
        SUBSTR(customerID, 1, 4)as id_prefix,
        REPLACE(
            PaymentMethod,
            ' (automatic)', '')  as clean_payment
    FROM customers
    LIMIT 5
""", "String manipulation functions")

run_query("""
    SELECT
        REPLACE(
            PaymentMethod,
            ' (automatic)', '')  as payment_type,
        CASE
            WHEN PaymentMethod LIKE '%automatic%'
            THEN 'Auto-pay'
            ELSE 'Manual'
        END                     as pay_category,
        COUNT(*)                as customers,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct
    FROM customers
    GROUP BY PaymentMethod
    ORDER BY churn_rate_pct DESC
""", "Payment method with string cleaning")

# ─────────────────────────────────────────────
# STEP 6: DATE/NUMERIC FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 6. NUMERIC FUNCTIONS")

run_query("""
    SELECT
        customerID,
        MonthlyCharges,
        tenure,
        ROUND(
            MonthlyCharges * tenure,
            2)                  as lifetime_value,
        ROUND(
            MonthlyCharges * 12,
            2)                  as annual_value,
        ABS(MonthlyCharges -
            64.76)              as diff_from_avg,
        CAST(tenure AS REAL) /
            12                  as tenure_years,
        MIN(MonthlyCharges,
            100)                as capped_charge
    FROM customers
    ORDER BY lifetime_value DESC
    LIMIT 8
""", "Numeric functions for DE calculations")

# ─────────────────────────────────────────────
# STEP 7: GENERATE SUMMARY REPORT
# ─────────────────────────────────────────────
print("\n📌 7. COMPREHENSIVE BUSINESS REPORT")

run_query("""
    WITH lifecycle AS (
        SELECT
            CASE
                WHEN tenure <= 6
                THEN '1-New'
                WHEN tenure <= 12
                THEN '2-Early'
                WHEN tenure <= 24
                THEN '3-Growing'
                WHEN tenure <= 48
                THEN '4-Established'
                ELSE '5-Loyal'
            END                 as stage,
            *
        FROM customers
    ),
    stage_stats AS (
        SELECT
            stage,
            COUNT(*)            as customers,
            SUM(Churn)          as churned,
            ROUND(
                SUM(Churn) * 100.0 /
                COUNT(*), 1)    as churn_pct,
            ROUND(
                SUM(MonthlyCharges),
                2)              as total_revenue,
            ROUND(
                SUM(CASE WHEN Churn = 1
                    THEN MonthlyCharges
                    ELSE 0 END),
                2)              as revenue_at_risk
        FROM lifecycle
        GROUP BY stage
    )
    SELECT
        stage,
        customers,
        churned,
        churn_pct,
        total_revenue,
        revenue_at_risk,
        ROUND(
            revenue_at_risk * 100.0 /
            total_revenue, 1)   as risk_pct,
        SUM(customers) OVER (
            ORDER BY stage
        )                       as cumulative_customers
    FROM stage_stats
    ORDER BY stage
""", "Full lifecycle report with window functions")

conn.close()
logger.info("Module 1 complete")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   UNION ALL      → Combine result sets")
print("   Advanced CASE  → Complex conditions")
print("   PIVOT          → Rows to columns")
print("   COALESCE       → Handle NULLs")
print("   NULLIF         → Avoid divide by zero")
print("   String funcs   → UPPER LOWER LENGTH")
print("   Numeric funcs  → ROUND ABS CAST")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 advanced patterns done")