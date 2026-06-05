# ============================================
# WEEK 4 — MODULE 4: Subqueries & CTEs
# WITH, IN, EXISTS, Nested SELECT
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import logging
import os

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

DB_PATH = "../data/telco.db"
conn = sqlite3.connect(DB_PATH)

print("=" * 55)
print("   MODULE 4 — SUBQUERIES & CTEs")
print("   WITH | IN | EXISTS | NESTED SELECT")
print("=" * 55)

def run_query(sql, description):
    """Run SQL and display results."""
    print(f"\n   🔍 {description}")
    result = pd.read_sql_query(sql, conn)
    print(result.to_string(index=False))
    logger.info(
        f"Query '{description}': "
        f"{len(result)} rows")
    return result

# ─────────────────────────────────────────────
# STEP 1: SCALAR SUBQUERY
# ─────────────────────────────────────────────
print("\n📌 1. SCALAR SUBQUERY")
print("   A subquery that returns ONE value")

run_query("""
    SELECT
        customerID,
        MonthlyCharges,
        ROUND((SELECT AVG(MonthlyCharges)
               FROM customers), 2)
                        as avg_charge,
        ROUND(MonthlyCharges -
            (SELECT AVG(MonthlyCharges)
             FROM customers), 2)
                        as diff_from_avg,
        Contract,
        Churn
    FROM customers
    ORDER BY diff_from_avg DESC
    LIMIT 8
""", "Customers vs average monthly charge")

# ─────────────────────────────────────────────
# STEP 2: SUBQUERY IN WHERE
# ─────────────────────────────────────────────
print("\n📌 2. SUBQUERY IN WHERE CLAUSE")

run_query("""
    SELECT customerID, MonthlyCharges,
           Contract, Churn
    FROM customers
    WHERE MonthlyCharges > (
        SELECT AVG(MonthlyCharges)
        FROM customers
        WHERE Churn = 1
    )
    AND Churn = 0
    ORDER BY MonthlyCharges DESC
    LIMIT 8
""", "Retained customers paying more than avg churned")

run_query("""
    SELECT COUNT(*) as at_risk_count
    FROM customers
    WHERE MonthlyCharges > (
        SELECT AVG(MonthlyCharges)
        FROM customers
        WHERE Churn = 1
    )
    AND Contract = 'Month-to-month'
    AND Churn = 0
""", "At-risk retained customers above churn avg")

# ─────────────────────────────────────────────
# STEP 3: SUBQUERY WITH IN
# ─────────────────────────────────────────────
print("\n📌 3. SUBQUERY WITH IN")

run_query("""
    SELECT customerID, Contract,
           InternetService, MonthlyCharges
    FROM customers
    WHERE Contract IN (
        SELECT contract_type
        FROM contract_pricing
        WHERE risk_level = 'High'
    )
    AND Churn = 0
    LIMIT 8
""", "Retained customers on high-risk contracts")

# ─────────────────────────────────────────────
# STEP 4: CTE — WITH CLAUSE
# ─────────────────────────────────────────────
print("\n📌 4. CTEs — WITH CLAUSE")
print("   Makes complex queries readable!")

run_query("""
    WITH churn_stats AS (
        SELECT
            Contract,
            COUNT(*)                    as total,
            SUM(Churn)                  as churned,
            ROUND(
                SUM(Churn) * 100.0 /
                COUNT(*), 1)            as churn_rate,
            ROUND(
                AVG(MonthlyCharges), 2) as avg_charge
        FROM customers
        GROUP BY Contract
    )
    SELECT *
    FROM churn_stats
    ORDER BY churn_rate DESC
""", "CTE — contract churn stats")

# ─────────────────────────────────────────────
# STEP 5: MULTIPLE CTEs
# ─────────────────────────────────────────────
print("\n📌 5. MULTIPLE CTEs")

run_query("""
    WITH
    churned_summary AS (
        SELECT
            Contract,
            COUNT(*)                    as churned_count,
            ROUND(
                AVG(MonthlyCharges), 2) as churned_avg_charge,
            ROUND(
                AVG(tenure), 1)         as churned_avg_tenure
        FROM customers
        WHERE Churn = 1
        GROUP BY Contract
    ),
    retained_summary AS (
        SELECT
            Contract,
            COUNT(*)                    as retained_count,
            ROUND(
                AVG(MonthlyCharges), 2) as retained_avg_charge,
            ROUND(
                AVG(tenure), 1)         as retained_avg_tenure
        FROM customers
        WHERE Churn = 0
        GROUP BY Contract
    )
    SELECT
        c.Contract,
        c.churned_count,
        c.churned_avg_charge,
        c.churned_avg_tenure,
        r.retained_count,
        r.retained_avg_charge,
        r.retained_avg_tenure
    FROM churned_summary c
    JOIN retained_summary r
        ON c.Contract = r.Contract
    ORDER BY c.churned_count DESC
""", "Multiple CTEs — churned vs retained by contract")

# ─────────────────────────────────────────────
# STEP 6: CTE WITH BUSINESS LOGIC
# ─────────────────────────────────────────────
print("\n📌 6. CTE — CUSTOMER RISK SCORING")

run_query("""
    WITH risk_scores AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            PaymentMethod,
            tenure,
            MonthlyCharges,
            Churn,
            CASE Contract
                WHEN 'Month-to-month' THEN 3
                WHEN 'One year'       THEN 1
                WHEN 'Two year'       THEN 0
            END as contract_risk_score,
            CASE PaymentMethod
                WHEN 'Electronic check' THEN 3
                WHEN 'Mailed check'     THEN 2
                ELSE 0
            END as payment_risk_score,
            CASE
                WHEN tenure <= 12  THEN 3
                WHEN tenure <= 24  THEN 2
                WHEN tenure <= 48  THEN 1
                ELSE 0
            END as tenure_risk_score
        FROM customers
    ),
    scored_customers AS (
        SELECT *,
            contract_risk_score +
            payment_risk_score +
            tenure_risk_score   as total_risk_score,
            CASE
                WHEN contract_risk_score +
                     payment_risk_score +
                     tenure_risk_score >= 7
                THEN '🔴 Critical'
                WHEN contract_risk_score +
                     payment_risk_score +
                     tenure_risk_score >= 4
                THEN '🟡 High'
                WHEN contract_risk_score +
                     payment_risk_score +
                     tenure_risk_score >= 2
                THEN '🟠 Medium'
                ELSE '🟢 Low'
            END as risk_band
        FROM risk_scores
    )
    SELECT
        risk_band,
        COUNT(*)                    as customers,
        SUM(Churn)                  as actual_churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly,
        ROUND(
            AVG(total_risk_score), 1)
                                    as avg_risk_score
    FROM scored_customers
    GROUP BY risk_band
    ORDER BY avg_risk_score DESC
""", "Risk scoring model — CTE chained analysis")

# ─────────────────────────────────────────────
# STEP 7: CTE — REVENUE RECOVERY OPPORTUNITY
# ─────────────────────────────────────────────
print("\n📌 7. REVENUE RECOVERY OPPORTUNITY")

run_query("""
    WITH at_risk_customers AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            PaymentMethod,
            MonthlyCharges,
            tenure,
            CASE Contract
                WHEN 'Month-to-month' THEN 3
                WHEN 'One year'       THEN 1
                ELSE 0
            END +
            CASE PaymentMethod
                WHEN 'Electronic check' THEN 3
                WHEN 'Mailed check'     THEN 2
                ELSE 0
            END +
            CASE
                WHEN tenure <= 12 THEN 3
                WHEN tenure <= 24 THEN 2
                ELSE 0
            END                     as risk_score
        FROM customers
        WHERE Churn = 0
    ),
    high_risk_retained AS (
        SELECT *
        FROM at_risk_customers
        WHERE risk_score >= 6
    )
    SELECT
        COUNT(*)                    as at_risk_customers,
        ROUND(
            SUM(MonthlyCharges), 2) as monthly_revenue_at_risk,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly_charge,
        ROUND(
            SUM(MonthlyCharges) * 12,
            2)                      as annual_revenue_at_risk
    FROM high_risk_retained
""", "Revenue recovery opportunity — high risk retained")

conn.close()
logger.info("Module 4 complete")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   Scalar subquery → Single value in SELECT")
print("   WHERE subquery  → Filter with subquery")
print("   IN subquery     → Match against list")
print("   WITH (CTE)      → Named temp table")
print("   Multiple CTEs   → Chain logic cleanly")
print("   Risk scoring    → Business logic in SQL")
print(f"\n✅ Module 4 Complete!")
logger.info("Module 4 subqueries CTEs complete")