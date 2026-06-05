# ============================================
# WEEK 4 — MODULE 2: SQL Aggregations
# GROUP BY, COUNT, SUM, AVG, MIN, MAX, HAVING
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
            '../logs/module2.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = "../data/telco.db"
conn = sqlite3.connect(DB_PATH)

print("=" * 55)
print("   MODULE 2 — SQL AGGREGATIONS")
print("   GROUP BY | COUNT | SUM | AVG | HAVING")
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
# STEP 1: BASIC AGGREGATIONS
# ─────────────────────────────────────────────
print("\n📌 1. BASIC AGGREGATIONS")

run_query("""
    SELECT
        COUNT(*)           as total_customers,
        SUM(Churn)         as total_churned,
        ROUND(AVG(MonthlyCharges), 2)
                           as avg_monthly_charge,
        ROUND(AVG(tenure), 1)
                           as avg_tenure_months,
        MIN(MonthlyCharges)as min_charge,
        MAX(MonthlyCharges)as max_charge,
        ROUND(SUM(MonthlyCharges), 2)
                           as total_monthly_revenue
    FROM customers
""", "Overall business KPIs")

# ─────────────────────────────────────────────
# STEP 2: GROUP BY CONTRACT
# ─────────────────────────────────────────────
print("\n📌 2. GROUP BY — CONTRACT TYPE")

run_query("""
    SELECT
        Contract,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(AVG(MonthlyCharges), 2)
                                    as avg_monthly,
        ROUND(AVG(tenure), 1)       as avg_tenure
    FROM customers
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
""", "Churn analysis by contract type")

# ─────────────────────────────────────────────
# STEP 3: GROUP BY INTERNET SERVICE
# ─────────────────────────────────────────────
print("\n📌 3. GROUP BY — INTERNET SERVICE")

run_query("""
    SELECT
        InternetService,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(AVG(MonthlyCharges), 2)
                                    as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2) as total_revenue
    FROM customers
    GROUP BY InternetService
    ORDER BY churn_rate_pct DESC
""", "Churn and revenue by internet service")

# ─────────────────────────────────────────────
# STEP 4: GROUP BY PAYMENT METHOD
# ─────────────────────────────────────────────
print("\n📌 4. GROUP BY — PAYMENT METHOD")

run_query("""
    SELECT
        PaymentMethod,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(AVG(MonthlyCharges), 2)
                                    as avg_monthly
    FROM customers
    GROUP BY PaymentMethod
    ORDER BY churn_rate_pct DESC
""", "Churn rate by payment method")

# ─────────────────────────────────────────────
# STEP 5: GROUP BY TENURE GROUP
# ─────────────────────────────────────────────
print("\n📌 5. GROUP BY — TENURE GROUP")

run_query("""
    SELECT
        tenure_group,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(AVG(MonthlyCharges), 2)
                                    as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2) as total_revenue
    FROM customers
    GROUP BY tenure_group
    ORDER BY churn_rate_pct DESC
""", "Churn rate by tenure group")

# ─────────────────────────────────────────────
# STEP 6: HAVING CLAUSE
# ─────────────────────────────────────────────
print("\n📌 6. HAVING CLAUSE — FILTER GROUPS")

run_query("""
    SELECT
        Contract,
        InternetService,
        COUNT(*)                    as total,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct
    FROM customers
    GROUP BY Contract, InternetService
    HAVING churn_rate_pct > 40
    ORDER BY churn_rate_pct DESC
""", "High risk segments (churn rate > 40%)")

# ─────────────────────────────────────────────
# STEP 7: REVENUE ANALYSIS
# ─────────────────────────────────────────────
print("\n📌 7. REVENUE ANALYSIS")

run_query("""
    SELECT
        CASE
            WHEN Churn = 1 THEN 'Churned'
            ELSE 'Retained'
        END                         as status,
        COUNT(*)                    as customers,
        ROUND(
            SUM(MonthlyCharges), 2) as monthly_revenue,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly,
        ROUND(
            SUM(TotalCharges), 2)   as lifetime_revenue
    FROM customers
    GROUP BY Churn
    ORDER BY Churn DESC
""", "Revenue comparison: churned vs retained")

# ─────────────────────────────────────────────
# STEP 8: MULTI-LEVEL GROUPING
# ─────────────────────────────────────────────
print("\n📌 8. MULTI-LEVEL GROUPING")

run_query("""
    SELECT
        Contract,
        SeniorCitizen,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly
    FROM customers
    GROUP BY Contract, SeniorCitizen
    ORDER BY Contract, SeniorCitizen
""", "Churn by contract and senior citizen status")

# ─────────────────────────────────────────────
# STEP 9: SAVE AGGREGATION RESULTS
# ─────────────────────────────────────────────
print("\n📌 9. SAVING KEY AGGREGATIONS TO CSV")

os.makedirs("../data", exist_ok=True)

# Save contract summary
contract_summary = pd.read_sql_query("""
    SELECT
        Contract,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2) as total_revenue
    FROM customers
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
""", conn)
contract_summary.to_csv(
    "../data/contract_summary.csv", index=False)
print(f"   ✅ Saved contract_summary.csv")

# Save segment risk table
segment_risk = pd.read_sql_query("""
    SELECT
        Contract,
        InternetService,
        tenure_group,
        COUNT(*)                    as total,
        SUM(Churn)                  as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly
    FROM customers
    GROUP BY Contract, InternetService,
             tenure_group
    ORDER BY churn_rate_pct DESC
""", conn)
segment_risk.to_csv(
    "../data/segment_risk.csv", index=False)
print(f"   ✅ Saved segment_risk.csv")
print(f"   Segments analysed: {len(segment_risk)}")

conn.close()
logger.info(
    "Module 2 aggregations complete")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   COUNT(*)     → Count rows")
print("   SUM()        → Total values")
print("   AVG()        → Average values")
print("   MIN/MAX()    → Extremes")
print("   GROUP BY     → Group rows")
print("   HAVING       → Filter groups")
print("   CASE WHEN    → Conditional labels")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 complete")