# ============================================
# WEEK 5 — MODULE 4: Views & Stored Procedures
# CREATE VIEW | TEMP VIEW | REUSABLE QUERIES
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

W5_DB = Path("../data/telco_w5.db")
conn = sqlite3.connect(str(W5_DB))
cursor = conn.cursor()

print("=" * 55)
print("   MODULE 4 — VIEWS")
print("   CREATE VIEW | TEMP VIEW | REUSABLE")
print("=" * 55)

def run_query(sql, description):
    result = pd.read_sql_query(sql, conn)
    print(f"\n   🔍 {description}")
    print(result.to_string(index=False))
    logger.info(
        f"'{description}': {len(result)} rows")
    return result

# ─────────────────────────────────────────────
# STEP 1: CREATE BASIC VIEW
# ─────────────────────────────────────────────
print("\n📌 1. CREATING VIEWS")
print("   A VIEW is a saved SQL query")
print("   Acts like a virtual table!")
logger.info("Creating database views")

# Drop existing views
for view in [
    'vw_customer_risk',
    'vw_churn_summary',
    'vw_revenue_dashboard',
    'vw_intervention_list',
    'vw_executive_kpis'
]:
    cursor.execute(
        f"DROP VIEW IF EXISTS {view}")

# View 1 — Customer risk profile
cursor.execute("""
    CREATE VIEW vw_customer_risk AS
    SELECT
        c.customerID,
        c.Contract,
        c.InternetService,
        c.PaymentMethod,
        c.MonthlyCharges,
        c.tenure,
        c.Churn,
        CASE c.Contract
            WHEN 'Month-to-month' THEN 3
            WHEN 'One year'       THEN 1
            ELSE 0
        END +
        CASE c.PaymentMethod
            WHEN 'Electronic check' THEN 3
            WHEN 'Mailed check'     THEN 2
            ELSE 0
        END +
        CASE
            WHEN c.tenure <= 12  THEN 3
            WHEN c.tenure <= 24  THEN 2
            WHEN c.tenure <= 48  THEN 1
            ELSE 0
        END                     as risk_score,
        CASE
            WHEN (CASE c.Contract
                WHEN 'Month-to-month'
                THEN 3 ELSE 0 END +
                CASE c.PaymentMethod
                WHEN 'Electronic check'
                THEN 3 ELSE 0 END +
                CASE WHEN c.tenure <= 12
                THEN 3 ELSE 0 END) >= 7
            THEN 'Critical'
            WHEN (CASE c.Contract
                WHEN 'Month-to-month'
                THEN 3 ELSE 0 END) >= 3
            THEN 'High'
            ELSE 'Low'
        END                     as risk_level,
        ROUND(
            c.MonthlyCharges * c.tenure,
            2)                  as lifetime_value
    FROM customers c
""")
print("   ✅ vw_customer_risk created")

# View 2 — Churn summary by segment
cursor.execute("""
    CREATE VIEW vw_churn_summary AS
    SELECT
        Contract,
        InternetService,
        COUNT(*)                as total,
        SUM(Churn)              as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)
                                as avg_monthly,
        ROUND(
            SUM(MonthlyCharges), 2)
                                as total_revenue,
        ROUND(
            SUM(CASE WHEN Churn = 1
                THEN MonthlyCharges
                ELSE 0 END), 2) as revenue_at_risk
    FROM customers
    GROUP BY Contract, InternetService
""")
print("   ✅ vw_churn_summary created")

# View 3 — Revenue dashboard
cursor.execute("""
    CREATE VIEW vw_revenue_dashboard AS
    SELECT
        CASE
            WHEN tenure <= 12
            THEN '1-New (0-12m)'
            WHEN tenure <= 24
            THEN '2-Early (13-24m)'
            WHEN tenure <= 48
            THEN '3-Mid (25-48m)'
            ELSE '4-Loyal (49m+)'
        END                     as lifecycle,
        Contract,
        COUNT(*)                as customers,
        ROUND(
            SUM(MonthlyCharges), 2)
                                as monthly_revenue,
        ROUND(
            SUM(CASE WHEN Churn = 1
                THEN MonthlyCharges
                ELSE 0 END), 2) as revenue_at_risk,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct
    FROM customers
    GROUP BY lifecycle, Contract
""")
print("   ✅ vw_revenue_dashboard created")

# View 4 — Intervention list
cursor.execute("""
    CREATE VIEW vw_intervention_list AS
    SELECT
        customerID,
        Contract,
        InternetService,
        PaymentMethod,
        MonthlyCharges,
        tenure,
        risk_score,
        lifetime_value,
        CASE
            WHEN risk_score >= 8
            THEN '🔴 Immediate call'
            WHEN risk_score >= 6
            THEN '🟡 Email campaign'
            WHEN risk_score >= 4
            THEN '🟠 Auto-pay offer'
            ELSE '🟢 Standard programme'
        END                     as action
    FROM vw_customer_risk
    WHERE Churn = 0
    ORDER BY risk_score DESC,
             MonthlyCharges DESC
""")
print("   ✅ vw_intervention_list created")

# View 5 — Executive KPIs
cursor.execute("""
    CREATE VIEW vw_executive_kpis AS
    SELECT
        COUNT(*)                    as total_customers,
        SUM(Churn)                  as total_churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 2)            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2) as monthly_revenue,
        ROUND(
            SUM(CASE WHEN Churn = 1
                THEN MonthlyCharges
                ELSE 0 END), 2)     as revenue_at_risk,
        ROUND(
            AVG(MonthlyCharges), 2) as avg_monthly_charge,
        ROUND(
            AVG(tenure), 1)         as avg_tenure_months,
        ROUND(
            AVG(CASE WHEN Churn = 0
                THEN MonthlyCharges * tenure
                END), 2)            as avg_retained_clv
    FROM customers
""")
print("   ✅ vw_executive_kpis created")

conn.commit()
logger.info("All 5 views created")

# ─────────────────────────────────────────────
# STEP 2: QUERY VIEWS LIKE TABLES
# ─────────────────────────────────────────────
print("\n📌 2. QUERYING VIEWS LIKE TABLES")

run_query("""
    SELECT *
    FROM vw_executive_kpis
""", "Executive KPIs from view")

run_query("""
    SELECT *
    FROM vw_churn_summary
    ORDER BY churn_rate_pct DESC
    LIMIT 5
""", "Top 5 high churn segments from view")

run_query("""
    SELECT action,
           COUNT(*)                as customers,
           ROUND(
               SUM(MonthlyCharges), 2)
                                   as at_risk_revenue
    FROM vw_intervention_list
    GROUP BY action
    ORDER BY customers DESC
""", "Intervention actions from view")

# ─────────────────────────────────────────────
# STEP 3: FILTER AND JOIN VIEWS
# ─────────────────────────────────────────────
print("\n📌 3. FILTERING AND JOINING VIEWS")

run_query("""
    SELECT customerID, Contract,
           MonthlyCharges, risk_score,
           action
    FROM vw_intervention_list
    WHERE MonthlyCharges >= 100
    AND risk_score >= 7
    ORDER BY MonthlyCharges DESC
    LIMIT 8
""", "High-value critical risk customers")

run_query("""
    SELECT
        vr.lifecycle,
        vr.Contract,
        vr.customers,
        vr.monthly_revenue,
        vr.churn_rate_pct,
        vr.revenue_at_risk
    FROM vw_revenue_dashboard vr
    WHERE vr.churn_rate_pct > 30
    ORDER BY vr.revenue_at_risk DESC
""", "High risk segments from revenue view")

# ─────────────────────────────────────────────
# STEP 4: PYTHON STORED PROCEDURES
# ─────────────────────────────────────────────
print("\n📌 4. PYTHON STORED PROCEDURES")
print("   In SQLite we use Python functions")
print("   In PostgreSQL these are actual")
print("   stored procedures!")
logger.info("Testing Python stored procedures")

def get_customer_risk_report(
        min_monthly=50,
        max_risk_score=None,
        limit=10):
    """
    Reusable function — like a stored procedure.
    Returns at-risk customers above a charge threshold.
    """
    conditions = [
    f"MonthlyCharges >= {min_monthly}",
    ]
    if max_risk_score:
        conditions.append(
            f"risk_score >= {max_risk_score}")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT customerID, Contract,
               InternetService, MonthlyCharges,
               risk_score, action
        FROM vw_intervention_list
        WHERE {where}
        ORDER BY risk_score DESC,
                 MonthlyCharges DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(sql, conn)

def get_segment_performance(
        contract=None,
        internet=None):
    """
    Returns segment performance metrics.
    Parameterised like a stored procedure.
    """
    conditions = ["1=1"]
    if contract:
        conditions.append(
            f"Contract = '{contract}'")
    if internet:
        conditions.append(
            f"InternetService = '{internet}'")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT
            Contract,
            InternetService,
            COUNT(*)                as customers,
            ROUND(
                SUM(Churn) * 100.0 /
                COUNT(*), 1)        as churn_rate,
            ROUND(
                AVG(MonthlyCharges), 2)
                                    as avg_monthly,
            ROUND(
                SUM(MonthlyCharges), 2)
                                    as total_revenue
        FROM customers
        WHERE {where}
        GROUP BY Contract, InternetService
        ORDER BY churn_rate DESC
    """
    return pd.read_sql_query(sql, conn)

# Test stored procedure functions
print("\n   🔍 get_customer_risk_report"
      "(min_monthly=100, max_risk_score=7)")
result1 = get_customer_risk_report(
    min_monthly=100, max_risk_score=7)
print(result1.to_string(index=False))
logger.info(
    f"Risk report: {len(result1)} customers")

print("\n   🔍 get_segment_performance"
      "(contract='Month-to-month')")
result2 = get_segment_performance(
    contract='Month-to-month')
print(result2.to_string(index=False))
logger.info(
    f"Segment performance: "
    f"{len(result2)} segments")

# ─────────────────────────────────────────────
# STEP 5: LIST ALL VIEWS
# ─────────────────────────────────────────────
print("\n📌 5. ALL VIEWS IN DATABASE")

run_query("""
    SELECT name, type
    FROM sqlite_master
    WHERE type = 'view'
    ORDER BY name
""", "All views in telco_w5.db")

conn.close()
logger.info("Module 4 views complete")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   CREATE VIEW    → Save SQL as virtual table")
print("   Query views    → SELECT * FROM view_name")
print("   Filter views   → Add WHERE to view")
print("   Join views     → Combine multiple views")
print("   Python funcs   → Reusable parameterised")
print("                    queries like stored procs")
print(f"\n✅ Module 4 Complete!")
logger.info("Module 4 complete")