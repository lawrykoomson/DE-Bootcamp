# ============================================
# WEEK 4 — MODULE 3: SQL Joins
# INNER JOIN, LEFT JOIN, RIGHT JOIN
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
            '../logs/module3.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = "../data/telco.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("=" * 55)
print("   MODULE 3 — SQL JOINS")
print("   INNER | LEFT | RIGHT | CROSS")
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
# STEP 1: CREATE LOOKUP TABLES
# ─────────────────────────────────────────────
print("\n📌 1. CREATING LOOKUP TABLES")

# Contract pricing table
cursor.execute(
    "DROP TABLE IF EXISTS contract_pricing")
cursor.execute("""
    CREATE TABLE contract_pricing (
        contract_type    TEXT PRIMARY KEY,
        discount_pct     INTEGER,
        retention_bonus  INTEGER,
        risk_level       TEXT
    )
""")
cursor.executemany("""
    INSERT INTO contract_pricing VALUES
    (?, ?, ?, ?)
""", [
    ('Month-to-month', 0,   0,   'High'),
    ('One year',       5,   50,  'Medium'),
    ('Two year',       15,  150, 'Low'),
])

# Internet SLA table
cursor.execute(
    "DROP TABLE IF EXISTS internet_sla")
cursor.execute("""
    CREATE TABLE internet_sla (
        service_type   TEXT PRIMARY KEY,
        speed_mbps     INTEGER,
        support_tier   TEXT,
        monthly_cost   REAL
    )
""")
cursor.executemany("""
    INSERT INTO internet_sla VALUES
    (?, ?, ?, ?)
""", [
    ('Fiber optic', 300, 'Premium',  45.0),
    ('DSL',         50,  'Standard', 25.0),
    ('No',          0,   'Basic',    0.0),
])

# Payment risk table
cursor.execute(
    "DROP TABLE IF EXISTS payment_risk")
cursor.execute("""
    CREATE TABLE payment_risk (
        payment_method     TEXT PRIMARY KEY,
        risk_level         TEXT,
        reliability_score  REAL
    )
""")
cursor.executemany("""
    INSERT INTO payment_risk VALUES
    (?, ?, ?)
""", [
    ('Electronic check',        'High',   6.2),
    ('Mailed check',            'Medium', 7.1),
    ('Bank transfer (automatic)','Low',   9.4),
    ('Credit card (automatic)', 'Low',    9.1),
])

conn.commit()
print("   ✅ contract_pricing table created")
print("   ✅ internet_sla table created")
print("   ✅ payment_risk table created")
logger.info("Lookup tables created")

# ─────────────────────────────────────────────
# STEP 2: INNER JOIN
# ─────────────────────────────────────────────
print("\n📌 2. INNER JOIN")
print("   Returns only rows that match in BOTH tables")

run_query("""
    SELECT
        c.customerID,
        c.Contract,
        c.MonthlyCharges,
        c.Churn,
        cp.discount_pct,
        cp.risk_level,
        ROUND(c.MonthlyCharges *
            (1 - cp.discount_pct / 100.0),
            2) as discounted_price
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    LIMIT 8
""", "INNER JOIN — customers with contract pricing")

run_query("""
    SELECT
        cp.contract_type,
        cp.risk_level,
        COUNT(c.customerID)     as customers,
        SUM(c.Churn)            as churned,
        ROUND(
            SUM(c.Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct,
        ROUND(
            AVG(c.MonthlyCharges *
            (1 - cp.discount_pct / 100.0)),
            2)                  as avg_discounted
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    GROUP BY cp.contract_type
    ORDER BY churn_rate_pct DESC
""", "Churn rate with discount analysis")

# ─────────────────────────────────────────────
# STEP 3: LEFT JOIN
# ─────────────────────────────────────────────
print("\n📌 3. LEFT JOIN")
print("   Returns ALL rows from left table")
print("   + matching rows from right table")

run_query("""
    SELECT
        c.customerID,
        c.InternetService,
        c.MonthlyCharges,
        c.Churn,
        s.speed_mbps,
        s.support_tier,
        s.monthly_cost as service_base_cost
    FROM customers c
    LEFT JOIN internet_sla s
        ON c.InternetService = s.service_type
    LIMIT 8
""", "LEFT JOIN — customers with internet SLA")

run_query("""
    SELECT
        s.support_tier,
        COUNT(c.customerID)     as customers,
        SUM(c.Churn)            as churned,
        ROUND(
            SUM(c.Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct,
        ROUND(
            AVG(c.MonthlyCharges), 2)
                                as avg_monthly,
        s.speed_mbps
    FROM customers c
    LEFT JOIN internet_sla s
        ON c.InternetService = s.service_type
    GROUP BY s.support_tier
    ORDER BY churn_rate_pct DESC
""", "Churn by support tier")

# ─────────────────────────────────────────────
# STEP 4: MULTI-TABLE JOIN
# ─────────────────────────────────────────────
print("\n📌 4. MULTI-TABLE JOIN")
print("   Joining 3 tables at once!")

run_query("""
    SELECT
        c.customerID,
        c.Contract,
        c.InternetService,
        c.PaymentMethod,
        c.MonthlyCharges,
        c.Churn,
        cp.risk_level       as contract_risk,
        s.support_tier      as internet_tier,
        pr.risk_level       as payment_risk,
        pr.reliability_score
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    LEFT JOIN internet_sla s
        ON c.InternetService = s.service_type
    LEFT JOIN payment_risk pr
        ON c.PaymentMethod = pr.payment_method
    WHERE c.Churn = 1
    AND cp.risk_level = 'High'
    AND pr.risk_level = 'High'
    LIMIT 8
""", "High risk churned customers — all 3 joins")

run_query("""
    SELECT
        cp.risk_level       as contract_risk,
        pr.risk_level       as payment_risk,
        COUNT(*)            as customers,
        SUM(c.Churn)        as churned,
        ROUND(
            SUM(c.Churn) * 100.0 /
            COUNT(*), 1)    as churn_rate_pct
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    LEFT JOIN payment_risk pr
        ON c.PaymentMethod = pr.payment_method
    GROUP BY cp.risk_level, pr.risk_level
    ORDER BY churn_rate_pct DESC
""", "Risk matrix — contract risk vs payment risk")

# ─────────────────────────────────────────────
# STEP 5: SELF JOIN — Find similar customers
# ─────────────────────────────────────────────
print("\n📌 5. ENRICHED CUSTOMER VIEW")

run_query("""
    SELECT
        c.customerID,
        c.tenure,
        c.Contract,
        c.InternetService,
        c.PaymentMethod,
        c.MonthlyCharges,
        c.Churn,
        cp.risk_level       as contract_risk,
        pr.reliability_score,
        CASE
            WHEN cp.risk_level = 'High'
             AND pr.risk_level = 'High'
            THEN '🔴 Critical Risk'
            WHEN cp.risk_level = 'High'
             OR  pr.risk_level = 'High'
            THEN '🟡 Elevated Risk'
            ELSE '🟢 Low Risk'
        END                 as overall_risk
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    LEFT JOIN payment_risk pr
        ON c.PaymentMethod = pr.payment_method
    WHERE c.Churn = 0
    ORDER BY c.MonthlyCharges DESC
    LIMIT 8
""", "Enriched retained customers with risk scores")

# ─────────────────────────────────────────────
# STEP 6: SAVE ENRICHED TABLE
# ─────────────────────────────────────────────
print("\n📌 6. SAVING ENRICHED CUSTOMER TABLE")

enriched = pd.read_sql_query("""
    SELECT
        c.*,
        cp.discount_pct,
        cp.risk_level           as contract_risk,
        cp.retention_bonus,
        s.speed_mbps,
        s.support_tier,
        pr.risk_level           as payment_risk,
        pr.reliability_score,
        ROUND(c.MonthlyCharges *
            (1 - cp.discount_pct / 100.0),
            2)                  as discounted_price,
        CASE
            WHEN cp.risk_level = 'High'
             AND pr.risk_level = 'High'
            THEN 'Critical'
            WHEN cp.risk_level = 'High'
             OR  pr.risk_level = 'High'
            THEN 'Elevated'
            ELSE 'Low'
        END                     as overall_risk
    FROM customers c
    INNER JOIN contract_pricing cp
        ON c.Contract = cp.contract_type
    LEFT JOIN internet_sla s
        ON c.InternetService = s.service_type
    LEFT JOIN payment_risk pr
        ON c.PaymentMethod = pr.payment_method
""", conn)

enriched.to_csv(
    "../data/customers_enriched.csv",
    index=False)
print(f"   ✅ Saved customers_enriched.csv")
print(f"   Rows:    {len(enriched):,}")
print(f"   Columns: {len(enriched.columns)}")

risk_summary = enriched.groupby(
    'overall_risk').agg(
    Customers=('customerID', 'count'),
    Churned=('Churn', 'sum')
).round(2)
risk_summary['Churn_Rate_%'] = (
    risk_summary['Churned'] /
    risk_summary['Customers'] * 100
).round(1)
print(f"\n   Risk Distribution:")
print(risk_summary.to_string())

conn.close()
logger.info("Module 3 joins complete")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   INNER JOIN → Only matching rows")
print("   LEFT JOIN  → All left + matching right")
print("   Multi-join → 3+ tables at once")
print("   ON         → Join condition")
print("   Aliases    → c, cp, s, pr")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 complete")