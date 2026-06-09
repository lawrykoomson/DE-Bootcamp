# ============================================
# WEEK 5 — MODULE 2: Data Modelling
# Star Schema Design for Data Warehousing
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime

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

W5_DB = Path("../data/telco_w5.db")
conn = sqlite3.connect(str(W5_DB))
cursor = conn.cursor()

print("=" * 55)
print("   MODULE 2 — DATA MODELLING")
print("   STAR SCHEMA | DIM | FACT TABLES")
print("=" * 55)

def run_query(sql, description):
    result = pd.read_sql_query(sql, conn)
    print(f"\n   🔍 {description}")
    print(result.to_string(index=False))
    logger.info(
        f"'{description}': {len(result)} rows")
    return result

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN STAR SCHEMA
# ─────────────────────────────────────────────
print("\n📌 1. STAR SCHEMA DESIGN")
print("""
   A Star Schema has:
   ┌─────────────────────────────────┐
   │  FACT TABLE (centre)            │
   │  → Contains measurable events   │
   │  → Has foreign keys to dims     │
   │  → Examples: transactions,      │
   │    subscriptions, payments      │
   └─────────────────────────────────┘
         ↑         ↑         ↑
   DIM_customer  DIM_plan  DIM_date
   (who?)      (what?)    (when?)

   Benefits:
   ✅ Fast query performance
   ✅ Easy to understand
   ✅ Works great with BI tools
""")

# ─────────────────────────────────────────────
# STEP 2: CREATE DIMENSION TABLES
# ─────────────────────────────────────────────
print("\n📌 2. CREATING DIMENSION TABLES")
logger.info("Creating dimension tables")

# DIM_customer
cursor.execute(
    "DROP TABLE IF EXISTS dim_customer")
cursor.execute("""
    CREATE TABLE dim_customer (
        customer_key     INTEGER PRIMARY KEY,
        customerID       TEXT UNIQUE NOT NULL,
        gender           TEXT,
        senior_citizen   TEXT,
        partner          TEXT,
        dependents       TEXT,
        tenure_group     TEXT,
        created_at       TEXT DEFAULT
            (datetime('now'))
    )
""")

cursor.execute("""
    INSERT INTO dim_customer (
        customerID, gender, senior_citizen,
        partner, dependents, tenure_group)
    SELECT DISTINCT
        customerID,
        gender,
        SeniorCitizen,
        Partner,
        Dependents,
        tenure_group
    FROM customers
""")
print(f"   ✅ dim_customer created: "
      f"{cursor.rowcount:,} rows")

# DIM_plan
cursor.execute(
    "DROP TABLE IF EXISTS dim_plan")
cursor.execute("""
    CREATE TABLE dim_plan (
        plan_key         INTEGER PRIMARY KEY,
        contract_type    TEXT,
        internet_service TEXT,
        phone_service    TEXT,
        streaming_tv     TEXT,
        streaming_movies TEXT,
        online_security  TEXT,
        online_backup    TEXT,
        UNIQUE(contract_type,
               internet_service,
               phone_service)
    )
""")

cursor.execute("""
    INSERT OR IGNORE INTO dim_plan (
        contract_type, internet_service,
        phone_service, streaming_tv,
        streaming_movies, online_security,
        online_backup)
    SELECT DISTINCT
        Contract,
        InternetService,
        PhoneService,
        StreamingTV,
        StreamingMovies,
        OnlineSecurity,
        OnlineBackup
    FROM customers
""")
print(f"   ✅ dim_plan created: "
      f"{cursor.rowcount:,} distinct plans")

# DIM_payment
cursor.execute(
    "DROP TABLE IF EXISTS dim_payment")
cursor.execute("""
    CREATE TABLE dim_payment (
        payment_key         INTEGER PRIMARY KEY,
        payment_method      TEXT UNIQUE,
        payment_category    TEXT,
        is_automatic        INTEGER,
        risk_level          TEXT,
        reliability_score   REAL
    )
""")

cursor.executemany("""
    INSERT OR IGNORE INTO dim_payment (
        payment_method, payment_category,
        is_automatic, risk_level,
        reliability_score)
    VALUES (?, ?, ?, ?, ?)
""", [
    ('Electronic check',
     'Digital', 0, 'High', 6.2),
    ('Mailed check',
     'Paper', 0, 'Medium', 7.1),
    ('Bank transfer (automatic)',
     'Digital', 1, 'Low', 9.4),
    ('Credit card (automatic)',
     'Digital', 1, 'Low', 9.1),
])
print(f"   ✅ dim_payment created: "
      f"4 payment methods")

# DIM_date (simplified)
cursor.execute(
    "DROP TABLE IF EXISTS dim_date")
cursor.execute("""
    CREATE TABLE dim_date (
        date_key     INTEGER PRIMARY KEY,
        year         INTEGER,
        quarter      INTEGER,
        month        INTEGER,
        month_name   TEXT,
        is_weekday   INTEGER
    )
""")

date_data = []
for year in [2023, 2024, 2025]:
    for month in range(1, 13):
        quarter = (month - 1) // 3 + 1
        month_names = [
            'Jan', 'Feb', 'Mar', 'Apr',
            'May', 'Jun', 'Jul', 'Aug',
            'Sep', 'Oct', 'Nov', 'Dec']
        date_data.append((
            year * 100 + month,
            year, quarter, month,
            month_names[month-1], 1
        ))

cursor.executemany("""
    INSERT INTO dim_date VALUES
    (?, ?, ?, ?, ?, ?)
""", date_data)
print(f"   ✅ dim_date created: "
      f"{len(date_data)} periods")

conn.commit()
logger.info("All dimension tables created")

# ─────────────────────────────────────────────
# STEP 3: CREATE FACT TABLE
# ─────────────────────────────────────────────
print("\n📌 3. CREATING FACT TABLE")
logger.info("Creating fact table")

cursor.execute(
    "DROP TABLE IF EXISTS fact_subscription")
cursor.execute("""
    CREATE TABLE fact_subscription (
        subscription_key  INTEGER
            PRIMARY KEY AUTOINCREMENT,
        customer_key      INTEGER,
        plan_key          INTEGER,
        payment_key       INTEGER,
        customerID        TEXT,
        monthly_charges   REAL,
        total_charges     REAL,
        tenure_months     INTEGER,
        is_churned        INTEGER,
        churn_risk_score  INTEGER,
        lifetime_value    REAL,
        loaded_at         TEXT DEFAULT
            (datetime('now')),
        FOREIGN KEY (customer_key)
            REFERENCES dim_customer(
                customer_key),
        FOREIGN KEY (plan_key)
            REFERENCES dim_plan(plan_key),
        FOREIGN KEY (payment_key)
            REFERENCES dim_payment(
                payment_key)
    )
""")

cursor.execute("""
    INSERT INTO fact_subscription (
        customer_key, plan_key,
        payment_key, customerID,
        monthly_charges, total_charges,
        tenure_months, is_churned,
        churn_risk_score, lifetime_value)
    SELECT
        dc.customer_key,
        dp.plan_key,
        dpm.payment_key,
        c.customerID,
        c.MonthlyCharges,
        c.TotalCharges,
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
            WHEN c.tenure <= 12 THEN 3
            WHEN c.tenure <= 24 THEN 2
            WHEN c.tenure <= 48 THEN 1
            ELSE 0
        END,
        ROUND(
            c.MonthlyCharges * c.tenure, 2)
    FROM customers c
    LEFT JOIN dim_customer dc
        ON c.customerID = dc.customerID
    LEFT JOIN dim_plan dp
        ON c.Contract = dp.contract_type
        AND c.InternetService =
            dp.internet_service
        AND c.PhoneService = dp.phone_service
    LEFT JOIN dim_payment dpm
        ON c.PaymentMethod =
            dpm.payment_method
""")

fact_count = cursor.rowcount
conn.commit()
print(f"   ✅ fact_subscription created: "
      f"{fact_count:,} rows")
logger.info(
    f"fact_subscription: {fact_count:,} rows")

# ─────────────────────────────────────────────
# STEP 4: QUERY THE STAR SCHEMA
# ─────────────────────────────────────────────
print("\n📌 4. QUERYING THE STAR SCHEMA")

run_query("""
    SELECT
        dp.contract_type,
        dp.internet_service,
        COUNT(*)                as subscriptions,
        SUM(f.is_churned)       as churned,
        ROUND(
            SUM(f.is_churned) * 100.0 /
            COUNT(*), 1)        as churn_rate,
        ROUND(
            AVG(f.monthly_charges), 2)
                                as avg_monthly,
        ROUND(
            SUM(f.monthly_charges), 2)
                                as total_revenue
    FROM fact_subscription f
    JOIN dim_plan dp
        ON f.plan_key = dp.plan_key
    GROUP BY dp.contract_type,
             dp.internet_service
    ORDER BY churn_rate DESC
    LIMIT 6
""", "Star schema — churn by plan")

run_query("""
    SELECT
        dpm.payment_category,
        dpm.risk_level,
        dpm.reliability_score,
        COUNT(*)                as customers,
        SUM(f.is_churned)       as churned,
        ROUND(
            SUM(f.is_churned) * 100.0 /
            COUNT(*), 1)        as churn_rate,
        ROUND(
            SUM(f.monthly_charges), 2)
                                as total_revenue
    FROM fact_subscription f
    JOIN dim_payment dpm
        ON f.payment_key = dpm.payment_key
    GROUP BY dpm.payment_category,
             dpm.risk_level
    ORDER BY churn_rate DESC
""", "Star schema — churn by payment type")

run_query("""
    SELECT
        dc.tenure_group,
        dc.senior_citizen,
        COUNT(*)                as customers,
        SUM(f.is_churned)       as churned,
        ROUND(
            SUM(f.is_churned) * 100.0 /
            COUNT(*), 1)        as churn_rate,
        ROUND(
            AVG(f.lifetime_value), 2)
                                as avg_ltv
    FROM fact_subscription f
    JOIN dim_customer dc
        ON f.customer_key = dc.customer_key
    GROUP BY dc.tenure_group,
             dc.senior_citizen
    ORDER BY churn_rate DESC
    LIMIT 8
""", "Star schema — churn by customer segment")

# ─────────────────────────────────────────────
# STEP 5: VERIFY STAR SCHEMA
# ─────────────────────────────────────────────
print("\n📌 5. STAR SCHEMA VERIFICATION")

tables = ['dim_customer', 'dim_plan',
          'dim_payment', 'dim_date',
          'fact_subscription']

print(f"\n   {'Table':<25} {'Rows':>8} "
      f"{'Type':<10}")
print("   " + "-" * 45)
for table in tables:
    count = pd.read_sql_query(
        f"SELECT COUNT(*) as n FROM {table}",
        conn).iloc[0]['n']
    ttype = "FACT" if "fact" in table \
        else "DIM"
    print(f"   {table:<25} "
          f"{count:>8,} {ttype:<10}")

conn.close()
logger.info("Module 2 data modelling complete")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   Star Schema → Fact + Dimension tables")
print("   dim_customer → WHO are the customers")
print("   dim_plan     → WHAT plan they are on")
print("   dim_payment  → HOW they pay")
print("   dim_date     → WHEN events happen")
print("   fact_table   → MEASURABLE events")
print("   FK → Foreign keys link tables")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 complete")