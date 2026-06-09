# ============================================
# WEEK 5 — MODULE 3: SQL for ETL
# INSERT, UPDATE, DELETE, UPSERT, MERGE
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
            '../logs/module3.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

W5_DB = Path("../data/telco_w5.db")
conn = sqlite3.connect(str(W5_DB))
cursor = conn.cursor()

print("=" * 55)
print("   MODULE 3 — SQL FOR ETL")
print("   INSERT | UPDATE | DELETE | UPSERT")
print("=" * 55)

def run_query(sql, description):
    result = pd.read_sql_query(sql, conn)
    print(f"\n   🔍 {description}")
    print(result.to_string(index=False))
    logger.info(
        f"'{description}': {len(result)} rows")
    return result

def count_table(table):
    return pd.read_sql_query(
        f"SELECT COUNT(*) as n FROM {table}",
        conn).iloc[0]['n']

# ─────────────────────────────────────────────
# STEP 1: CREATE STAGING TABLE
# ─────────────────────────────────────────────
print("\n📌 1. STAGING TABLE PATTERN")
print("   DE best practice: load raw data first")
print("   then transform into final tables")
logger.info("Creating staging tables")

cursor.execute(
    "DROP TABLE IF EXISTS staging_new_customers")
cursor.execute("""
    CREATE TABLE staging_new_customers (
        customerID       TEXT,
        gender           TEXT,
        SeniorCitizen    TEXT,
        Partner          TEXT,
        Dependents       TEXT,
        tenure           INTEGER,
        PhoneService     TEXT,
        MultipleLines    TEXT,
        InternetService  TEXT,
        OnlineSecurity   TEXT,
        OnlineBackup     TEXT,
        DeviceProtection TEXT,
        TechSupport      TEXT,
        StreamingTV      TEXT,
        StreamingMovies  TEXT,
        Contract         TEXT,
        PaperlessBilling TEXT,
        PaymentMethod    TEXT,
        MonthlyCharges   REAL,
        TotalCharges     REAL,
        Churn            INTEGER,
        staged_at        TEXT DEFAULT
            (datetime('now')),
        is_processed     INTEGER DEFAULT 0
    )
""")
conn.commit()
print("   ✅ staging_new_customers created")

# ─────────────────────────────────────────────
# STEP 2: INSERT NEW RECORDS
# ─────────────────────────────────────────────
print("\n📌 2. INSERT — LOADING NEW CUSTOMERS")
logger.info("Inserting new customer records")

new_customers = [
    ('NEW001', 'Male', 'No', 'Yes', 'No',
     1, 'Yes', 'No', 'Fiber optic',
     'No', 'No', 'No', 'No', 'No', 'No',
     'Month-to-month', 'Yes',
     'Electronic check', 89.50, 89.50,
     0),
    ('NEW002', 'Female', 'Yes', 'No', 'No',
     2, 'Yes', 'Yes', 'Fiber optic',
     'No', 'Yes', 'No', 'No', 'Yes', 'No',
     'Month-to-month', 'Yes',
     'Electronic check', 99.00, 198.00,
     0),
    ('NEW003', 'Male', 'No', 'Yes', 'Yes',
     1, 'Yes', 'No', 'DSL',
     'Yes', 'Yes', 'Yes', 'Yes', 'No', 'No',
     'One year', 'No',
     'Bank transfer (automatic)', 65.00,
     65.00, 0),
    ('NEW004', 'Female', 'No', 'No', 'No',
     3, 'Yes', 'No', 'Fiber optic',
     'No', 'No', 'No', 'No', 'No', 'Yes',
     'Two year', 'No',
     'Credit card (automatic)', 78.00,
     234.00, 0),
    ('NEW005', 'Male', 'Yes', 'Yes', 'No',
     1, 'No', 'No phone service', 'DSL',
     'No', 'Yes', 'No', 'No', 'No', 'No',
     'Month-to-month', 'Yes',
     'Mailed check', 35.00, 35.00, 0),
]

cursor.executemany("""
    INSERT INTO staging_new_customers (
        customerID, gender, SeniorCitizen,
        Partner, Dependents, tenure,
        PhoneService, MultipleLines,
        InternetService, OnlineSecurity,
        OnlineBackup, DeviceProtection,
        TechSupport, StreamingTV,
        StreamingMovies, Contract,
        PaperlessBilling, PaymentMethod,
        MonthlyCharges, TotalCharges, Churn)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?)
""", new_customers)
conn.commit()

staged = count_table(
    'staging_new_customers')
print(f"   ✅ Inserted {staged} new "
      f"customers into staging")

run_query("""
    SELECT customerID, gender,
           Contract, InternetService,
           MonthlyCharges, staged_at,
           is_processed
    FROM staging_new_customers
""", "Staged new customers")

# ─────────────────────────────────────────────
# STEP 3: VALIDATE STAGED DATA
# ─────────────────────────────────────────────
print("\n📌 3. VALIDATE STAGED DATA")
logger.info("Validating staged data")

run_query("""
    SELECT
        COUNT(*)                as total,
        SUM(CASE WHEN customerID IS NULL
            OR customerID = ''
            THEN 1 ELSE 0 END)  as missing_id,
        SUM(CASE WHEN MonthlyCharges <= 0
            THEN 1 ELSE 0 END)  as invalid_charge,
        SUM(CASE WHEN Contract NOT IN (
            'Month-to-month',
            'One year', 'Two year')
            THEN 1 ELSE 0 END)  as invalid_contract,
        SUM(CASE WHEN customerID IN (
            SELECT customerID FROM customers)
            THEN 1 ELSE 0 END)  as duplicates
    FROM staging_new_customers
""", "Validation results for staged data")

# ─────────────────────────────────────────────
# STEP 4: INSERT FROM STAGING TO MAIN
# ─────────────────────────────────────────────
print("\n📌 4. INSERT FROM STAGING TO MAIN TABLE")
logger.info("Moving valid records to main table")

before_count = count_table('customers')

cursor.execute("""
    INSERT INTO customers (
        customerID, gender, SeniorCitizen,
        Partner, Dependents, tenure,
        PhoneService, MultipleLines,
        InternetService, OnlineSecurity,
        OnlineBackup, DeviceProtection,
        TechSupport, StreamingTV,
        StreamingMovies, Contract,
        PaperlessBilling, PaymentMethod,
        MonthlyCharges, TotalCharges, Churn)
    SELECT
        customerID, gender, SeniorCitizen,
        Partner, Dependents, tenure,
        PhoneService, MultipleLines,
        InternetService, OnlineSecurity,
        OnlineBackup, DeviceProtection,
        TechSupport, StreamingTV,
        StreamingMovies, Contract,
        PaperlessBilling, PaymentMethod,
        MonthlyCharges, TotalCharges, Churn
    FROM staging_new_customers
    WHERE is_processed = 0
    AND customerID NOT IN (
        SELECT customerID FROM customers)
    AND MonthlyCharges > 0
""")

inserted = cursor.rowcount
conn.commit()

after_count = count_table('customers')
print(f"   Before: {before_count:,} customers")
print(f"   After:  {after_count:,} customers")
print(f"   ✅ Inserted: {inserted} new records")
logger.info(
    f"Inserted {inserted} records from staging")

# Mark as processed
cursor.execute("""
    UPDATE staging_new_customers
    SET is_processed = 1
    WHERE customerID IN (
        SELECT customerID FROM customers
        WHERE customerID LIKE 'NEW%')
""")
conn.commit()
print(f"   ✅ Staged records marked as processed")

# ─────────────────────────────────────────────
# STEP 5: UPDATE — Modify existing records
# ─────────────────────────────────────────────
print("\n📌 5. UPDATE — MODIFYING RECORDS")
logger.info("Testing UPDATE operations")

run_query("""
    SELECT customerID, Contract,
           MonthlyCharges, tenure
    FROM customers
    WHERE customerID LIKE 'NEW%'
""", "New customers before update")

cursor.execute("""
    UPDATE customers
    SET tenure = tenure + 1,
        TotalCharges = ROUND(
            TotalCharges + MonthlyCharges, 2)
    WHERE customerID LIKE 'NEW%'
""")
conn.commit()

updated = cursor.rowcount
print(f"\n   ✅ Updated {updated} records "
      f"— tenure +1 month")

run_query("""
    SELECT customerID, Contract,
           MonthlyCharges, tenure,
           TotalCharges
    FROM customers
    WHERE customerID LIKE 'NEW%'
""", "New customers after tenure update")

# ─────────────────────────────────────────────
# STEP 6: UPSERT — INSERT OR UPDATE
# ─────────────────────────────────────────────
print("\n📌 6. UPSERT — INSERT OR REPLACE")
print("   Key DE pattern — update if exists,")
print("   insert if new!")
logger.info("Testing UPSERT pattern")

cursor.execute("""
    DROP TABLE IF EXISTS customer_scores
""")
cursor.execute("""
    CREATE TABLE customer_scores (
        customerID       TEXT PRIMARY KEY,
        risk_score       INTEGER,
        churn_probability REAL,
        recommended_action TEXT,
        scored_at        TEXT
    )
""")

cursor.execute("""
    INSERT INTO customer_scores (
        customerID, risk_score,
        churn_probability,
        recommended_action, scored_at)
    SELECT
        customerID,
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
            WHEN tenure <= 48 THEN 1
            ELSE 0
        END                     as risk_score,
        ROUND(
            (CASE Contract
                WHEN 'Month-to-month'
                THEN 0.43
                WHEN 'One year' THEN 0.11
                ELSE 0.03
            END +
            CASE
                WHEN tenure <= 12
                THEN 0.15 ELSE 0
            END) / 2.0, 3)      as churn_probability,
        CASE
            WHEN (CASE Contract
                WHEN 'Month-to-month'
                THEN 3 ELSE 0 END +
                CASE
                WHEN tenure <= 12 THEN 3
                ELSE 0 END) >= 5
            THEN 'Immediate intervention'
            WHEN (CASE Contract
                WHEN 'Month-to-month'
                THEN 3 ELSE 0 END) >= 3
            THEN 'Email campaign'
            ELSE 'Standard programme'
        END,
        datetime('now')
    FROM customers
    WHERE Churn = 0
    LIMIT 100
""")

scores_count = count_table(
    'customer_scores')
conn.commit()
print(f"   ✅ Initial scores: {scores_count}")

# Now UPSERT with updated scores
cursor.execute("""
    INSERT OR REPLACE INTO customer_scores (
        customerID, risk_score,
        churn_probability,
        recommended_action, scored_at)
    SELECT
        customerID,
        risk_score + 1,
        ROUND(churn_probability * 1.1, 3),
        CASE
            WHEN risk_score >= 7
            THEN 'URGENT — call today'
            ELSE recommended_action
        END,
        datetime('now')
    FROM customer_scores
    WHERE risk_score >= 6
""")
upserted = cursor.rowcount
conn.commit()
print(f"   ✅ UPSERT updated: {upserted} "
      f"high-risk scores refreshed")

run_query("""
    SELECT customerID, risk_score,
           churn_probability,
           recommended_action
    FROM customer_scores
    ORDER BY risk_score DESC
    LIMIT 8
""", "Top risk customers after UPSERT")

# ─────────────────────────────────────────────
# STEP 7: DELETE — Remove records
# ─────────────────────────────────────────────
print("\n📌 7. DELETE — REMOVING RECORDS")
logger.info("Testing DELETE operations")

before = count_table('customers')

cursor.execute("""
    DELETE FROM customers
    WHERE customerID LIKE 'NEW%'
""")
deleted = cursor.rowcount
conn.commit()

after = count_table('customers')
print(f"   Before: {before:,}")
print(f"   After:  {after:,}")
print(f"   ✅ Deleted {deleted} test records")
logger.info(
    f"Deleted {deleted} test records")

# ─────────────────────────────────────────────
# STEP 8: TRANSACTION CONTROL
# ─────────────────────────────────────────────
print("\n📌 8. TRANSACTION CONTROL")
print("   All or nothing — COMMIT or ROLLBACK!")

try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute("""
        UPDATE customers
        SET MonthlyCharges = MonthlyCharges
            * 1.05
        WHERE Contract = 'Month-to-month'
        AND Churn = 0
        AND tenure <= 6
    """)
    updated = cursor.rowcount
    print(f"\n   Simulated price increase: "
          f"{updated} records updated")

    avg_new = pd.read_sql_query("""
        SELECT ROUND(AVG(MonthlyCharges), 2)
            as new_avg
        FROM customers
        WHERE Contract = 'Month-to-month'
        AND tenure <= 6
    """, conn).iloc[0]['new_avg']
    print(f"   New avg charge: ${avg_new}")

    # ROLLBACK — we don't want to keep this!
    conn.rollback()
    print(f"\n   ✅ ROLLBACK executed — "
          f"changes reverted!")

    avg_original = pd.read_sql_query("""
        SELECT ROUND(AVG(MonthlyCharges), 2)
            as orig_avg
        FROM customers
        WHERE Contract = 'Month-to-month'
        AND tenure <= 6
    """, conn).iloc[0]['orig_avg']
    print(f"   Original avg restored: "
          f"${avg_original}")

except Exception as e:
    conn.rollback()
    logger.error(f"Transaction failed: {e}")
    print(f"   ❌ Transaction failed: {e}")

conn.close()
logger.info("Module 3 ETL complete")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   Staging tables  → Load raw first")
print("   INSERT          → Add new records")
print("   Validate first  → Check before load")
print("   UPDATE          → Modify existing")
print("   UPSERT          → Insert or update")
print("   DELETE          → Remove records")
print("   TRANSACTION     → All or nothing")
print("   ROLLBACK        → Undo changes")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 complete")