# ============================================
# WEEK 3 — BIGQUERY MODULE 3: Advanced Queries
# Parameterised Queries, JOINs, Aggregations
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import (
    ScalarQueryParameter,
    QueryJobConfig)

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

PROJECT_ID = "de-bootcamp-sandbox"
DATASET_ID = "telco_de_bootcamp"
TABLE_ID = (
    f"{PROJECT_ID}.{DATASET_ID}"
    f".telco_customers")

client = bigquery.Client(
    project=PROJECT_ID)

print("=" * 55)
print("   MODULE 3 — ADVANCED BQ QUERIES")
print("   PARAMETERISED | JOINS | WINDOW")
print("=" * 55)

# ─────────────────────────────────────────────
# HELPER FUNCTION
# ─────────────────────────────────────────────
def run_query(sql, label,
              params=None):
    """Run a BQ query and print results."""
    logger.info(f"Query: {label}")
    config = QueryJobConfig(
        query_parameters=params or [])
    job = client.query(
        sql, job_config=config)
    df = job.to_dataframe()
    logger.info(
        f"'{label}': {len(df)} rows")
    return df

# ─────────────────────────────────────────────
# STEP 1: PARAMETERISED QUERIES
# ─────────────────────────────────────────────
print("\n📌 1. PARAMETERISED QUERIES")
print("""
   Parameterised queries are SAFER
   than string formatting!

   String format (DANGEROUS):
   f"WHERE Contract = '{contract}'"
   → SQL injection risk!

   Parameterised (SAFE):
   WHERE Contract = @contract
   → BigQuery escapes values safely!
""")
logger.info("Running parameterised queries")

# Parameterised query 1
print("   🔍 Customers by contract type "
      "(parameterised):")

for contract_type in [
        "Month-to-month",
        "One year",
        "Two year"]:

    df = run_query(
        f"""
        SELECT
            @contract as contract_type,
            COUNT(*) as total,
            COUNTIF(Churn = TRUE) as churned,
            ROUND(
                COUNTIF(Churn = TRUE) *
                100.0 / COUNT(*), 1)
                as churn_rate_pct,
            ROUND(
                AVG(MonthlyCharges), 2)
                as avg_monthly
        FROM `{TABLE_ID}`
        WHERE Contract = @contract
        """,
        f"Contract={contract_type}",
        params=[
            ScalarQueryParameter(
                "contract", "STRING",
                contract_type)
        ]
    )
    row = df.iloc[0]
    print(f"   {row['contract_type']:<20} "
          f"Total:{row['total']:>5,} "
          f"Churn:{row['churn_rate_pct']:>5.1f}% "
          f"Avg:${row['avg_monthly']:>7.2f}")

# Parameterised query 2 — with number param
print("\n   🔍 High-value customers "
      "(parameterised threshold):")

for threshold in [50, 80, 100]:
    df = run_query(
        f"""
        SELECT
            @threshold as min_charge,
            COUNT(*) as customers,
            COUNTIF(Churn = TRUE) as churned,
            ROUND(
                SUM(MonthlyCharges), 2)
                as total_revenue
        FROM `{TABLE_ID}`
        WHERE MonthlyCharges >= @threshold
        AND Churn = FALSE
        """,
        f"Threshold={threshold}",
        params=[
            ScalarQueryParameter(
                "threshold", "FLOAT64",
                float(threshold))
        ]
    )
    row = df.iloc[0]
    print(f"   ${threshold}+/mo: "
          f"{row['customers']:>5,} retained "
          f"customers — "
          f"${row['total_revenue']:>10,.2f} "
          f"revenue")

logger.info(
    "Parameterised queries complete")

# ─────────────────────────────────────────────
# STEP 2: CREATE A SECOND TABLE TO JOIN
# ─────────────────────────────────────────────
print("\n📌 2. CREATING LOOKUP TABLE FOR JOIN")
logger.info("Creating contract lookup table")

CONTRACT_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}"
    f".contract_lookup")

# Create lookup table via SQL
client.query(f"""
    CREATE OR REPLACE TABLE
    `{CONTRACT_TABLE}` AS
    SELECT
        'Month-to-month' as Contract,
        'Flexible' as PlanType,
        0 as DiscountPct,
        'High' as ChurnRisk
    UNION ALL SELECT
        'One year', 'Annual',
        10, 'Medium'
    UNION ALL SELECT
        'Two year', 'Biennial',
        20, 'Low'
""").result()

print(f"   ✅ Lookup table created: "
      f"contract_lookup")
logger.info("Lookup table created")

# ─────────────────────────────────────────────
# STEP 3: JOIN IN BIGQUERY
# ─────────────────────────────────────────────
print("\n📌 3. JOINING TABLES IN BIGQUERY")
logger.info("Running JOIN queries")

print("\n   🔍 Customers with plan details "
      "(JOIN):")
df_join = run_query(f"""
    SELECT
        c.Contract,
        l.PlanType,
        l.DiscountPct,
        l.ChurnRisk,
        COUNT(*) as customers,
        COUNTIF(c.Churn = TRUE) as churned,
        ROUND(
            COUNTIF(c.Churn = TRUE) *
            100.0 / COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            SUM(c.MonthlyCharges), 2)
            as total_revenue
    FROM `{TABLE_ID}` c
    LEFT JOIN `{CONTRACT_TABLE}` l
        ON c.Contract = l.Contract
    GROUP BY
        c.Contract, l.PlanType,
        l.DiscountPct, l.ChurnRisk
    ORDER BY churn_rate_pct DESC
""", "JOIN customers with lookup")

print(df_join.to_string(index=False))
logger.info(
    f"JOIN result: {len(df_join)} rows")

# ─────────────────────────────────────────────
# STEP 4: CTEs IN BIGQUERY
# ─────────────────────────────────────────────
print("\n📌 4. CTEs IN BIGQUERY")
logger.info("Running CTE query")

print("\n   🔍 Risk segmentation with CTE:")
df_cte = run_query(f"""
    WITH risk_scores AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            MonthlyCharges,
            tenure,
            Churn,
            CASE Contract
                WHEN 'Month-to-month'
                THEN 3
                WHEN 'One year' THEN 1
                ELSE 0
            END +
            CASE
                WHEN tenure <= 12 THEN 3
                WHEN tenure <= 24 THEN 2
                WHEN tenure <= 48 THEN 1
                ELSE 0
            END as risk_score
        FROM `{TABLE_ID}`
    ),
    risk_labels AS (
        SELECT *,
            CASE
                WHEN risk_score >= 5
                THEN 'Critical'
                WHEN risk_score >= 3
                THEN 'High'
                WHEN risk_score >= 1
                THEN 'Medium'
                ELSE 'Low'
            END as risk_label
        FROM risk_scores
    )
    SELECT
        risk_label,
        COUNT(*) as customers,
        COUNTIF(Churn = TRUE) as churned,
        ROUND(
            COUNTIF(Churn = TRUE) *
            100.0 / COUNT(*), 1)
            as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly
    FROM risk_labels
    GROUP BY risk_label
    ORDER BY avg_monthly DESC
""", "Risk segmentation CTE")

print(df_cte.to_string(index=False))
logger.info(
    f"CTE result: {len(df_cte)} rows")

# ─────────────────────────────────────────────
# STEP 5: WINDOW FUNCTIONS IN BIGQUERY
# ─────────────────────────────────────────────
print("\n📌 5. WINDOW FUNCTIONS IN BIGQUERY")
logger.info("Running window functions")

print("\n   🔍 Top 3 spenders per contract:")
df_window = run_query(f"""
    SELECT *
    FROM (
        SELECT
            Contract,
            customerID,
            MonthlyCharges,
            tenure,
            RANK() OVER (
                PARTITION BY Contract
                ORDER BY
                    MonthlyCharges DESC
            ) as spend_rank
        FROM `{TABLE_ID}`
        WHERE Churn = FALSE
    )
    WHERE spend_rank <= 3
    ORDER BY Contract, spend_rank
""", "Window function rank")

print(df_window.to_string(index=False))
logger.info(
    f"Window result: "
    f"{len(df_window)} rows")

# ─────────────────────────────────────────────
# STEP 6: BIGQUERY-SPECIFIC FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 6. BIGQUERY-SPECIFIC FUNCTIONS")
logger.info("BQ-specific functions")

print("\n   🔍 BQ-specific aggregations:")
df_bq = run_query(f"""
    SELECT
        Contract,
        COUNT(*) as total,
        COUNTIF(Churn = TRUE)
            as churned,
        COUNTIF(Churn = FALSE)
            as retained,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly,
        ROUND(
            MAX(MonthlyCharges), 2)
            as max_monthly,
        ROUND(
            MIN(MonthlyCharges), 2)
            as min_monthly,
        ROUND(
            STDDEV(MonthlyCharges), 2)
            as stddev_monthly
    FROM `{TABLE_ID}`
    GROUP BY Contract
    ORDER BY total DESC
""", "BQ aggregation functions")

print(df_bq.to_string(index=False))
logger.info(
    f"BQ functions: {len(df_bq)} rows")

# ─────────────────────────────────────────────
# STEP 7: SAVE RESULTS
# ─────────────────────────────────────────────
print("\n📌 7. SAVING QUERY RESULTS")
logger.info("Saving results")

df_join.to_csv(
    "../data/bq_join_analysis.csv",
    index=False)
df_cte.to_csv(
    "../data/bq_risk_segments.csv",
    index=False)
df_window.to_csv(
    "../data/bq_top_customers.csv",
    index=False)

print("   ✅ bq_join_analysis.csv")
print("   ✅ bq_risk_segments.csv")
print("   ✅ bq_top_customers.csv")
logger.info("All results saved")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   @parameter      → Safe parameterised")
print("   ScalarQueryParam→ Define param type")
print("   JOIN            → Combine BQ tables")
print("   CTE (WITH)      → Step-by-step SQL")
print("   WINDOW RANK()   → Rank within groups")
print("   COUNTIF()       → BQ conditional count")
print("   STDDEV()        → Standard deviation")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 complete")