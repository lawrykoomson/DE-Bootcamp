# ============================================
# WEEK 4 — MODULE 5: Window Functions
# ROW_NUMBER, RANK, LAG, LEAD, NTILE
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
            '../logs/module5.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = "../data/telco.db"
conn = sqlite3.connect(DB_PATH)

print("=" * 55)
print("   MODULE 5 — WINDOW FUNCTIONS")
print("   ROW_NUMBER | RANK | NTILE | AVG OVER")
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
# STEP 1: ROW_NUMBER
# ─────────────────────────────────────────────
print("\n📌 1. ROW_NUMBER()")
print("   Assigns sequential numbers to rows")

run_query("""
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY MonthlyCharges DESC
        )                       as rank_overall,
        customerID,
        Contract,
        MonthlyCharges,
        tenure,
        Churn
    FROM customers
    LIMIT 8
""", "Overall ranking by monthly charges")


# SQLite workaround for WHERE with window functions
run_query("""
    WITH ranked AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY Contract
                ORDER BY MonthlyCharges DESC
            )                   as rank_in_contract,
            customerID,
            Contract,
            MonthlyCharges,
            tenure,
            Churn
        FROM customers
    )
    SELECT *
    FROM ranked
    WHERE rank_in_contract <= 3
    ORDER BY Contract, rank_in_contract
""", "Top 3 customers per contract type (CTE method)")

# ─────────────────────────────────────────────
# STEP 2: RANK AND DENSE_RANK
# ─────────────────────────────────────────────
print("\n📌 2. RANK() AND DENSE_RANK()")

run_query("""
    WITH charge_ranks AS (
        SELECT
            customerID,
            Contract,
            MonthlyCharges,
            Churn,
            RANK() OVER (
                ORDER BY MonthlyCharges DESC
            )                   as rank_with_gaps,
            DENSE_RANK() OVER (
                ORDER BY MonthlyCharges DESC
            )                   as dense_rank_no_gaps
        FROM customers
    )
    SELECT *
    FROM charge_ranks
    LIMIT 10
""", "RANK vs DENSE_RANK comparison")

# ─────────────────────────────────────────────
# STEP 3: RUNNING TOTALS WITH SUM OVER
# ─────────────────────────────────────────────
print("\n📌 3. RUNNING TOTALS — SUM OVER")

run_query("""
    WITH contract_revenue AS (
        SELECT
            Contract,
            ROUND(
                SUM(MonthlyCharges), 2) as monthly_rev,
            COUNT(*)                    as customers
        FROM customers
        GROUP BY Contract
        ORDER BY monthly_rev DESC
    )
    SELECT
        Contract,
        customers,
        monthly_rev,
        ROUND(
            SUM(monthly_rev) OVER (
                ORDER BY monthly_rev DESC
            ), 2)               as running_total,
        ROUND(
            monthly_rev * 100.0 /
            SUM(monthly_rev) OVER (),
            1)                  as pct_of_total
    FROM contract_revenue
""", "Running total and % of total revenue by contract")

# ─────────────────────────────────────────────
# STEP 4: MOVING AVERAGE WITH AVG OVER
# ─────────────────────────────────────────────
print("\n📌 4. AVG OVER — COMPARE TO GROUP AVG")

run_query("""
    WITH contract_avgs AS (
        SELECT
            customerID,
            Contract,
            MonthlyCharges,
            tenure,
            Churn,
            ROUND(
                AVG(MonthlyCharges) OVER (
                    PARTITION BY Contract
                ), 2)           as avg_in_contract,
            ROUND(
                AVG(MonthlyCharges) OVER (),
                2)              as overall_avg
        FROM customers
    )
    SELECT
        customerID,
        Contract,
        MonthlyCharges,
        avg_in_contract,
        overall_avg,
        ROUND(
            MonthlyCharges - avg_in_contract,
            2)                  as vs_contract_avg,
        Churn
    FROM contract_avgs
    WHERE MonthlyCharges > avg_in_contract
    AND Churn = 1
    ORDER BY vs_contract_avg DESC
    LIMIT 8
""", "Churned customers above their contract average")

# ─────────────────────────────────────────────
# STEP 5: NTILE — PERCENTILE BUCKETS
# ─────────────────────────────────────────────
print("\n📌 5. NTILE() — PERCENTILE BUCKETS")

run_query("""
    WITH quartiles AS (
        SELECT
            customerID,
            Contract,
            MonthlyCharges,
            tenure,
            Churn,
            NTILE(4) OVER (
                ORDER BY MonthlyCharges
            )                   as charge_quartile
        FROM customers
    )
    SELECT
        charge_quartile,
        COUNT(*)                as customers,
        ROUND(
            MIN(MonthlyCharges), 2)
                                as min_charge,
        ROUND(
            MAX(MonthlyCharges), 2)
                                as max_charge,
        ROUND(
            AVG(MonthlyCharges), 2)
                                as avg_charge,
        SUM(Churn)              as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)        as churn_rate_pct
    FROM quartiles
    GROUP BY charge_quartile
    ORDER BY charge_quartile
""", "Churn rate by monthly charge quartile")

# ─────────────────────────────────────────────
# STEP 6: CUMULATIVE CHURN ANALYSIS
# ─────────────────────────────────────────────
print("\n📌 6. CUMULATIVE CHURN BY TENURE")

run_query("""
    WITH tenure_churn AS (
        SELECT
            tenure,
            COUNT(*)            as customers,
            SUM(Churn)          as churned
        FROM customers
        GROUP BY tenure
    )
    SELECT
        tenure,
        customers,
        churned,
        ROUND(
            churned * 100.0 /
            customers, 1)       as churn_rate_pct,
        SUM(customers) OVER (
            ORDER BY tenure
        )                       as cumulative_customers,
        SUM(churned) OVER (
            ORDER BY tenure
        )                       as cumulative_churned,
        ROUND(
            SUM(churned) OVER (
                ORDER BY tenure
            ) * 100.0 /
            SUM(customers) OVER (
                ORDER BY tenure
            ), 1)               as cumulative_churn_rate
    FROM tenure_churn
    WHERE tenure <= 24
    ORDER BY tenure
""", "Cumulative churn rate by tenure month")

# ─────────────────────────────────────────────
# STEP 7: SAVE WINDOW FUNCTION RESULTS
# ─────────────────────────────────────────────
print("\n📌 7. SAVING RANKED CUSTOMER TABLE")

ranked_customers = pd.read_sql_query("""
    WITH scored AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            PaymentMethod,
            MonthlyCharges,
            tenure,
            TotalCharges,
            Churn,
            RANK() OVER (
                ORDER BY MonthlyCharges DESC
            )                   as revenue_rank,
            NTILE(4) OVER (
                ORDER BY MonthlyCharges
            )                   as charge_quartile,
            ROUND(
                AVG(MonthlyCharges) OVER (
                    PARTITION BY Contract
                ), 2)           as contract_avg_charge,
            ROUND(
                MonthlyCharges -
                AVG(MonthlyCharges) OVER (
                    PARTITION BY Contract
                ), 2)           as vs_contract_avg
        FROM customers
    )
    SELECT *,
        CASE charge_quartile
            WHEN 1 THEN 'Budget'
            WHEN 2 THEN 'Standard'
            WHEN 3 THEN 'Premium'
            WHEN 4 THEN 'Enterprise'
        END                     as customer_tier
    FROM scored
    ORDER BY revenue_rank
""", conn)

ranked_customers.to_csv(
    "../data/customers_ranked.csv",
    index=False)

print(f"   ✅ Saved customers_ranked.csv")
print(f"   Rows:    {len(ranked_customers):,}")
print(f"   Columns: {len(ranked_customers.columns)}")

print(f"\n   Customer Tier Distribution:")
tier_summary = ranked_customers.groupby(
    'customer_tier').agg(
    Customers=('customerID', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Charge=('MonthlyCharges', 'mean')
).round(2)
tier_summary['Churn_Rate_%'] = (
    tier_summary['Churned'] /
    tier_summary['Customers'] * 100
).round(1)
print(tier_summary.to_string())

conn.close()
logger.info("Module 5 window functions complete")

print(f"\n{'='*55}")
print("   MODULE 5 SUMMARY")
print(f"{'='*55}")
print("   ROW_NUMBER()  → Sequential row numbers")
print("   RANK()        → Rank with gaps")
print("   DENSE_RANK()  → Rank without gaps")
print("   NTILE(n)      → Split into n buckets")
print("   SUM OVER()    → Running totals")
print("   AVG OVER()    → Group averages")
print("   PARTITION BY  → Reset window per group")
print("   ORDER BY      → Window ordering")
print(f"\n✅ Module 5 Complete!")
logger.info("Module 5 complete")