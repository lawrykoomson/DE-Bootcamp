# ============================================
# WEEK 4 — MODULE 6 CAPSTONE:
# Complete SQL Analysis Pipeline
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import json
import logging
import os
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Telco SQL Analysis Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"

DB_PATH = "../data/telco.db"
OUTPUT_DIR = Path("../data")
LOGS_DIR = Path("../logs")
CHARTS_DIR = Path("../data/charts")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOGS_DIR / 'capstone.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONNECT
# ─────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)

def run_query(sql, description=None):
    """Run SQL query and return DataFrame."""
    result = pd.read_sql_query(sql, conn)
    if description:
        logger.info(
            f"{description}: {len(result)} rows")
    return result

print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   Version: {PIPELINE_VERSION}")
print(f"   Author:  {AUTHOR}")
print("=" * 55)

# ─────────────────────────────────────────────
# ANALYSIS 1: EXECUTIVE SUMMARY
# ─────────────────────────────────────────────
print("\n📊 1. EXECUTIVE SUMMARY")
logger.info("Running executive summary")

exec_summary = run_query("""
    SELECT
        COUNT(*)                        as total_customers,
        SUM(Churn)                      as total_churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 2)                as churn_rate_pct,
        ROUND(
            AVG(MonthlyCharges), 2)     as avg_monthly_charge,
        ROUND(
            SUM(MonthlyCharges), 2)     as total_monthly_revenue,
        ROUND(
            SUM(CASE WHEN Churn = 1
                THEN MonthlyCharges
                ELSE 0 END), 2)         as revenue_at_risk,
        ROUND(
            AVG(tenure), 1)             as avg_tenure_months,
        ROUND(
            AVG(CASE WHEN Churn = 0
                THEN tenure END), 1)    as avg_retained_tenure,
        ROUND(
            AVG(CASE WHEN Churn = 1
                THEN tenure END), 1)    as avg_churned_tenure
    FROM customers
""", "Executive summary")

for col in exec_summary.columns:
    val = exec_summary[col].iloc[0]
    if col in ['total_monthly_revenue',
               'revenue_at_risk']:
        print(f"   {col:<30} ${val:,.2f}")
    elif col in ['churn_rate_pct']:
        print(f"   {col:<30} {val}%")
    else:
        print(f"   {col:<30} {val:,}")

# ─────────────────────────────────────────────
# ANALYSIS 2: SEGMENT RISK MATRIX
# ─────────────────────────────────────────────
print("\n📊 2. SEGMENT RISK MATRIX")
logger.info("Building segment risk matrix")

risk_matrix = run_query("""
    WITH segments AS (
        SELECT
            Contract,
            InternetService,
            COUNT(*)                    as customers,
            SUM(Churn)                  as churned,
            ROUND(
                SUM(Churn) * 100.0 /
                COUNT(*), 1)            as churn_rate,
            ROUND(
                SUM(MonthlyCharges), 2) as segment_revenue,
            ROUND(
                SUM(CASE WHEN Churn = 1
                    THEN MonthlyCharges
                    ELSE 0 END), 2)     as revenue_at_risk
        FROM customers
        GROUP BY Contract, InternetService
    )
    SELECT *,
        CASE
            WHEN churn_rate >= 50
            THEN '🔴 Critical'
            WHEN churn_rate >= 30
            THEN '🟡 High Risk'
            WHEN churn_rate >= 15
            THEN '🟠 Medium Risk'
            ELSE '🟢 Low Risk'
        END                             as risk_label
    FROM segments
    ORDER BY churn_rate DESC
""", "Risk matrix")

print(risk_matrix[[
    'Contract', 'InternetService',
    'customers', 'churn_rate',
    'revenue_at_risk',
    'risk_label']].to_string(index=False))

# ─────────────────────────────────────────────
# ANALYSIS 3: CUSTOMER LIFETIME VALUE
# ─────────────────────────────────────────────
print("\n📊 3. CUSTOMER LIFETIME VALUE")
logger.info("Calculating CLV")

clv_analysis = run_query("""
    WITH clv_calc AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            MonthlyCharges,
            tenure,
            TotalCharges,
            Churn,
            ROUND(
                MonthlyCharges * tenure,
                2)                      as estimated_clv,
            NTILE(5) OVER (
                ORDER BY
                    MonthlyCharges * tenure
            )                           as clv_quintile
        FROM customers
    )
    SELECT
        clv_quintile,
        CASE clv_quintile
            WHEN 1 THEN 'Bronze'
            WHEN 2 THEN 'Silver'
            WHEN 3 THEN 'Gold'
            WHEN 4 THEN 'Platinum'
            WHEN 5 THEN 'Diamond'
        END                             as tier,
        COUNT(*)                        as customers,
        ROUND(
            MIN(estimated_clv), 2)      as min_clv,
        ROUND(
            MAX(estimated_clv), 2)      as max_clv,
        ROUND(
            AVG(estimated_clv), 2)      as avg_clv,
        SUM(Churn)                      as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)                as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges *
                CASE WHEN Churn = 1
                THEN 1 ELSE 0 END),
            2)                          as clv_at_risk
    FROM clv_calc
    GROUP BY clv_quintile
    ORDER BY clv_quintile
""", "CLV analysis")

print(clv_analysis[[
    'tier', 'customers',
    'avg_clv', 'churn_rate_pct',
    'clv_at_risk']].to_string(index=False))

# ─────────────────────────────────────────────
# ANALYSIS 4: RETENTION INTERVENTION TARGETS
# ─────────────────────────────────────────────
print("\n📊 4. RETENTION INTERVENTION TARGETS")
logger.info("Identifying intervention targets")

interventions = run_query("""
    WITH risk_scored AS (
        SELECT
            customerID,
            Contract,
            InternetService,
            PaymentMethod,
            MonthlyCharges,
            tenure,
            Churn,
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
                WHEN tenure <= 12  THEN 3
                WHEN tenure <= 24  THEN 2
                WHEN tenure <= 48  THEN 1
                ELSE 0
            END                         as risk_score,
            ROUND(
                MonthlyCharges * tenure,
                2)                      as current_clv,
            ROUND(
                MonthlyCharges * 24,
                2)                      as potential_2yr_value
        FROM customers
        WHERE Churn = 0
    )
    SELECT
        customerID,
        Contract,
        InternetService,
        PaymentMethod,
        MonthlyCharges,
        tenure,
        risk_score,
        current_clv,
        potential_2yr_value,
        CASE
            WHEN risk_score >= 7
            THEN 'Immediate call + discount offer'
            WHEN risk_score >= 5
            THEN 'Email campaign + contract upgrade'
            WHEN risk_score >= 3
            THEN 'Auto-pay migration offer'
            ELSE 'Standard retention programme'
        END                             as recommended_action
    FROM risk_scored
    WHERE risk_score >= 6
    ORDER BY MonthlyCharges DESC
    LIMIT 15
""", "Intervention targets")

print(interventions[[
    'customerID', 'Contract',
    'MonthlyCharges', 'tenure',
    'risk_score',
    'recommended_action']].to_string(
    index=False))

# ─────────────────────────────────────────────
# ANALYSIS 5: MONTHLY CHURN TREND SIMULATION
# ─────────────────────────────────────────────
print("\n📊 5. CHURN TREND BY TENURE")
logger.info("Churn trend analysis")

churn_trend = run_query("""
    WITH monthly_stats AS (
        SELECT
            tenure,
            COUNT(*)                    as customers,
            SUM(Churn)                  as churned,
            ROUND(
                SUM(Churn) * 100.0 /
                COUNT(*), 1)            as churn_rate,
            ROUND(
                AVG(MonthlyCharges), 2) as avg_charge
        FROM customers
        GROUP BY tenure
    )
    SELECT
        tenure,
        customers,
        churned,
        churn_rate,
        avg_charge,
        SUM(customers) OVER (
            ORDER BY tenure
        )                               as cumulative_customers,
        ROUND(
            AVG(churn_rate) OVER (
                ORDER BY tenure
                ROWS BETWEEN 2 PRECEDING
                AND CURRENT ROW
            ), 1)                       as moving_avg_churn
    FROM monthly_stats
    WHERE tenure <= 36
    ORDER BY tenure
""", "Churn trend")

print(churn_trend[[
    'tenure', 'customers',
    'churn_rate',
    'moving_avg_churn']].to_string(
    index=False))

# ─────────────────────────────────────────────
# ANALYSIS 6: VISUALISATIONS
# ─────────────────────────────────────────────
print("\n📊 6. GENERATING VISUALISATIONS")
logger.info("Generating charts")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(
    f'Telco SQL Analysis Dashboard\n'
    f'{PIPELINE_NAME}',
    fontsize=14, fontweight='bold')

RED = '#E74C3C'
GREEN = '#27AE60'
BLUE = '#2980B9'
ORANGE = '#E67E22'

# Chart 1 — Risk Matrix
risk_pivot = risk_matrix.pivot_table(
    values='churn_rate',
    index='Contract',
    columns='InternetService',
    aggfunc='mean').fillna(0)
im = axes[0,0].imshow(
    risk_pivot.values,
    cmap='RdYlGn_r', aspect='auto')
axes[0,0].set_xticks(
    range(len(risk_pivot.columns)))
axes[0,0].set_xticklabels(
    risk_pivot.columns, rotation=15)
axes[0,0].set_yticks(
    range(len(risk_pivot.index)))
axes[0,0].set_yticklabels(risk_pivot.index)
axes[0,0].set_title(
    'Churn Rate Heatmap\n(Contract × Internet)',
    fontweight='bold')
for i in range(len(risk_pivot.index)):
    for j in range(len(risk_pivot.columns)):
        val = risk_pivot.values[i, j]
        axes[0,0].text(
            j, i, f'{val:.0f}%',
            ha='center', va='center',
            fontweight='bold', fontsize=11)
plt.colorbar(im, ax=axes[0,0])

# Chart 2 — CLV by Tier
clv_chart = clv_analysis.set_index('tier')
colors2 = ['#CD7F32', '#C0C0C0',
           '#FFD700', '#E5E4E2', '#B9F2FF']
bars = axes[0,1].bar(
    clv_chart.index,
    clv_chart['avg_clv'],
    color=colors2, edgecolor='white')
axes[0,1].set_title(
    'Avg Customer Lifetime Value by Tier',
    fontweight='bold')
axes[0,1].set_ylabel('Avg CLV ($)')
for bar, val in zip(
        bars, clv_chart['avg_clv']):
    axes[0,1].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 20,
        f'${val:,.0f}',
        ha='center', fontweight='bold',
        fontsize=9)

# Chart 3 — Churn rate by tenure
axes[1,0].plot(
    churn_trend['tenure'],
    churn_trend['churn_rate'],
    color=RED, linewidth=2,
    alpha=0.5, label='Monthly churn %')
axes[1,0].plot(
    churn_trend['tenure'],
    churn_trend['moving_avg_churn'],
    color=BLUE, linewidth=3,
    label='3-month moving avg')
axes[1,0].axhline(
    y=26.5, color='black',
    linestyle='--', alpha=0.4,
    label='Overall avg 26.5%')
axes[1,0].set_title(
    'Churn Rate by Tenure Month\n(with moving average)',
    fontweight='bold')
axes[1,0].set_xlabel('Tenure (months)')
axes[1,0].set_ylabel('Churn Rate (%)')
axes[1,0].legend(fontsize=9)
axes[1,0].fill_between(
    churn_trend['tenure'],
    churn_trend['moving_avg_churn'],
    26.5, alpha=0.15, color=RED)

# Chart 4 — Revenue at risk by segment
top_risk = risk_matrix.nlargest(
    6, 'revenue_at_risk')
labels = [
    f"{r['Contract'][:3]}\n{r['InternetService'][:3]}"
    for _, r in top_risk.iterrows()]
axes[1,1].barh(
    labels,
    top_risk['revenue_at_risk'],
    color=[RED, RED, ORANGE, ORANGE,
           ORANGE, GREEN],
    edgecolor='white')
axes[1,1].set_title(
    'Monthly Revenue at Risk\nby Segment',
    fontweight='bold')
axes[1,1].set_xlabel('Revenue at Risk ($)')
for i, val in enumerate(
        top_risk['revenue_at_risk']):
    axes[1,1].text(
        val + 100, i,
        f'${val:,.0f}',
        va='center', fontsize=9,
        fontweight='bold')

plt.tight_layout()
chart_path = CHARTS_DIR / 'sql_capstone_dashboard.png'
plt.savefig(
    chart_path, dpi=150,
    bbox_inches='tight')
plt.close()
print(f"   ✅ Chart saved: {chart_path}")
logger.info(f"Chart saved: {chart_path}")

# ─────────────────────────────────────────────
# ANALYSIS 7: SAVE ALL OUTPUTS
# ─────────────────────────────────────────────
print("\n📊 7. SAVING ALL OUTPUTS")

risk_matrix.to_csv(
    OUTPUT_DIR / 'risk_matrix.csv',
    index=False)
clv_analysis.to_csv(
    OUTPUT_DIR / 'clv_analysis.csv',
    index=False)
interventions.to_csv(
    OUTPUT_DIR / 'intervention_targets.csv',
    index=False)

report = {
    "pipeline_name": PIPELINE_NAME,
    "version": PIPELINE_VERSION,
    "author": AUTHOR,
    "run_timestamp": datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"),
    "executive_summary": {
        "total_customers": int(
            exec_summary[
                'total_customers'].iloc[0]),
        "churn_rate_pct": float(
            exec_summary[
                'churn_rate_pct'].iloc[0]),
        "total_monthly_revenue": float(
            exec_summary[
                'total_monthly_revenue'].iloc[0]),
        "revenue_at_risk": float(
            exec_summary[
                'revenue_at_risk'].iloc[0]),
        "avg_tenure_months": float(
            exec_summary[
                'avg_tenure_months'].iloc[0])
    },
    "top_risk_segment": {
        "contract": str(
            risk_matrix.iloc[0]['Contract']),
        "internet": str(
            risk_matrix.iloc[0][
                'InternetService']),
        "churn_rate": float(
            risk_matrix.iloc[0]['churn_rate']),
        "revenue_at_risk": float(
            risk_matrix.iloc[0][
                'revenue_at_risk'])
    },
    "intervention_targets": len(interventions),
    "sql_modules_completed": [
        "Module 1: SELECT WHERE ORDER BY LIMIT",
        "Module 2: GROUP BY COUNT SUM AVG HAVING",
        "Module 3: INNER LEFT MULTI-TABLE JOINS",
        "Module 4: SUBQUERIES CTEs RISK SCORING",
        "Module 5: WINDOW FUNCTIONS RANKING",
        "Module 6: CAPSTONE FULL SQL PIPELINE"
    ]
}

with open(
        OUTPUT_DIR / 'capstone_report.json',
        'w', encoding='utf-8') as f:
    json.dump(report, f, indent=4)

print(f"   ✅ risk_matrix.csv")
print(f"   ✅ clv_analysis.csv")
print(f"   ✅ intervention_targets.csv")
print(f"   ✅ capstone_report.json")
print(f"   ✅ sql_capstone_dashboard.png")

conn.close()
logger.info("SQL Capstone pipeline complete")

print("\n" + "=" * 55)
print("   CAPSTONE RESULTS SUMMARY")
print("=" * 55)
print(f"\n   Pipeline:       {PIPELINE_NAME}")
print(f"   Status:         ✅ SUCCESS")
print(f"   Customers:      "
      f"{exec_summary['total_customers'].iloc[0]:,}")
print(f"   Churn Rate:     "
      f"{exec_summary['churn_rate_pct'].iloc[0]}%")
print(f"   Revenue:        "
      f"${exec_summary['total_monthly_revenue'].iloc[0]:,.2f}/month")
print(f"   At Risk:        "
      f"${exec_summary['revenue_at_risk'].iloc[0]:,.2f}/month")
print(f"   Interventions:  {len(interventions)} customers")
print(f"   Risk Segments:  {len(risk_matrix)} identified")
print(f"\n✅ Week 4 SQL Capstone Complete!")
print(f"   SQL Analysis Pipeline — "
      f"Production Ready!")
logger.info("Week 4 complete!")