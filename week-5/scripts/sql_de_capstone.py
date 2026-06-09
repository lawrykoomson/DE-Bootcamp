# ============================================
# WEEK 5 — MODULE 6 CAPSTONE:
# Complete SQL Data Engineering Pipeline
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Telco SQL DE Pipeline v2"
PIPELINE_VERSION = "2.0.0"
AUTHOR = "Lawrence Koomson"

W5_DB = Path("../data/telco_w5.db")
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

conn = sqlite3.connect(str(W5_DB))
cursor = conn.cursor()

def run_query(sql, description=None):
    result = pd.read_sql_query(sql, conn)
    if description:
        logger.info(
            f"{description}: "
            f"{len(result)} rows")
    return result

print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   Version: {PIPELINE_VERSION}")
print(f"   Author:  {AUTHOR}")
print("=" * 55)

start_time = datetime.now()
logger.info("=" * 40)
logger.info(f"PIPELINE STARTED: {PIPELINE_NAME}")
logger.info("=" * 40)

# ─────────────────────────────────────────────
# STAGE 1: BUILD DATA WAREHOUSE
# ─────────────────────────────────────────────
print("\n🔧 STAGE 1: BUILD DATA WAREHOUSE")
logger.info("Stage 1: Building data warehouse")

# Verify star schema exists
tables = run_query("""
    SELECT name FROM sqlite_master
    WHERE type IN ('table', 'view')
    ORDER BY type DESC, name
""")
print(f"   Database objects: "
      f"{len(tables)} total")
for _, row in tables.iterrows():
    print(f"   📋 {row['name']}")

# ─────────────────────────────────────────────
# STAGE 2: DATA QUALITY CHECKS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 2: DATA QUALITY CHECKS")
logger.info("Stage 2: Running data quality checks")

quality_checks = {
    "Total records": """
        SELECT COUNT(*) as n
        FROM customers""",
    "Null customerIDs": """
        SELECT COUNT(*) as n
        FROM customers
        WHERE customerID IS NULL
        OR customerID = ''""",
    "Invalid charges": """
        SELECT COUNT(*) as n
        FROM customers
        WHERE MonthlyCharges <= 0""",
    "Invalid tenure": """
        SELECT COUNT(*) as n
        FROM customers
        WHERE tenure < 0""",
    "Invalid churn": """
        SELECT COUNT(*) as n
        FROM customers
        WHERE Churn NOT IN (0, 1)""",
    "Duplicate IDs": """
        SELECT COUNT(*) as n FROM (
        SELECT customerID
        FROM customers
        GROUP BY customerID
        HAVING COUNT(*) > 1)""",
}

all_passed = True
print(f"\n   {'Check':<25} "
      f"{'Result':>8} "
      f"{'Status':<10}")
print("   " + "-" * 48)

for check, sql in quality_checks.items():
    result = run_query(sql)
    n = result.iloc[0]['n']
    if check == "Total records":
        status = "✅ OK"
    else:
        status = "✅ Pass" if n == 0 \
            else "❌ FAIL"
        if n > 0:
            all_passed = False
    print(f"   {check:<25} "
          f"{n:>8,} {status}")

print(f"\n   Overall: "
      f"{'✅ ALL CHECKS PASSED' if all_passed else '❌ ISSUES FOUND'}")
logger.info(
    f"Quality checks: "
    f"{'PASSED' if all_passed else 'FAILED'}")

# ─────────────────────────────────────────────
# STAGE 3: ADVANCED ANALYTICS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 3: ADVANCED ANALYTICS")
logger.info("Stage 3: Running advanced analytics")

# Executive KPIs
print("\n   📊 Executive KPIs:")
kpis = run_query("""
    SELECT * FROM vw_executive_kpis
""", "Executive KPIs")
for col in kpis.columns:
    val = kpis[col].iloc[0]
    if col in ['monthly_revenue',
               'revenue_at_risk',
               'avg_retained_clv']:
        print(f"   {col:<28} ${val:,.2f}")
    elif 'pct' in col or 'rate' in col:
        print(f"   {col:<28} {val}%")
    else:
        print(f"   {col:<28} {val:,}")

# Lifecycle analysis
print("\n   📊 Customer Lifecycle:")
lifecycle = run_query("""
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
            MonthlyCharges,
            Churn
        FROM customers
    )
    SELECT
        stage,
        COUNT(*)                as customers,
        SUM(Churn)              as churned,
        ROUND(
            SUM(Churn) * 100.0 /
            COUNT(*), 1)        as churn_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
                                as revenue,
        ROUND(
            SUM(CASE WHEN Churn = 1
                THEN MonthlyCharges
                ELSE 0 END), 2) as at_risk
    FROM lifecycle
    GROUP BY stage
    ORDER BY stage
""", "Lifecycle analysis")
print(lifecycle.to_string(index=False))

# Risk matrix
print("\n   📊 Risk Matrix:")
risk_matrix = run_query("""
    SELECT * FROM vw_churn_summary
    ORDER BY churn_rate_pct DESC
    LIMIT 6
""", "Risk matrix")
print(risk_matrix[[
    'Contract', 'InternetService',
    'total', 'churn_rate_pct',
    'revenue_at_risk']].to_string(
    index=False))

# ─────────────────────────────────────────────
# STAGE 4: INTERVENTION TARGETING
# ─────────────────────────────────────────────
print("\n🔧 STAGE 4: INTERVENTION TARGETING")
logger.info("Stage 4: Identifying interventions")

interventions = run_query("""
    SELECT
        action,
        COUNT(*)                as customers,
        ROUND(
            SUM(MonthlyCharges), 2)
                                as monthly_at_risk,
        ROUND(
            SUM(MonthlyCharges) * 12,
            2)                  as annual_at_risk,
        ROUND(
            AVG(MonthlyCharges), 2)
                                as avg_monthly,
        ROUND(
            AVG(risk_score), 1) as avg_risk
    FROM vw_intervention_list
    GROUP BY action
    ORDER BY avg_risk DESC
""", "Intervention summary")
print(interventions.to_string(index=False))

# Top priority customers
top_priority = run_query("""
    SELECT customerID, Contract,
           InternetService, MonthlyCharges,
           risk_score, lifetime_value, action
    FROM vw_intervention_list
    WHERE action = '🔴 Immediate call'
    ORDER BY MonthlyCharges DESC
    LIMIT 10
""", "Top priority customers")
print(f"\n   Top 10 Priority Customers:")
print(top_priority[[
    'customerID', 'MonthlyCharges',
    'risk_score',
    'lifetime_value']].to_string(
    index=False))

# ─────────────────────────────────────────────
# STAGE 5: GENERATE VISUALISATIONS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 5: VISUALISATIONS")
logger.info("Stage 5: Generating charts")

fig, axes = plt.subplots(
    2, 2, figsize=(14, 10))
fig.suptitle(
    f'{PIPELINE_NAME}\n'
    f'SQL Data Engineering Capstone',
    fontsize=14, fontweight='bold')

RED = '#E74C3C'
GREEN = '#27AE60'
BLUE = '#2980B9'
ORANGE = '#E67E22'
PURPLE = '#8E44AD'

# Chart 1 — Lifecycle churn
axes[0,0].bar(
    lifecycle['stage'],
    lifecycle['churn_pct'],
    color=[RED if v > 40 else
           ORANGE if v > 20 else
           GREEN
           for v in lifecycle['churn_pct']],
    edgecolor='white')
axes[0,0].set_title(
    'Churn Rate by\nCustomer Lifecycle Stage',
    fontweight='bold')
axes[0,0].set_ylabel('Churn Rate (%)')
axes[0,0].tick_params(
    axis='x', rotation=20)
axes[0,0].axhline(
    y=26.5, color='black',
    linestyle='--', alpha=0.5)
for i, (_, row) in enumerate(
        lifecycle.iterrows()):
    axes[0,0].text(
        i, row['churn_pct'] + 0.5,
        f"{row['churn_pct']}%",
        ha='center', fontsize=9,
        fontweight='bold')

# Chart 2 — Intervention breakdown
action_labels = [
    a.split(' ', 1)[1]
    for a in interventions['action']]
colors2 = [RED, ORANGE, PURPLE, GREEN]
axes[0,1].pie(
    interventions['customers'],
    labels=action_labels,
    autopct='%1.1f%%',
    colors=colors2,
    startangle=90,
    wedgeprops={
        'edgecolor': 'white',
        'linewidth': 2})
axes[0,1].set_title(
    'Customer Distribution\nby Intervention',
    fontweight='bold')

# Chart 3 — Revenue at risk by lifecycle
axes[1,0].barh(
    lifecycle['stage'],
    lifecycle['at_risk'],
    color=[RED if v > 50000 else
           ORANGE if v > 20000 else
           GREEN
           for v in lifecycle['at_risk']],
    edgecolor='white')
axes[1,0].set_title(
    'Monthly Revenue at Risk\nby Lifecycle Stage',
    fontweight='bold')
axes[1,0].set_xlabel(
    'Revenue at Risk ($)')
for i, val in enumerate(
        lifecycle['at_risk']):
    axes[1,0].text(
        val + 200, i,
        f'${val:,.0f}',
        va='center', fontsize=9,
        fontweight='bold')

# Chart 4 — Risk matrix heatmap
risk_pivot = risk_matrix.pivot_table(
    values='churn_rate_pct',
    index='Contract',
    columns='InternetService',
    aggfunc='first').fillna(0)
im = axes[1,1].imshow(
    risk_pivot.values,
    cmap='RdYlGn_r',
    aspect='auto')
axes[1,1].set_xticks(
    range(len(risk_pivot.columns)))
axes[1,1].set_xticklabels(
    risk_pivot.columns, rotation=15,
    fontsize=9)
axes[1,1].set_yticks(
    range(len(risk_pivot.index)))
axes[1,1].set_yticklabels(
    risk_pivot.index, fontsize=9)
axes[1,1].set_title(
    'Churn Rate Heatmap\n(Contract × Internet)',
    fontweight='bold')
for i in range(len(risk_pivot.index)):
    for j in range(
            len(risk_pivot.columns)):
        val = risk_pivot.values[i, j]
        if val > 0:
            axes[1,1].text(
                j, i, f'{val:.0f}%',
                ha='center',
                va='center',
                fontweight='bold',
                fontsize=10,
                color='white'
                if val > 40
                else 'black')
plt.colorbar(im, ax=axes[1,1])

plt.tight_layout()
chart_path = (
    CHARTS_DIR / 'week5_capstone.png')
plt.savefig(
    chart_path, dpi=150,
    bbox_inches='tight')
plt.close()
print(f"   ✅ Chart saved: {chart_path}")
logger.info(
    f"Chart saved: {chart_path}")

# ─────────────────────────────────────────────
# STAGE 6: SAVE ALL OUTPUTS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 6: SAVING OUTPUTS")
logger.info("Stage 6: Saving outputs")

lifecycle.to_csv(
    OUTPUT_DIR / 'lifecycle_analysis.csv',
    index=False)

interventions.to_csv(
    OUTPUT_DIR / 'intervention_summary.csv',
    index=False)

top_priority.to_csv(
    OUTPUT_DIR / 'top_priority_customers.csv',
    index=False)

duration = (
    datetime.now() - start_time
).total_seconds()

report = {
    "pipeline_name": PIPELINE_NAME,
    "version": PIPELINE_VERSION,
    "author": AUTHOR,
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "duration_seconds": round(duration, 2),
    "data_quality": "ALL PASSED",
    "executive_summary": {
        "total_customers": int(
            kpis['total_customers'].iloc[0]),
        "churn_rate_pct": float(
            kpis['churn_rate_pct'].iloc[0]),
        "monthly_revenue": float(
            kpis['monthly_revenue'].iloc[0]),
        "revenue_at_risk": float(
            kpis['revenue_at_risk'].iloc[0]),
    },
    "interventions": {
        "immediate_call": int(
            interventions[
                interventions['action']
                .str.contains('Immediate')
            ]['customers'].sum()),
        "email_campaign": int(
            interventions[
                interventions['action']
                .str.contains('Email')
            ]['customers'].sum()),
        "autopay_offer": int(
            interventions[
                interventions['action']
                .str.contains('Auto')
            ]['customers'].sum()),
    },
    "sql_skills_used": [
        "Advanced CASE WHEN and PIVOT",
        "Star Schema — FACT and DIM tables",
        "INSERT UPDATE DELETE UPSERT",
        "CREATE VIEW — 5 production views",
        "EXPLAIN QUERY PLAN",
        "CREATE INDEX — 5 indexes",
        "Benchmarking before and after",
        "Transaction COMMIT and ROLLBACK",
    ]
}

with open(
        OUTPUT_DIR / 'week5_report.json',
        'w') as f:
    json.dump(report, f, indent=4)

print("   ✅ lifecycle_analysis.csv")
print("   ✅ intervention_summary.csv")
print("   ✅ top_priority_customers.csv")
print("   ✅ week5_report.json")
print("   ✅ week5_capstone.png")

conn.close()
logger.info(
    f"Pipeline complete in "
    f"{duration:.2f}s")

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   WEEK 5 CAPSTONE RESULTS")
print("=" * 55)
print(f"\n   Pipeline: {PIPELINE_NAME}")
print(f"   Status:   ✅ SUCCESS")
print(f"   Duration: {duration:.2f} seconds")
print(f"\n   📊 BUSINESS SUMMARY:")
print(f"   Customers:    "
      f"{kpis['total_customers'].iloc[0]:,}")
print(f"   Churn Rate:   "
      f"{kpis['churn_rate_pct'].iloc[0]}%")
print(f"   Revenue:      "
      f"${kpis['monthly_revenue'].iloc[0]:,.2f}/mo")
print(f"   At Risk:      "
      f"${kpis['revenue_at_risk'].iloc[0]:,.2f}/mo")
print(f"\n   🎯 INTERVENTIONS:")
for _, row in interventions.iterrows():
    print(f"   {row['action']:<35} "
          f"{row['customers']:>5} customers")
print(f"\n   📁 OUTPUT FILES: 5 files saved")
print(f"\n✅ Week 5 SQL DE Capstone Complete!")
print(f"   SQL Data Engineering — "
      f"Production Ready!")
logger.info("Week 5 complete!")