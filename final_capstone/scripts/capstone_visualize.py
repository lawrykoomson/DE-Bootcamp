# ============================================
# FINAL CAPSTONE — MODULE 4: VISUALIZATIONS
# Charts for executive presentation
# GetSkills Network DE Bootcamp
# Lawrence Koomson
# ============================================

import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from datetime import datetime

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../charts", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/visualize.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

plt.style.use('seaborn-v0_8-darkgrid')
COLORS = {
    'Critical': '#D32F2F',
    'High': '#F57C00',
    'Medium': '#FBC02D',
    'Low': '#388E3C'
}

print("=" * 55)
print("   FINAL CAPSTONE")
print("   MODULE 4 — VISUALIZATIONS")
print("=" * 55)

logger.info("Module 4: Visualizations started")

# ─────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING DATA FOR CHARTS")
logger.info("Loading transformed data")

df = pd.read_csv(
    "../data/telco_transformed.csv")

segment_df = pd.read_csv(
    "../reports/segment_analysis.csv")

intervention_df = pd.read_csv(
    "../reports/intervention_summary.csv")

print(f"   ✅ Loaded {len(df):,} rows "
      f"for visualization")
logger.info(f"Loaded {len(df):,} rows")

# ─────────────────────────────────────────────
# CHART 1: RISK DISTRIBUTION
# ─────────────────────────────────────────────
print("\n📌 2. CHART 1 — RISK DISTRIBUTION")
logger.info("Creating risk distribution chart")

risk_counts = df[
    'risk_label'].value_counts()
risk_order = [
    'Critical', 'High', 'Medium', 'Low']
risk_counts = risk_counts.reindex(
    risk_order)

fig, ax = plt.subplots(
    figsize=(9, 6))
bars = ax.bar(
    risk_counts.index,
    risk_counts.values,
    color=[COLORS[r] for r in
           risk_counts.index])

for bar in bars:
    height = bar.get_height()
    ax.annotate(
        f'{int(height):,}',
        xy=(bar.get_x() +
            bar.get_width()/2, height),
        xytext=(0, 5),
        textcoords="offset points",
        ha='center', fontweight='bold')

ax.set_title(
    'Customer Risk Distribution',
    fontsize=15, fontweight='bold')
ax.set_xlabel('Risk Level')
ax.set_ylabel('Number of Customers')
plt.tight_layout()
plt.savefig(
    '../charts/01_risk_distribution.png',
    dpi=150)
plt.close()
print(f"   ✅ Saved: "
      f"01_risk_distribution.png")
logger.info(
    "Risk distribution chart saved")

# ─────────────────────────────────────────────
# CHART 2: CHURN BY CONTRACT & INTERNET
# ─────────────────────────────────────────────
print("\n📌 3. CHART 2 — CHURN BY SEGMENT")
logger.info("Creating segment churn chart")

fig, ax = plt.subplots(
    figsize=(11, 6))
pivot = segment_df.pivot(
    index='InternetService',
    columns='Contract',
    values='churn_rate_pct')
pivot.plot(
    kind='bar', ax=ax,
    color=['#E53935', '#FB8C00',
           '#43A047'])

ax.set_title(
    'Churn Rate by Contract & '
    'Internet Service',
    fontsize=15, fontweight='bold')
ax.set_xlabel('Internet Service')
ax.set_ylabel('Churn Rate (%)')
ax.legend(title='Contract')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig(
    '../charts/02_churn_by_segment.png',
    dpi=150)
plt.close()
print(f"   ✅ Saved: "
      f"02_churn_by_segment.png")
logger.info(
    "Segment churn chart saved")

# ─────────────────────────────────────────────
# CHART 3: INTERVENTION REVENUE IMPACT
# ─────────────────────────────────────────────
print("\n📌 4. CHART 3 — INTERVENTION IMPACT")
logger.info("Creating intervention chart")

fig, ax = plt.subplots(
    figsize=(9, 6))
intervention_sorted = (
    intervention_df.sort_values(
        'revenue', ascending=True))
bars = ax.barh(
    intervention_sorted['action'],
    intervention_sorted['revenue'],
    color=['#388E3C', '#FBC02D',
           '#F57C00', '#D32F2F'])

for bar in bars:
    width = bar.get_width()
    ax.annotate(
        f'${width:,.0f}',
        xy=(width, bar.get_y() +
            bar.get_height()/2),
        xytext=(5, 0),
        textcoords="offset points",
        va='center', fontweight='bold')

ax.set_title(
    'Monthly Revenue by Intervention '
    'Category',
    fontsize=15, fontweight='bold')
ax.set_xlabel('Monthly Revenue ($)')
plt.tight_layout()
plt.savefig(
    '../charts/03_intervention_impact.png',
    dpi=150)
plt.close()
print(f"   ✅ Saved: "
      f"03_intervention_impact.png")
logger.info(
    "Intervention chart saved")

# ─────────────────────────────────────────────
# CHART 4: TENURE VS CHURN
# ─────────────────────────────────────────────
print("\n📌 5. CHART 4 — TENURE VS CHURN")
logger.info("Creating tenure chart")

tenure_bins = pd.cut(
    df['tenure'],
    bins=[0, 12, 24, 36, 48, 60, 72],
    labels=['0-12', '13-24', '25-36',
            '37-48', '49-60', '61-72'])
tenure_churn = df.groupby(
    tenure_bins, observed=True
)['Churn'].agg(
    ['mean', 'count'])
tenure_churn['churn_pct'] = (
    tenure_churn['mean'] * 100)

fig, ax1 = plt.subplots(
    figsize=(10, 6))
ax2 = ax1.twinx()

ax1.bar(
    tenure_churn.index.astype(str),
    tenure_churn['count'],
    alpha=0.3, color='steelblue',
    label='Customer Count')
ax2.plot(
    tenure_churn.index.astype(str),
    tenure_churn['churn_pct'],
    color='#D32F2F', marker='o',
    linewidth=3, markersize=8,
    label='Churn Rate')

ax1.set_xlabel('Tenure (months)')
ax1.set_ylabel(
    'Customer Count', color='steelblue')
ax2.set_ylabel(
    'Churn Rate (%)', color='#D32F2F')
ax1.set_title(
    'Customer Tenure vs Churn Rate',
    fontsize=15, fontweight='bold')

plt.tight_layout()
plt.savefig(
    '../charts/04_tenure_vs_churn.png',
    dpi=150)
plt.close()
print(f"   ✅ Saved: "
      f"04_tenure_vs_churn.png")
logger.info("Tenure chart saved")

# ─────────────────────────────────────────────
# CHART 5: REVENUE AT RISK SUMMARY
# ─────────────────────────────────────────────
print("\n📌 6. CHART 5 — REVENUE SUMMARY")
logger.info("Creating revenue summary chart")

total_revenue = df[
    'MonthlyCharges'].sum()
revenue_at_risk = df[
    df['Churn'] == True][
    'MonthlyCharges'].sum()
revenue_safe = (
    total_revenue - revenue_at_risk)

fig, ax = plt.subplots(
    figsize=(8, 8))
wedges, texts, autotexts = ax.pie(
    [revenue_safe, revenue_at_risk],
    labels=['Retained Revenue',
            'Revenue at Risk'],
    colors=['#388E3C', '#D32F2F'],
    autopct=lambda p: (
        f'${p*total_revenue/100:,.0f}\n'
        f'({p:.1f}%)'),
    startangle=90,
    explode=(0, 0.08))

for text in texts:
    text.set_fontsize(12)
    text.set_fontweight('bold')
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')

ax.set_title(
    f'Monthly Revenue Breakdown\n'
    f'Total: ${total_revenue:,.2f}',
    fontsize=15, fontweight='bold')
plt.tight_layout()
plt.savefig(
    '../charts/05_revenue_breakdown.png',
    dpi=150)
plt.close()
print(f"   ✅ Saved: "
      f"05_revenue_breakdown.png")
logger.info(
    "Revenue breakdown chart saved")

print(f"\n{'='*55}")
print("   MODULE 4 VISUALIZATION SUMMARY")
print(f"{'='*55}")
print("   01_risk_distribution.png")
print("   02_churn_by_segment.png")
print("   03_intervention_impact.png")
print("   04_tenure_vs_churn.png")
print("   05_revenue_breakdown.png")
print(f"\n   Total revenue:    "
      f"${total_revenue:,.2f}")
print(f"   Revenue at risk:  "
      f"${revenue_at_risk:,.2f}")
print(f"\n✅ Module 4 — Visualizations Complete!")
logger.info(
    "Module 4 visualizations complete")