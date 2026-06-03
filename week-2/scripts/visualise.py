# ============================================
# WEEK 2 — MODULE 5: Visualisations & Charts
# GetSkills Network DE Bootcamp
# ============================================

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import logging
import os
from pathlib import Path

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data/charts", exist_ok=True)

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

CLEAN_DATA = Path(__file__).parent.parent / \
    "data/processed/telco_clean.csv"

print("=" * 55)
print("   MODULE 5 — VISUALISATIONS & CHARTS")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────
logger.info("Loading clean Telco dataset")
df = pd.read_csv(CLEAN_DATA)
print(f"\n   Loaded: {df.shape[0]:,} rows x "
      f"{df.shape[1]} columns")

# Colors
CHURN_COLOR = '#E74C3C'
RETAIN_COLOR = '#27AE60'
BLUE = '#2980B9'
ORANGE = '#E67E22'
PURPLE = '#8E44AD'
GOLD = '#F39C12'

# ─────────────────────────────────────────────
# STEP 2: DASHBOARD — 6 CHARTS
# ─────────────────────────────────────────────
print("\n📌 Building 6-chart dashboard...")

fig, axes = plt.subplots(3, 2, figsize=(16, 18))
fig.suptitle(
    'Telco Customer Churn Analysis Dashboard',
    fontsize=18, fontweight='bold', y=1.01)

# --- Chart 1: Churn Distribution (Pie) ---
churn_counts = df['Churn'].map(
    {True: 'Churned', False: 'Retained'}
).value_counts()
axes[0,0].pie(
    churn_counts.values,
    labels=churn_counts.index,
    autopct='%1.1f%%',
    colors=[CHURN_COLOR, RETAIN_COLOR],
    startangle=90,
    wedgeprops={
        'edgecolor': 'white', 'linewidth': 2})
axes[0,0].set_title(
    'Overall Churn Distribution',
    fontweight='bold', fontsize=13)

# --- Chart 2: Churn by Contract (Bar) ---
contract_churn = df.groupby('Contract')[
    'Churn'].mean() * 100
contract_churn = contract_churn.sort_values(
    ascending=False)
colors2 = [CHURN_COLOR if v > 30
           else ORANGE if v > 10
           else RETAIN_COLOR
           for v in contract_churn.values]
bars2 = axes[0,1].bar(
    contract_churn.index,
    contract_churn.values,
    color=colors2, edgecolor='white', width=0.5)
axes[0,1].set_title(
    'Churn Rate by Contract Type (%)',
    fontweight='bold', fontsize=13)
axes[0,1].set_ylabel('Churn Rate (%)')
axes[0,1].axhline(
    y=26.5, color='black',
    linestyle='--', alpha=0.5,
    label='Overall 26.5%')
axes[0,1].legend()
for bar, val in zip(
        bars2, contract_churn.values):
    axes[0,1].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 0.5,
        f'{val:.1f}%', ha='center',
        fontweight='bold', fontsize=11)

# --- Chart 3: Churn by Tenure Group (Bar) ---
tenure_order = [
    '0-12 months', '13-24 months',
    '25-48 months', '49+ months']
tenure_churn = df.groupby(
    'tenure_group')['Churn'].mean() * 100
tenure_churn = tenure_churn.reindex(tenure_order)
colors3 = [CHURN_COLOR if v > 30
           else ORANGE if v > 15
           else RETAIN_COLOR
           for v in tenure_churn.values]
bars3 = axes[1,0].bar(
    tenure_churn.index,
    tenure_churn.values,
    color=colors3, edgecolor='white', width=0.5)
axes[1,0].set_title(
    'Churn Rate by Tenure Group (%)',
    fontweight='bold', fontsize=13)
axes[1,0].set_ylabel('Churn Rate (%)')
axes[1,0].tick_params(
    axis='x', rotation=15)
for bar, val in zip(
        bars3, tenure_churn.values):
    axes[1,0].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 0.5,
        f'{val:.1f}%', ha='center',
        fontweight='bold', fontsize=11)

# --- Chart 4: Monthly Charges Distribution ---
churned_charges = df[
    df['Churn'] == True]['MonthlyCharges']
retained_charges = df[
    df['Churn'] == False]['MonthlyCharges']
axes[1,1].hist(
    retained_charges, bins=30,
    alpha=0.6, color=RETAIN_COLOR,
    label='Retained', edgecolor='white')
axes[1,1].hist(
    churned_charges, bins=30,
    alpha=0.6, color=CHURN_COLOR,
    label='Churned', edgecolor='white')
axes[1,1].set_title(
    'Monthly Charges: Churned vs Retained',
    fontweight='bold', fontsize=13)
axes[1,1].set_xlabel('Monthly Charges ($)')
axes[1,1].set_ylabel('Number of Customers')
axes[1,1].legend()
axes[1,1].axvline(
    x=churned_charges.mean(),
    color=CHURN_COLOR, linestyle='--',
    label=f'Churned avg: '
          f'${churned_charges.mean():.0f}')
axes[1,1].axvline(
    x=retained_charges.mean(),
    color=RETAIN_COLOR, linestyle='--')

# --- Chart 5: Churn by Internet Service ---
internet_churn = df.groupby(
    'InternetService')['Churn'].mean() * 100
internet_churn = internet_churn.sort_values(
    ascending=False)
colors5 = [CHURN_COLOR if v > 30
           else ORANGE if v > 15
           else RETAIN_COLOR
           for v in internet_churn.values]
bars5 = axes[2,0].bar(
    internet_churn.index,
    internet_churn.values,
    color=colors5, edgecolor='white', width=0.4)
axes[2,0].set_title(
    'Churn Rate by Internet Service (%)',
    fontweight='bold', fontsize=13)
axes[2,0].set_ylabel('Churn Rate (%)')
for bar, val in zip(
        bars5, internet_churn.values):
    axes[2,0].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 0.5,
        f'{val:.1f}%', ha='center',
        fontweight='bold', fontsize=11)

# --- Chart 6: Revenue at Risk ---
revenue_data = {
    'Retained\nCustomers': df[
        df['Churn'] == False
    ]['MonthlyCharges'].sum(),
    'Revenue at\nRisk (Churned)': df[
        df['Churn'] == True
    ]['MonthlyCharges'].sum()
}
colors6 = [RETAIN_COLOR, CHURN_COLOR]
bars6 = axes[2,1].bar(
    revenue_data.keys(),
    revenue_data.values(),
    color=colors6, edgecolor='white', width=0.4)
axes[2,1].set_title(
    'Monthly Revenue: Retained vs At Risk',
    fontweight='bold', fontsize=13)
axes[2,1].set_ylabel('Monthly Revenue ($)')
for bar, val in zip(
        bars6, revenue_data.values()):
    axes[2,1].text(
        bar.get_x() + bar.get_width()/2,
        bar.get_height() + 1000,
        f'${val:,.0f}',
        ha='center', fontweight='bold',
        fontsize=11)

plt.tight_layout()
chart_path = Path("../data/charts/churn_dashboard.png")
plt.savefig(chart_path, dpi=150,
            bbox_inches='tight')
plt.close()
print(f"\n   ✅ Dashboard saved: {chart_path}")
logger.info(
    f"Dashboard saved to {chart_path}")

# ─────────────────────────────────────────────
# STEP 3: CORRELATION HEATMAP
# ─────────────────────────────────────────────
print("\n📌 Building correlation heatmap...")

fig2, ax = plt.subplots(figsize=(10, 8))

numeric_df = df[[
    'tenure', 'MonthlyCharges',
    'TotalCharges', 'Churn',
    'SeniorCitizen']].copy()
numeric_df['Churn'] = numeric_df[
    'Churn'].astype(int)
numeric_df['SeniorCitizen'] = numeric_df[
    'SeniorCitizen'].map({'Yes': 1, 'No': 0})

corr_matrix = numeric_df.corr().round(2)
sns.heatmap(
    corr_matrix,
    annot=True, fmt='.2f',
    cmap='RdYlGn', center=0,
    ax=ax, linewidths=0.5,
    annot_kws={'size': 12})
ax.set_title(
    'Correlation Matrix — Numeric Features',
    fontsize=14, fontweight='bold')

heatmap_path = Path(
    "../data/charts/correlation_heatmap.png")
plt.tight_layout()
plt.savefig(heatmap_path, dpi=150,
            bbox_inches='tight')
plt.close()
print(f"   ✅ Heatmap saved: {heatmap_path}")
logger.info(
    f"Heatmap saved to {heatmap_path}")

# ─────────────────────────────────────────────
# STEP 4: PRINT KEY CHART INSIGHTS
# ─────────────────────────────────────────────
print(f"\n📌 KEY VISUAL INSIGHTS")
print(f"   Month-to-month churn: "
      f"{contract_churn.iloc[0]:.1f}% 🔴")
print(f"   Two year churn:       "
      f"{contract_churn.iloc[-1]:.1f}% 🟢")
print(f"   New customer churn:   "
      f"{tenure_churn.iloc[0]:.1f}% 🔴")
print(f"   Loyal customer churn: "
      f"{tenure_churn.iloc[-1]:.1f}% 🟢")
print(f"   Fiber optic churn:    "
      f"{internet_churn.iloc[0]:.1f}% 🔴")
print(f"   Avg churned charge:   "
      f"${churned_charges.mean():.2f}")
print(f"   Avg retained charge:  "
      f"${retained_charges.mean():.2f}")

print(f"\n{'='*55}")
print("   MODULE 5 SUMMARY")
print(f"{'='*55}")
print("   plt.subplots()   → Multi-chart layout")
print("   .bar()           → Bar charts")
print("   .pie()           → Pie charts")
print("   .hist()          → Histograms")
print("   sns.heatmap()    → Correlation matrix")
print("   .savefig()       → Save to file")
print(f"\n✅ Module 5 Complete!")
logger.info("Module 5 visualisation complete")