# ============================================
# WEEK 2 — MODULE 3: Filtering & Aggregation
# GetSkills Network DE Bootcamp
# ============================================

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

CLEAN_DATA = Path(__file__).parent.parent / \
    "data/processed/telco_clean.csv"

print("=" * 55)
print("   MODULE 3 — FILTERING & AGGREGATION")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD CLEAN DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING CLEAN DATA")
logger.info("Loading clean Telco dataset")
df = pd.read_csv(CLEAN_DATA)
print(f"   Loaded: {df.shape[0]:,} rows x "
      f"{df.shape[1]} columns")

# ─────────────────────────────────────────────
# STEP 2: BASIC FILTERING
# ─────────────────────────────────────────────
print("\n📌 2. BASIC FILTERING")

# Churned customers
churned = df[df['Churn'] == True]
retained = df[df['Churn'] == False]
print(f"   Churned customers:  {len(churned):,}")
print(f"   Retained customers: {len(retained):,}")

# High value churned customers
high_value_churned = df[
    (df['Churn'] == True) &
    (df['MonthlyCharges'] >= 90)]
print(f"   High value churned: "
      f"{len(high_value_churned):,} "
      f"(Premium customers lost!)")

# Month-to-month churned
mtm_churned = df[
    (df['Churn'] == True) &
    (df['Contract'] == 'Month-to-month')]
print(f"   Month-to-month churned: "
      f"{len(mtm_churned):,}")

# New customers (tenure <= 12)
new_customers = df[df['tenure'] <= 12]
print(f"   New customers (≤12 months): "
      f"{len(new_customers):,}")

logger.info(
    f"Filtering complete: {len(churned)} churned, "
    f"{len(retained)} retained")

# ─────────────────────────────────────────────
# STEP 3: CHURN BY CONTRACT TYPE
# ─────────────────────────────────────────────
print("\n📌 3. CHURN BY CONTRACT TYPE")

contract_churn = df.groupby('Contract').agg(
    Total=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Monthly=('MonthlyCharges', 'mean'),
    Avg_Tenure=('tenure', 'mean')
).round(2)
contract_churn['Churn_Rate_%'] = (
    contract_churn['Churned'] /
    contract_churn['Total'] * 100).round(1)
contract_churn = contract_churn.sort_values(
    'Churn_Rate_%', ascending=False)

print(f"\n{contract_churn.to_string()}")
logger.info("Contract churn analysis complete")

# ─────────────────────────────────────────────
# STEP 4: CHURN BY TENURE GROUP
# ─────────────────────────────────────────────
print("\n📌 4. CHURN BY TENURE GROUP")

tenure_order = [
    '0-12 months', '13-24 months',
    '25-48 months', '49+ months']
tenure_churn = df.groupby('tenure_group').agg(
    Total=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Monthly=('MonthlyCharges', 'mean')
).round(2)
tenure_churn['Churn_Rate_%'] = (
    tenure_churn['Churned'] /
    tenure_churn['Total'] * 100).round(1)
tenure_churn = tenure_churn.reindex(tenure_order)
print(f"\n{tenure_churn.to_string()}")

# ─────────────────────────────────────────────
# STEP 5: CHURN BY INTERNET SERVICE
# ─────────────────────────────────────────────
print("\n📌 5. CHURN BY INTERNET SERVICE")

internet_churn = df.groupby(
    'InternetService').agg(
    Total=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Monthly=('MonthlyCharges', 'mean')
).round(2)
internet_churn['Churn_Rate_%'] = (
    internet_churn['Churned'] /
    internet_churn['Total'] * 100).round(1)
internet_churn = internet_churn.sort_values(
    'Churn_Rate_%', ascending=False)
print(f"\n{internet_churn.to_string()}")

# ─────────────────────────────────────────────
# STEP 6: CHURN BY CHARGE BAND
# ─────────────────────────────────────────────
print("\n📌 6. CHURN BY CHARGE BAND")

charge_order = [
    'Low (<$35)', 'Medium ($35-65)',
    'High ($65-90)', 'Premium (>$90)']
charge_churn = df.groupby('charge_band').agg(
    Total=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Tenure=('tenure', 'mean')
).round(2)
charge_churn['Churn_Rate_%'] = (
    charge_churn['Churned'] /
    charge_churn['Total'] * 100).round(1)
charge_churn = charge_churn.reindex(charge_order)
print(f"\n{charge_churn.to_string()}")

# ─────────────────────────────────────────────
# STEP 7: TOP 5 CHURN RISK FACTORS
# ─────────────────────────────────────────────
print("\n📌 7. TOP CHURN RISK FACTORS")

risk_factors = []

# Contract risk
for contract in df['Contract'].unique():
    subset = df[df['Contract'] == contract]
    rate = subset['Churn'].mean() * 100
    risk_factors.append({
        'Factor': f"Contract: {contract}",
        'Churn_Rate_%': round(rate, 1),
        'Customers': len(subset)
    })

# Internet service risk
for svc in df['InternetService'].unique():
    subset = df[df['InternetService'] == svc]
    rate = subset['Churn'].mean() * 100
    risk_factors.append({
        'Factor': f"Internet: {svc}",
        'Churn_Rate_%': round(rate, 1),
        'Customers': len(subset)
    })

# Tenure group risk
for tg in tenure_order:
    subset = df[df['tenure_group'] == tg]
    rate = subset['Churn'].mean() * 100
    risk_factors.append({
        'Factor': f"Tenure: {tg}",
        'Churn_Rate_%': round(rate, 1),
        'Customers': len(subset)
    })

risk_df = pd.DataFrame(risk_factors).sort_values(
    'Churn_Rate_%',
    ascending=False).head(8)

print(f"\n{'Factor':<35} "
      f"{'Churn Rate':<15} {'Customers'}")
print("-" * 60)
for _, row in risk_df.iterrows():
    print(f"   {row['Factor']:<33} "
          f"{row['Churn_Rate_%']:>6.1f}%"
          f"         {row['Customers']:,}")

# ─────────────────────────────────────────────
# STEP 8: REVENUE IMPACT OF CHURN
# ─────────────────────────────────────────────
print("\n📌 8. REVENUE IMPACT OF CHURN")

monthly_lost = churned['MonthlyCharges'].sum()
monthly_retained = retained['MonthlyCharges'].sum()
total_monthly = df['MonthlyCharges'].sum()
churn_revenue_pct = (
    monthly_lost / total_monthly * 100)

print(f"   Total monthly revenue:    "
      f"${total_monthly:,.2f}")
print(f"   Revenue from churned:     "
      f"${monthly_lost:,.2f} "
      f"({churn_revenue_pct:.1f}%)")
print(f"   Revenue from retained:    "
      f"${monthly_retained:,.2f}")
print(f"\n   💸 Fixing churn could recover "
      f"${monthly_lost:,.2f}/month!")

logger.info(
    f"Revenue analysis: ${monthly_lost:,.2f} "
    f"monthly revenue at risk from churn")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   df[condition]          → Filter rows")
print("   df[(cond1) & (cond2)]  → Multi-filter")
print("   df.groupby().agg()     → Group & aggregate")
print("   .sort_values()         → Sort results")
print("   .reindex()             → Custom ordering")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 analysis complete")