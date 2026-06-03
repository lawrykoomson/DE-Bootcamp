# ============================================
# WEEK 2 — MODULE 4: Merging & Joining
# GetSkills Network DE Bootcamp
# ============================================

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data/processed", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/module4.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

CLEAN_DATA = Path(__file__).parent.parent / \
    "data/processed/telco_clean.csv"

print("=" * 55)
print("   MODULE 4 — MERGING & JOINING")
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
# STEP 2: CREATE SUPPLEMENTARY TABLES
# ─────────────────────────────────────────────
print("\n📌 2. CREATING SUPPLEMENTARY TABLES")

# Contract pricing table
contract_pricing = pd.DataFrame({
    'Contract': [
        'Month-to-month', 'One year', 'Two year'],
    'discount_pct': [0, 5, 15],
    'retention_bonus_usd': [0, 50, 150],
    'contract_type': [
        'Flexible', 'Annual', 'Long-term']
})
print(f"\n   Contract pricing table:")
print(contract_pricing.to_string(index=False))

# Internet service SLA table
internet_sla = pd.DataFrame({
    'InternetService': [
        'Fiber optic', 'DSL', 'No'],
    'avg_speed_mbps': [300, 50, 0],
    'sla_uptime_pct': [99.9, 99.5, 100.0],
    'support_tier': [
        'Premium', 'Standard', 'Basic']
})
print(f"\n   Internet SLA table:")
print(internet_sla.to_string(index=False))

# Payment risk table
payment_risk = pd.DataFrame({
    'PaymentMethod': [
        'Electronic check',
        'Mailed check',
        'Bank transfer (automatic)',
        'Credit card (automatic)'],
    'risk_level': [
        'High', 'Medium', 'Low', 'Low'],
    'payment_reliability_score': [
        6.2, 7.1, 9.4, 9.1]
})
print(f"\n   Payment risk table:")
print(payment_risk.to_string(index=False))

logger.info(
    "Supplementary tables created: "
    "contract_pricing, internet_sla, payment_risk")

# ─────────────────────────────────────────────
# STEP 3: INNER JOIN — Contract Pricing
# ─────────────────────────────────────────────
print("\n📌 3. INNER JOIN — Contract Pricing")

df_with_pricing = df.merge(
    contract_pricing,
    on='Contract',
    how='inner'
)
print(f"   Before merge: {df.shape}")
print(f"   After merge:  {df_with_pricing.shape}")
print(f"\n   Sample merged rows:")
print(df_with_pricing[[
    'customerID', 'Contract',
    'MonthlyCharges', 'discount_pct',
    'contract_type']].head(5).to_string(
    index=False))

# Discounted price calculation
df_with_pricing['discounted_price'] = (
    df_with_pricing['MonthlyCharges'] *
    (1 - df_with_pricing['discount_pct'] / 100)
).round(2)

print(f"\n   Avg discount by contract:")
print(df_with_pricing.groupby('Contract')[[
    'MonthlyCharges',
    'discounted_price']].mean().round(2))

logger.info(
    "Inner join with contract_pricing complete")

# ─────────────────────────────────────────────
# STEP 4: LEFT JOIN — Internet SLA
# ─────────────────────────────────────────────
print("\n📌 4. LEFT JOIN — Internet SLA")

df_with_sla = df.merge(
    internet_sla,
    on='InternetService',
    how='left'
)
print(f"   Before merge: {df.shape}")
print(f"   After merge:  {df_with_sla.shape}")
print(f"\n   Churn rate by support tier:")
tier_churn = df_with_sla.groupby(
    'support_tier').agg(
    Customers=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Speed=('avg_speed_mbps', 'mean')
).round(2)
tier_churn['Churn_Rate_%'] = (
    tier_churn['Churned'] /
    tier_churn['Customers'] * 100).round(1)
print(tier_churn)

logger.info("Left join with internet_sla complete")

# ─────────────────────────────────────────────
# STEP 5: MERGE — Payment Risk
# ─────────────────────────────────────────────
print("\n📌 5. MERGE — Payment Risk Analysis")

df_with_risk = df.merge(
    payment_risk,
    on='PaymentMethod',
    how='left'
)
print(f"   Churn rate by payment risk level:")
risk_churn = df_with_risk.groupby(
    'risk_level').agg(
    Customers=('Churn', 'count'),
    Churned=('Churn', 'sum'),
    Avg_Reliability=(
        'payment_reliability_score', 'mean')
).round(2)
risk_churn['Churn_Rate_%'] = (
    risk_churn['Churned'] /
    risk_churn['Customers'] * 100).round(1)
risk_churn = risk_churn.sort_values(
    'Churn_Rate_%', ascending=False)
print(risk_churn)

logger.info(
    "Payment risk merge complete")

# ─────────────────────────────────────────────
# STEP 6: CONCAT — SPLIT AND COMBINE
# ─────────────────────────────────────────────
print("\n📌 6. CONCAT — Combining DataFrames")

# Split by churn status
churned_df = df[df['Churn'] == True].copy()
retained_df = df[df['Churn'] == False].copy()

# Add source label
churned_df['segment'] = 'Churned'
retained_df['segment'] = 'Retained'

# Concat back together
combined_df = pd.concat(
    [churned_df, retained_df],
    ignore_index=True)

print(f"   Churned df:   {churned_df.shape}")
print(f"   Retained df:  {retained_df.shape}")
print(f"   Combined df:  {combined_df.shape}")
print(f"\n   Segment distribution:")
print(combined_df['segment'].value_counts())

logger.info(
    "Concat of churned + retained complete")

# ─────────────────────────────────────────────
# STEP 7: FULL ENRICHED DATASET
# ─────────────────────────────────────────────
print("\n📌 7. BUILDING FULL ENRICHED DATASET")

df_enriched = df.merge(
    contract_pricing,
    on='Contract', how='left'
).merge(
    internet_sla,
    on='InternetService', how='left'
).merge(
    payment_risk,
    on='PaymentMethod', how='left'
)

df_enriched['discounted_price'] = (
    df_enriched['MonthlyCharges'] *
    (1 - df_enriched['discount_pct'] / 100)
).round(2)

print(f"   Original columns:  {df.shape[1]}")
print(f"   Enriched columns:  {df_enriched.shape[1]}")
print(f"   New columns added: "
      f"{df_enriched.shape[1] - df.shape[1]}")
print(f"\n   New columns:")
new_cols = [c for c in df_enriched.columns
            if c not in df.columns]
for col in new_cols:
    print(f"   + {col}")

# Save enriched dataset
output = Path(
    "../data/processed/telco_enriched.csv")
df_enriched.to_csv(output, index=False)
print(f"\n   ✅ Saved enriched dataset: {output}")
print(f"   Rows: {len(df_enriched):,} | "
      f"Columns: {len(df_enriched.columns)}")

logger.info(
    f"Enriched dataset saved: "
    f"{df_enriched.shape[0]:,} rows, "
    f"{df_enriched.shape[1]} columns")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   df.merge(how='inner') → Keep matching rows")
print("   df.merge(how='left')  → Keep all left rows")
print("   df.merge(how='right') → Keep all right rows")
print("   pd.concat()           → Stack DataFrames")
print("   Chain .merge().merge()→ Multi-table joins")
print(f"\n✅ Module 4 Complete!")
logger.info("Module 4 merging complete")