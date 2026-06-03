# ============================================
# WEEK 2 — MODULE 2: Data Cleaning & Type Casting
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
            '../logs/module2.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

DATA = Path(__file__).parent.parent / \
    "data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"

print("=" * 55)
print("   MODULE 2 — DATA CLEANING & TYPE CASTING")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: LOAD RAW DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING RAW DATA")
logger.info("Loading raw Telco dataset")
df = pd.read_csv(DATA)
print(f"   Loaded: {df.shape[0]:,} rows x "
      f"{df.shape[1]} columns")

# Snapshot before cleaning
before_shape = df.shape
before_nulls = df.isnull().sum().sum()
logger.info(
    f"Before cleaning: {before_shape}, "
    f"{before_nulls} nulls")

# ─────────────────────────────────────────────
# STEP 2: FIX TOTALCHARGES TRAP
# ─────────────────────────────────────────────
print("\n📌 2. FIXING TOTALCHARGES TRAP")

# Show the problem
problem_count = df[
    pd.to_numeric(
        df['TotalCharges'],
        errors='coerce').isna()].shape[0]
print(f"   Problem rows before fix: {problem_count}")
print(f"   TotalCharges dtype before: "
      f"{df['TotalCharges'].dtype}")

# Fix: Replace blank spaces with NaN then convert
df['TotalCharges'] = df['TotalCharges'].replace(
    ' ', np.nan)
df['TotalCharges'] = pd.to_numeric(
    df['TotalCharges'], errors='coerce')

print(f"   TotalCharges dtype after: "
      f"{df['TotalCharges'].dtype}")
print(f"   Null TotalCharges after fix: "
      f"{df['TotalCharges'].isnull().sum()}")

logger.info(
    "TotalCharges converted from str to float64")

# ─────────────────────────────────────────────
# STEP 3: HANDLE MISSING VALUES
# ─────────────────────────────────────────────
print("\n📌 3. HANDLING MISSING VALUES")

nulls_before = df.isnull().sum()
print(f"   Null values before:")
for col, count in nulls_before[
        nulls_before > 0].items():
    print(f"   {col:<25} {count} nulls")

# Fill TotalCharges nulls with 0
# (tenure=0 means no charges yet)
df['TotalCharges'] = df[
    'TotalCharges'].fillna(0.0)

nulls_after = df.isnull().sum().sum()
print(f"\n   Total nulls after fix: {nulls_after}")
logger.info(
    "Missing TotalCharges filled with 0.0 "
    "(new customers with tenure=0)")

# ─────────────────────────────────────────────
# STEP 4: FIX SENIORCITIZEN COLUMN
# ─────────────────────────────────────────────
print("\n📌 4. FIXING SENIORCITIZEN COLUMN")
print(f"   Before: dtype={df['SeniorCitizen'].dtype}"
      f", values={df['SeniorCitizen'].unique()}")

# Convert 0/1 to No/Yes for consistency
df['SeniorCitizen'] = df['SeniorCitizen'].map(
    {0: 'No', 1: 'Yes'})

print(f"   After:  dtype={df['SeniorCitizen'].dtype}"
      f", values={df['SeniorCitizen'].unique()}")
logger.info(
    "SeniorCitizen converted from 0/1 to No/Yes")

# ─────────────────────────────────────────────
# STEP 5: STANDARDISE STRING COLUMNS
# ─────────────────────────────────────────────
print("\n📌 5. STANDARDISING STRING COLUMNS")

# Get all string columns except customerID
str_cols = df.select_dtypes(
    include=['object', 'str']).columns.tolist()
str_cols = [c for c in str_cols
            if c != 'customerID']

for col in str_cols:
    df[col] = df[col].str.strip()

print(f"   Stripped whitespace from "
      f"{len(str_cols)} string columns")
print(f"   Columns cleaned: {str_cols}")
logger.info(
    f"Whitespace stripped from {len(str_cols)} "
    f"string columns")

# ─────────────────────────────────────────────
# STEP 6: CONVERT CHURN TO BOOLEAN
# ─────────────────────────────────────────────
print("\n📌 6. CONVERTING CHURN TO BOOLEAN")
print(f"   Before: {df['Churn'].unique()}")

df['Churn'] = df['Churn'].map(
    {'Yes': True, 'No': False})

print(f"   After:  {df['Churn'].unique()}")
print(f"   dtype:  {df['Churn'].dtype}")
logger.info(
    "Churn converted from Yes/No to bool")

# ─────────────────────────────────────────────
# STEP 7: ADD ENGINEERED COLUMNS
# ─────────────────────────────────────────────
print("\n📌 7. FEATURE ENGINEERING")

# Tenure group
def tenure_group(months):
    if months <= 12:
        return '0-12 months'
    elif months <= 24:
        return '13-24 months'
    elif months <= 48:
        return '25-48 months'
    else:
        return '49+ months'

df['tenure_group'] = df['tenure'].apply(
    tenure_group)

# Monthly charge band
def charge_band(charge):
    if charge < 35:
        return 'Low (<$35)'
    elif charge < 65:
        return 'Medium ($35-65)'
    elif charge < 90:
        return 'High ($65-90)'
    else:
        return 'Premium (>$90)'

df['charge_band'] = df[
    'MonthlyCharges'].apply(charge_band)

print(f"   Added tenure_group:")
print(f"   {df['tenure_group'].value_counts().to_dict()}")
print(f"\n   Added charge_band:")
print(f"   {df['charge_band'].value_counts().to_dict()}")

logger.info(
    "Added tenure_group and charge_band columns")

# ─────────────────────────────────────────────
# STEP 8: FINAL VALIDATION
# ─────────────────────────────────────────────
print("\n📌 8. FINAL VALIDATION")
print(f"   Shape:          {df.shape}")
print(f"   Total nulls:    {df.isnull().sum().sum()}")
print(f"   Churn rate:     "
      f"{df['Churn'].mean()*100:.1f}%")
print(f"   Avg monthly $:  "
      f"{df['MonthlyCharges'].mean():.2f}")
print(f"   TotalCharges:   "
      f"{df['TotalCharges'].dtype} ✅")
print(f"   SeniorCitizen:  "
      f"{df['SeniorCitizen'].dtype} ✅")
print(f"   Churn:          "
      f"{df['Churn'].dtype} ✅")

# ─────────────────────────────────────────────
# STEP 9: SAVE CLEAN DATA
# ─────────────────────────────────────────────
print("\n📌 9. SAVING CLEAN DATA")
output_path = Path(
    "../data/processed/telco_clean.csv")
df.to_csv(output_path, index=False)
size = os.path.getsize(output_path)
print(f"   ✅ Saved: {output_path}")
print(f"   Size: {size:,} bytes")
print(f"   Rows: {len(df):,}")
print(f"   Columns: {len(df.columns)}")

logger.info(
    f"Clean data saved to {output_path} "
    f"({len(df):,} rows, {len(df.columns)} cols)")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   pd.to_numeric(errors='coerce') "
      "→ Fix TotalCharges")
print("   fillna()                       "
      "→ Handle nulls")
print("   .map()                         "
      "→ Convert values")
print("   .str.strip()                   "
      "→ Clean strings")
print("   feature engineering            "
      "→ Add useful columns")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 cleaning complete")