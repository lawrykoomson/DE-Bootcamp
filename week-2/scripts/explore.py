# ============================================
# WEEK 2 — MODULE 1: Pandas Setup & First Look
# GetSkills Network DE Bootcamp
# ============================================

import pandas as pd
import logging
import os
from pathlib import Path

# ─────────────────────────────────────────────
# SETUP LOGGING
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/module1.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# FILE PATH
# ─────────────────────────────────────────────
DATA = Path(__file__).parent.parent / \
    "data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"

# ─────────────────────────────────────────────
# STEP 1: LOAD THE DATASET
# ─────────────────────────────────────────────
print("=" * 55)
print("   MODULE 1 — PANDAS SETUP & FIRST LOOK")
print("=" * 55)

logger.info("Loading Telco Customer Churn dataset")
df = pd.read_csv(DATA)
logger.info(
    f"Dataset loaded: {df.shape[0]:,} rows x "
    f"{df.shape[1]} columns")

# ─────────────────────────────────────────────
# STEP 2: SHAPE
# ─────────────────────────────────────────────
print("\n📌 1. SHAPE")
print(f"   Rows:    {df.shape[0]:,}")
print(f"   Columns: {df.shape[1]}")
print(f"   All columns: {list(df.columns)}")

# ─────────────────────────────────────────────
# STEP 3: DTYPES & NULL COUNTS
# ─────────────────────────────────────────────
print("\n📌 2. DTYPES & NULL COUNTS")
print(f"\n{'Column':<25} {'Dtype':<12} "
      f"{'Non-Null':<12} {'Nulls'}")
print("-" * 60)
for col in df.columns:
    nulls = df[col].isnull().sum()
    print(f"   {col:<23} {str(df[col].dtype):<12} "
          f"{df[col].count():<12} {nulls}")

# ─────────────────────────────────────────────
# STEP 4: NUMERIC STATS
# ─────────────────────────────────────────────
print("\n📌 3. NUMERIC STATISTICS")
print(df.describe().round(2).to_string())

# ─────────────────────────────────────────────
# STEP 5: SAMPLE ROWS
# ─────────────────────────────────────────────
print("\n📌 4. SAMPLE — 5 RANDOM ROWS")
print(df.sample(5, random_state=42)[
    ['customerID', 'tenure',
     'MonthlyCharges', 'TotalCharges',
     'Contract', 'Churn']].to_string())

# ─────────────────────────────────────────────
# STEP 6: TOTALCHARGES TRAP
# ─────────────────────────────────────────────
print("\n📌 5. THE TOTALCHARGES TRAP")
print(f"   TotalCharges dtype: "
      f"{df['TotalCharges'].dtype}")

problem_rows = df[
    pd.to_numeric(
        df['TotalCharges'],
        errors='coerce').isna()]

print(f"   Non-numeric rows:   {len(problem_rows)}")
print(f"\n   Problematic rows:")
print(problem_rows[[
    'customerID', 'tenure',
    'MonthlyCharges',
    'TotalCharges']].to_string())

logger.warning(
    f"TotalCharges has {len(problem_rows)} "
    f"non-numeric values — all have tenure=0")

# ─────────────────────────────────────────────
# STEP 7: KEY COLUMN DISTRIBUTIONS
# ─────────────────────────────────────────────
print("\n📌 6. KEY COLUMN DISTRIBUTIONS")

key_cols = [
    "Churn", "Contract",
    "InternetService", "PaymentMethod"]

for col in key_cols:
    counts = df[col].value_counts()
    total = len(df)
    print(f"\n   {col}:")
    for val, count in counts.items():
        pct = count / total * 100
        print(f"   {val:<30} "
              f"{count:>5} ({pct:.1f}%)")

# ─────────────────────────────────────────────
# STEP 8: SENIOR CITIZEN INSIGHT
# ─────────────────────────────────────────────
print("\n📌 7. SENIORCITIZEN COLUMN")
sc_mean = df['SeniorCitizen'].mean()
sc_count = df['SeniorCitizen'].sum()
print(f"   dtype:   {df['SeniorCitizen'].dtype}")
print(f"   Mean:    {sc_mean:.3f}")
print(f"   Count:   {sc_count:,}")
print(f"   Meaning: {sc_mean*100:.1f}% of customers"
      f" are senior citizens")
print(f"   Note:    Stored as 0/1 integers"
      f" not True/False!")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   pd.read_csv()     → Load CSV into DataFrame")
print("   df.shape          → Rows and columns count")
print("   df.info()         → Dtypes and null counts")
print("   df.describe()     → Numeric statistics")
print("   df.sample()       → Random rows preview")
print("   df.value_counts() → Category distributions")
print(f"\n✅ Module 1 Complete!")

logger.info("Module 1 exploration complete")