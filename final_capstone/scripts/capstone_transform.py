# ============================================
# FINAL CAPSTONE — MODULE 2: TRANSFORMATION
# Clean, enrich, and score the unified dataset
# GetSkills Network DE Bootcamp
# Lawrence Koomson
# ============================================

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data", exist_ok=True)
os.makedirs("../reports", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/transform.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
AUTHOR = "Lawrence Koomson"

print("=" * 55)
print("   FINAL CAPSTONE")
print("   MODULE 2 — TRANSFORMATION")
print("=" * 55)

logger.info(
    "Module 2: Transformation started")

# ─────────────────────────────────────────────
# STEP 1: LOAD INGESTED DATA
# ─────────────────────────────────────────────
print("\n📌 1. LOADING INGESTED DATA")
logger.info("Loading ingested data")

df = pd.read_csv(
    "../data/telco_ingested.csv")

with open(
        "../data/api_data.json") as f:
    api_data = json.load(f)

print(f"   ✅ Loaded {len(df):,} rows")
print(f"   ✅ API context loaded")
logger.info(
    f"Loaded {len(df):,} rows for "
    f"transformation")

# ─────────────────────────────────────────────
# STEP 2: DATA QUALITY VALIDATION
# ─────────────────────────────────────────────
print("\n📌 2. DATA QUALITY VALIDATION")
logger.info("Running data quality checks")

quality_checks = {
    "Total rows": len(df),
    "Null customerIDs": df[
        'customerID'].isnull().sum(),
    "Duplicate customerIDs": df[
        'customerID'].duplicated().sum(),
    "Invalid MonthlyCharges": (
        df['MonthlyCharges'] <= 0
    ).sum(),
    "Invalid tenure": (
        df['tenure'] < 0).sum(),
    "Missing Contract": df[
        'Contract'].isnull().sum(),
}

all_passed = True
print(f"\n   {'Check':<28} "
      f"{'Result':>8}  Status")
print("   " + "-" * 50)
for check, value in quality_checks.items():
    if check == "Total rows":
        status = "✅ OK"
    else:
        status = "✅ Pass" if value == 0 \
            else "❌ FAIL"
        if value > 0:
            all_passed = False
    print(f"   {check:<28} "
          f"{value:>8,}  {status}")

print(f"\n   Overall: "
      f"{'✅ ALL PASSED' if all_passed else '⚠️ ISSUES FOUND'}")
logger.info(
    f"Quality: "
    f"{'PASSED' if all_passed else 'ISSUES'}")

# ─────────────────────────────────────────────
# STEP 3: RISK SCORING ENGINE
# ─────────────────────────────────────────────
print("\n📌 3. RISK SCORING ENGINE")
logger.info("Building risk scoring engine")

df['risk_score'] = (
    df['Contract'].map({
        'Month-to-month': 3,
        'One year': 1,
        'Two year': 0
    }).fillna(0) +
    df['PaymentMethod'].map({
        'Electronic check': 3,
        'Mailed check': 2,
        'Bank transfer (automatic)': 0,
        'Credit card (automatic)': 0
    }).fillna(0) +
    df['tenure'].apply(
        lambda t: 3 if t <= 12
        else 2 if t <= 24
        else 1 if t <= 48
        else 0)
)

df['risk_label'] = df[
    'risk_score'].apply(
    lambda s: 'Critical' if s >= 7
    else 'High' if s >= 5
    else 'Medium' if s >= 3
    else 'Low')

df['lifetime_value'] = (
    df['MonthlyCharges'] *
    df['tenure']).round(2)

df['annual_value'] = (
    df['MonthlyCharges'] * 12).round(2)

risk_dist = df[
    'risk_label'].value_counts()
print(f"   ✅ Risk scoring complete!")
print(f"\n   Risk distribution:")
for label, count in risk_dist.items():
    pct = count / len(df) * 100
    print(f"   {label:<12} "
          f"{count:>5,} ({pct:.1f}%)")

logger.info(
    f"Risk scoring complete: "
    f"{len(df.columns)} columns")

# ─────────────────────────────────────────────
# STEP 4: INTERVENTION TARGETING
# ─────────────────────────────────────────────
print("\n📌 4. INTERVENTION TARGETING")
logger.info("Building intervention targets")

df['action'] = np.select(
    [
        (df['risk_score'] >= 8) &
        (df['Churn'] == False),
        (df['risk_score'] >= 6) &
        (df['Churn'] == False),
        (df['risk_score'] >= 4) &
        (df['Churn'] == False),
    ],
    [
        '🔴 Immediate call',
        '🟡 Email campaign',
        '🟠 Auto-pay offer',
    ],
    default='🟢 Standard programme'
)

action_summary = df[
    df['Churn'] == False
].groupby('action').agg(
    customers=('customerID', 'count'),
    revenue=('MonthlyCharges', 'sum')
).round(2).reset_index()

print(f"   ✅ Interventions assigned!")
print(f"\n   {action_summary.to_string(index=False)}")
logger.info(
    f"Interventions: "
    f"{len(action_summary)} categories")

# ─────────────────────────────────────────────
# STEP 5: ENRICH WITH API CONTEXT
# ─────────────────────────────────────────────
print("\n📌 5. ENRICHING WITH API CONTEXT")
logger.info("Enriching with API context")

weather = api_data.get('weather', {})
ghana = api_data.get('ghana', {})

df['market_context'] = (
    f"Ghana | "
    f"{weather.get('temperature_c', 'N/A')}°C")
df['pipeline_run_date'] = RUN_DATE
df['pipeline_author'] = AUTHOR

print(f"   ✅ Market context added")
print(f"   Region:      "
      f"{ghana.get('name', 'Ghana')}")
print(f"   Temperature: "
      f"{weather.get('temperature_c', 'N/A')}°C")
logger.info("API context merged")

# ─────────────────────────────────────────────
# STEP 6: SEGMENT ANALYSIS
# ─────────────────────────────────────────────
print("\n📌 6. SEGMENT ANALYSIS")
logger.info("Running segment analysis")

segment_summary = df.groupby(
    ['Contract', 'InternetService']
).agg(
    customers=('customerID', 'count'),
    churned=('Churn', 'sum'),
    avg_monthly=(
        'MonthlyCharges', 'mean'),
    total_revenue=(
        'MonthlyCharges', 'sum')
).round(2).reset_index()

segment_summary['churn_rate_pct'] = (
    segment_summary['churned'] /
    segment_summary['customers'] * 100
).round(1)

segment_summary = (
    segment_summary.sort_values(
        'churn_rate_pct',
        ascending=False))

print(f"   ✅ Segment analysis complete!")
print(f"\n   Top 5 highest-risk segments:")
print(segment_summary.head(5)[[
    'Contract', 'InternetService',
    'customers', 'churn_rate_pct',
    'total_revenue']].to_string(
    index=False))
logger.info(
    f"Segments: {len(segment_summary)}")

# ─────────────────────────────────────────────
# STEP 7: SAVE TRANSFORMED DATA
# ─────────────────────────────────────────────
print("\n📌 7. SAVING TRANSFORMED DATA")
logger.info("Saving transformed outputs")

df.to_csv(
    "../data/telco_transformed.csv",
    index=False)

action_summary.to_csv(
    "../reports/intervention_summary.csv",
    index=False)

segment_summary.to_csv(
    "../reports/segment_analysis.csv",
    index=False)

transform_report = {
    "module": "Transformation",
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "data_quality": (
        "ALL PASSED" if all_passed
        else "ISSUES FOUND"),
    "input_rows": len(df),
    "columns_after_transform": len(
        df.columns),
    "risk_distribution": {
        k: int(v) for k, v in
        risk_dist.items()
    },
    "intervention_summary": (
        action_summary.to_dict(
            orient='records')),
    "top_risk_segment": {
        "contract": segment_summary.iloc[0][
            'Contract'],
        "internet": segment_summary.iloc[0][
            'InternetService'],
        "churn_rate_pct": float(
            segment_summary.iloc[0][
                'churn_rate_pct'])
    },
    "market_context": {
        "country": ghana.get(
            'name', 'Ghana'),
        "temperature_c": weather.get(
            'temperature_c', 'N/A')
    }
}

with open(
        "../reports/transform_report.json",
        'w') as f:
    json.dump(
        transform_report, f, indent=4)

print("   ✅ telco_transformed.csv")
print("   ✅ intervention_summary.csv")
print("   ✅ segment_analysis.csv")
print("   ✅ transform_report.json")
logger.info("Transformation outputs saved")

print(f"\n{'='*55}")
print("   MODULE 2 TRANSFORMATION SUMMARY")
print(f"{'='*55}")
print(f"   Rows transformed: {len(df):,}")
print(f"   Columns added:    "
      f"{len(df.columns) - 23}")
print(f"   Data quality:     "
      f"{'✅ PASSED' if all_passed else '⚠️ ISSUES'}")
print(f"   Risk segments:    "
      f"{len(risk_dist)}")
print(f"   Interventions:    "
      f"{len(action_summary)}")
print(f"\n✅ Module 2 — Transformation Complete!")
logger.info("Module 2 transformation complete")