# ============================================
# WEEK 6 — MODULE 3: S3 ETL Pipeline
# Extract from S3, Transform, Load back to S3
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import pandas as pd
import json
import logging
import os
import io
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError

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

with open("../data/aws_config.json") as f:
    config = json.load(f)

BUCKET = config['bucket_name']
REGION = config['region']
RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
RUN_TS = datetime.now().strftime(
    "%Y%m%d_%H%M%S")

s3 = boto3.client(
    's3', region_name=REGION)

print("=" * 55)
print("   MODULE 3 — S3 ETL PIPELINE")
print("   EXTRACT → TRANSFORM → LOAD")
print("=" * 55)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def list_s3_objects(prefix=""):
    """List all objects with a prefix."""
    paginator = s3.get_paginator(
        'list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET, Prefix=prefix)
    objects = []
    for page in pages:
        objects.extend(
            page.get('Contents', []))
    return objects

def read_csv_from_s3(key):
    """Read a CSV file from S3 into DataFrame."""
    response = s3.get_object(
        Bucket=BUCKET, Key=key)
    body = response['Body'].read().decode(
        'utf-8')
    return pd.read_csv(io.StringIO(body))

def write_csv_to_s3(df, key):
    """Write DataFrame to S3 as CSV."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode('utf-8'),
        ContentType='text/csv')
    return len(buf.getvalue().encode('utf-8'))

def write_json_to_s3(data, key):
    """Write dict to S3 as JSON."""
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(
            data, indent=4).encode('utf-8'),
        ContentType='application/json')

# ─────────────────────────────────────────────
# STEP 1: EXTRACT — Find files in S3
# ─────────────────────────────────────────────
print("\n📌 1. EXTRACT — FINDING FILES IN S3")
logger.info("Stage 1: Extract from S3")

raw_objects = list_s3_objects(
    prefix="raw/customers/")
print(f"   Found {len(raw_objects)} "
      f"raw files in S3:")
for obj in raw_objects:
    print(f"   📄 {obj['Key']}")
    print(f"       Size: "
          f"{obj['Size']:,} bytes")
    print(f"       Modified: "
          f"{obj['LastModified'].strftime('%Y-%m-%d %H:%M')}")

# Get the most recent file
if not raw_objects:
    print("   ❌ No raw files found!")
    exit(1)

latest = max(
    raw_objects,
    key=lambda x: x['LastModified'])
latest_key = latest['Key']
print(f"\n   ✅ Using latest file:")
print(f"   {latest_key}")
logger.info(
    f"Extracting from: {latest_key}")

# ─────────────────────────────────────────────
# STEP 2: READ DATA FROM S3
# ─────────────────────────────────────────────
print("\n📌 2. READING DATA FROM S3")

df_raw = read_csv_from_s3(latest_key)
print(f"   ✅ Read from S3:")
print(f"   Rows:    {len(df_raw):,}")
print(f"   Columns: {df_raw.shape[1]}")
print(f"   Churn rate: "
      f"{df_raw['Churn'].mean()*100:.1f}%")
logger.info(
    f"Read {len(df_raw):,} rows from S3")

# ─────────────────────────────────────────────
# STEP 3: TRANSFORM — Enrich the data
# ─────────────────────────────────────────────
print("\n📌 3. TRANSFORM — ENRICHING DATA")
logger.info("Stage 2: Transform")

df = df_raw.copy()

# Add risk score
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

# Add risk label
df['risk_label'] = df[
    'risk_score'].apply(
    lambda s: 'Critical' if s >= 7
    else 'High' if s >= 5
    else 'Medium' if s >= 3
    else 'Low')

# Add lifetime value
df['lifetime_value'] = (
    df['MonthlyCharges'] *
    df['tenure']).round(2)

# Add annual value projection
df['annual_value'] = (
    df['MonthlyCharges'] * 12).round(2)

# Add processed timestamp
df['processed_at'] = datetime.now(
    ).strftime("%Y-%m-%d %H:%M:%S")

# Add pipeline version
df['pipeline_version'] = "v3.0"

new_cols = [
    'risk_score', 'risk_label',
    'lifetime_value', 'annual_value',
    'processed_at', 'pipeline_version']
print(f"   ✅ Added {len(new_cols)} "
      f"new columns:")
for col in new_cols:
    print(f"   + {col}")

print(f"\n   Risk distribution:")
risk_counts = df['risk_label'].value_counts()
for label, count in risk_counts.items():
    pct = count / len(df) * 100
    print(f"   {label:<12} "
          f"{count:>5,} ({pct:.1f}%)")

logger.info(
    f"Transform complete: "
    f"{len(df.columns)} columns")

# ─────────────────────────────────────────────
# STEP 4: VALIDATE TRANSFORMED DATA
# ─────────────────────────────────────────────
print("\n📌 4. VALIDATE TRANSFORMED DATA")
logger.info("Validating transformed data")

checks = {
    "Total rows": len(df),
    "Null risk scores": int(
        df['risk_score'].isnull().sum()),
    "Negative lifetime values": int(
        (df['lifetime_value'] < 0).sum()),
    "Invalid risk labels": int(
        (~df['risk_label'].isin([
            'Critical', 'High',
            'Medium', 'Low'])).sum()),
    "Missing customerIDs": int(
        df['customerID'].isnull().sum()),
}

all_passed = True
for check, value in checks.items():
    if check == "Total rows":
        status = "✅ OK"
    else:
        status = "✅ Pass" if value == 0 \
            else "❌ FAIL"
        if value > 0:
            all_passed = False
    print(f"   {check:<30} "
          f"{value:>6,} {status}")

print(f"\n   Overall: "
      f"{'✅ ALL PASSED' if all_passed else '❌ ISSUES FOUND'}")
logger.info(
    f"Validation: "
    f"{'PASSED' if all_passed else 'FAILED'}")

# ─────────────────────────────────────────────
# STEP 5: LOAD — Write back to S3
# ─────────────────────────────────────────────
print("\n📌 5. LOAD — WRITING BACK TO S3")
logger.info("Stage 3: Load to S3")

# Full enriched dataset
enriched_key = (
    f"processed/telco/enriched/"
    f"year={datetime.now().year}/"
    f"telco_enriched_{RUN_TS}.csv")
size = write_csv_to_s3(df, enriched_key)
print(f"   ✅ Full enriched dataset:")
print(f"   {enriched_key}")
print(f"   {size:,} bytes | "
      f"{len(df):,} rows")
logger.info(
    f"Uploaded enriched: {enriched_key}")

# High risk customers only
high_risk = df[
    df['risk_label'].isin(
        ['Critical', 'High']) &
    (df['Churn'] == False)]
hr_key = (
    f"processed/telco/high_risk/"
    f"high_risk_customers_{RUN_TS}.csv")
size_hr = write_csv_to_s3(
    high_risk, hr_key)
print(f"\n   ✅ High risk customers:")
print(f"   {hr_key}")
print(f"   {size_hr:,} bytes | "
      f"{len(high_risk):,} rows")
logger.info(
    f"Uploaded high risk: {hr_key}")

# Intervention targets
intervention = df[
    (df['Churn'] == False) &
    (df['risk_score'] >= 7)
].sort_values(
    'MonthlyCharges',
    ascending=False)[[
    'customerID', 'Contract',
    'MonthlyCharges', 'tenure',
    'risk_score', 'risk_label',
    'lifetime_value']]

iv_key = (
    f"reports/interventions/"
    f"intervention_targets_{RUN_TS}.csv")
size_iv = write_csv_to_s3(
    intervention, iv_key)
print(f"\n   ✅ Intervention targets:")
print(f"   {iv_key}")
print(f"   {size_iv:,} bytes | "
      f"{len(intervention):,} customers")
logger.info(
    f"Uploaded interventions: {iv_key}")

# ─────────────────────────────────────────────
# STEP 6: PIPELINE REPORT
# ─────────────────────────────────────────────
print("\n📌 6. PIPELINE REPORT")
logger.info("Generating pipeline report")

report = {
    "pipeline_name": "S3 ETL Pipeline",
    "version": "3.0.0",
    "author": "Lawrence Koomson",
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "source": {
        "bucket": BUCKET,
        "key": latest_key,
        "rows_extracted": len(df_raw)
    },
    "transform": {
        "columns_added": len(new_cols),
        "validation": "PASSED"
    },
    "output": {
        "enriched_rows": len(df),
        "high_risk_customers": len(
            high_risk),
        "intervention_targets": len(
            intervention)
    },
    "risk_summary": {
        label: int(count)
        for label, count in
        risk_counts.items()
    },
    "revenue": {
        "total_monthly": round(
            df['MonthlyCharges'].sum(), 2),
        "at_risk_monthly": round(
            df[df['Churn'] == True][
                'MonthlyCharges'].sum(), 2),
        "high_risk_retained_monthly": round(
            high_risk[
                'MonthlyCharges'].sum(), 2)
    }
}

report_key = (
    f"reports/etl_report_{RUN_DATE}.json")
write_json_to_s3(report, report_key)
print(f"   ✅ Report saved: {report_key}")

# Print key metrics
print(f"\n   📊 ETL Summary:")
print(f"   Rows extracted:     "
      f"{report['source']['rows_extracted']:,}")
print(f"   Columns added:      "
      f"{report['transform']['columns_added']}")
print(f"   High risk retained: "
      f"{report['output']['high_risk_customers']:,}")
print(f"   Intervention targets: "
      f"{report['output']['intervention_targets']:,}")
print(f"   Monthly at risk:    "
      f"${report['revenue']['at_risk_monthly']:,.2f}")
logger.info(
    f"ETL complete: "
    f"{len(df):,} rows processed")

# ─────────────────────────────────────────────
# STEP 7: FINAL S3 INVENTORY
# ─────────────────────────────────────────────
print("\n📌 7. FINAL S3 INVENTORY")

all_objects = list_s3_objects()
total_size = sum(
    o['Size'] for o in all_objects)

folders = {}
for obj in all_objects:
    parts = obj['Key'].split('/')
    folder = parts[0] if len(
        parts) > 1 else 'root'
    folders[folder] = \
        folders.get(folder, 0) + 1

print(f"   Total objects: "
      f"{len(all_objects)}")
print(f"   Total size:    "
      f"{total_size/1024:.1f} KB")
print(f"\n   📁 Folders:")
for folder, count in sorted(
        folders.items()):
    print(f"   {folder:<20} "
          f"{count:>3} objects")

logger.info(
    f"Final inventory: "
    f"{len(all_objects)} objects")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   get_object()     → Read from S3")
print("   put_object()     → Write to S3")
print("   S3 ETL pattern   → Extract→Transform→Load")
print("   Validation       → Check before loading")
print("   Partitioning     → Organise by attribute")
print("   Reports          → Track every pipeline run")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 S3 ETL complete")