# ============================================
# WEEK 6 — MODULE 2: S3 Data Lake Pipeline
# Partitioning, Folders, Real Data Upload
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
            '../logs/module2.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Load config from Module 1
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
print("   MODULE 2 — S3 DATA LAKE PIPELINE")
print("   PARTITIONING | FOLDERS | REAL DATA")
print("=" * 55)
print(f"\n   Bucket: {BUCKET}")
print(f"   Region: {REGION}")
print(f"   Date:   {RUN_DATE}")

# ─────────────────────────────────────────────
# STEP 1: DATA LAKE FOLDER STRUCTURE
# ─────────────────────────────────────────────
print("\n📌 1. DATA LAKE FOLDER STRUCTURE")
print("""
   Production Data Lake structure:

   s3://bucket/
   ├── raw/                 ← Original files
   │   └── customers/
   │       └── year=2026/
   │           └── month=06/
   ├── processed/           ← Cleaned data
   │   └── telco/
   │       └── year=2026/
   ├── reports/             ← Analysis outputs
   ├── logs/                ← Pipeline logs
   └── archive/             ← Old data
""")

# ─────────────────────────────────────────────
# STEP 2: LOAD REAL TELCO DATA
# ─────────────────────────────────────────────
print("\n📌 2. LOADING REAL TELCO DATA")

CLEAN_DATA = Path(
    "../../week-2/data/processed/"
    "telco_clean.csv")

logger.info(
    f"Loading Telco data from {CLEAN_DATA}")

df = pd.read_csv(CLEAN_DATA)
print(f"   ✅ Loaded: {len(df):,} rows x "
      f"{df.shape[1]} columns")
logger.info(
    f"Loaded {len(df):,} rows")

# ─────────────────────────────────────────────
# STEP 3: PARTITION DATA
# ─────────────────────────────────────────────
print("\n📌 3. PARTITIONING DATA")
print("   Partitioning = splitting data into")
print("   folders for faster querying!")

# Partition by churn status and contract
partitions = {
    "churned": df[df['Churn'] == True],
    "retained": df[df['Churn'] == False],
}

for partition, data in partitions.items():
    print(f"   Partition '{partition}': "
          f"{len(data):,} rows")

# Split by contract type
contracts = df['Contract'].unique()
for contract in contracts:
    subset = df[
        df['Contract'] == contract]
    safe_name = contract.replace(
        '-', '_').replace(' ', '_').lower()
    print(f"   Contract '{safe_name}': "
          f"{len(subset):,} rows")

logger.info(
    "Data partitioned by churn and contract")

# ─────────────────────────────────────────────
# STEP 4: UPLOAD FULL DATASET TO S3
# ─────────────────────────────────────────────
print("\n📌 4. UPLOADING FULL DATASET TO S3")
logger.info("Uploading full dataset to S3")

# Convert DataFrame to CSV in memory
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
csv_bytes = csv_buffer.getvalue().encode(
    'utf-8')

# Upload with partitioned path
s3_key = (
    f"raw/customers/"
    f"year={datetime.now().year}/"
    f"month={datetime.now().month:02d}/"
    f"telco_customers_{RUN_TS}.csv")

s3.put_object(
    Bucket=BUCKET,
    Key=s3_key,
    Body=csv_bytes,
    ContentType='text/csv',
    Metadata={
        'rows': str(len(df)),
        'columns': str(df.shape[1]),
        'uploaded_by': 'lawrence-koomson',
        'pipeline': 'telco-de-bootcamp'
    }
)
print(f"   ✅ Uploaded full dataset:")
print(f"   Key:  {s3_key}")
print(f"   Size: {len(csv_bytes):,} bytes")
print(f"   Rows: {len(df):,}")
logger.info(
    f"Uploaded full dataset: {s3_key}")

# ─────────────────────────────────────────────
# STEP 5: UPLOAD PARTITIONED FILES
# ─────────────────────────────────────────────
print("\n📌 5. UPLOADING PARTITIONED FILES")
logger.info("Uploading partitioned files")

uploaded_partitions = []

for partition, data in partitions.items():
    buf = io.StringIO()
    data.to_csv(buf, index=False)
    body = buf.getvalue().encode('utf-8')

    key = (
        f"processed/telco/"
        f"churn={partition}/"
        f"year={datetime.now().year}/"
        f"telco_{partition}_{RUN_TS}.csv")

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentType='text/csv')

    print(f"   ✅ {partition:<12} "
          f"{len(data):>5,} rows → "
          f"{key}")
    uploaded_partitions.append(key)
    logger.info(
        f"Uploaded partition: {key}")

# Upload by contract
for contract in contracts:
    subset = df[
        df['Contract'] == contract]
    safe = contract.replace(
        '-', '_').replace(
        ' ', '_').lower()

    buf = io.StringIO()
    subset.to_csv(buf, index=False)
    body = buf.getvalue().encode('utf-8')

    key = (
        f"processed/telco/"
        f"contract={safe}/"
        f"year={datetime.now().year}/"
        f"telco_{safe}_{RUN_TS}.csv")

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentType='text/csv')

    print(f"   ✅ {safe:<25} "
          f"{len(subset):>5,} rows → S3")
    logger.info(
        f"Uploaded contract partition: {key}")

# ─────────────────────────────────────────────
# STEP 6: UPLOAD ANALYSIS REPORTS
# ─────────────────────────────────────────────
print("\n📌 6. UPLOADING ANALYSIS REPORTS")
logger.info("Uploading analysis reports")

# Generate churn summary
churn_summary = df.groupby(
    'Contract').agg(
    total=('Churn', 'count'),
    churned=('Churn', 'sum'),
    avg_monthly=('MonthlyCharges', 'mean'),
    total_revenue=('MonthlyCharges', 'sum')
).round(2).reset_index()
churn_summary['churn_rate_pct'] = (
    churn_summary['churned'] /
    churn_summary['total'] * 100
).round(1)

# Upload churn summary CSV
buf = io.StringIO()
churn_summary.to_csv(buf, index=False)
s3.put_object(
    Bucket=BUCKET,
    Key=f"reports/churn_summary_{RUN_DATE}.csv",
    Body=buf.getvalue().encode('utf-8'),
    ContentType='text/csv')
print(f"   ✅ churn_summary_{RUN_DATE}.csv")

# Upload JSON report
report = {
    "pipeline": "Telco S3 Data Lake Pipeline",
    "run_timestamp": datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"),
    "bucket": BUCKET,
    "total_customers": len(df),
    "churn_rate_pct": round(
        df['Churn'].mean() * 100, 2),
    "monthly_revenue": round(
        df['MonthlyCharges'].sum(), 2),
    "revenue_at_risk": round(
        df[df['Churn'] == True][
            'MonthlyCharges'].sum(), 2),
    "partitions_created": (
        len(partitions) + len(contracts)),
    "s3_structure": {
        "raw": "Full raw dataset",
        "processed/churn=churned":
            f"{len(partitions['churned']):,} rows",
        "processed/churn=retained":
            f"{len(partitions['retained']):,} rows",
    }
}

s3.put_object(
    Bucket=BUCKET,
    Key=f"reports/pipeline_report_{RUN_DATE}.json",
    Body=json.dumps(
        report, indent=4).encode('utf-8'),
    ContentType='application/json')
print(f"   ✅ pipeline_report_{RUN_DATE}.json")
logger.info("Reports uploaded to S3")

# ─────────────────────────────────────────────
# STEP 7: LIST ALL S3 OBJECTS
# ─────────────────────────────────────────────
print("\n📌 7. DATA LAKE INVENTORY")

paginator = s3.get_paginator(
    'list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET)

all_objects = []
for page in pages:
    all_objects.extend(
        page.get('Contents', []))

total_size = sum(
    o['Size'] for o in all_objects)

print(f"   Total objects: "
      f"{len(all_objects)}")
print(f"   Total size:    "
      f"{total_size:,} bytes "
      f"({total_size/1024:.1f} KB)")

# Group by folder
folders = {}
for obj in all_objects:
    folder = obj['Key'].split('/')[0]
    folders[folder] = folders.get(
        folder, 0) + 1

print(f"\n   📁 Folder breakdown:")
for folder, count in sorted(
        folders.items()):
    print(f"   {folder:<20} "
          f"{count:>3} objects")

logger.info(
    f"Data lake: {len(all_objects)} "
    f"objects, {total_size:,} bytes")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   put_object()      → Upload with metadata")
print("   Partitioning      → Folder by attribute")
print("   io.StringIO()     → Upload from memory")
print("   Paginator         → List all objects")
print("   Data Lake pattern → raw/processed/reports")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 S3 data lake complete")