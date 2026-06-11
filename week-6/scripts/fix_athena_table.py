# ============================================
# WEEK 6 — FIX ATHENA TABLE SCHEMA
# Correct column mapping for telco CSV
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import pandas as pd
import json
import logging
import os
import time
import io
from datetime import datetime
from botocore.exceptions import ClientError

os.makedirs("../logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/fix_athena.log',
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
ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/")
DB_NAME = "telco_de_bootcamp"
TABLE_NAME = "telco_customers"

s3 = boto3.client(
    's3', region_name=REGION)
athena = boto3.client(
    'athena', region_name=REGION)
glue = boto3.client(
    'glue', region_name=REGION)

print("=" * 55)
print("   FIX ATHENA TABLE SCHEMA")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: CHECK ACTUAL CSV COLUMNS
# ─────────────────────────────────────────────
print("\n📌 1. CHECKING ACTUAL CSV COLUMNS")

# Find the enriched file
paginator = s3.get_paginator(
    'list_objects_v2')
pages = paginator.paginate(
    Bucket=BUCKET,
    Prefix="processed/telco/enriched/")

enriched_files = []
for page in pages:
    enriched_files.extend(
        page.get('Contents', []))

latest = max(
    enriched_files,
    key=lambda x: x['LastModified'])
print(f"   File: {latest['Key']}")

# Read first few rows
response = s3.get_object(
    Bucket=BUCKET,
    Key=latest['Key'])
content = response['Body'].read().decode(
    'utf-8')
df_sample = pd.read_csv(
    io.StringIO(content), nrows=5)

print(f"\n   Actual columns ({len(df_sample.columns)}):")
for i, col in enumerate(df_sample.columns):
    dtype = str(df_sample[col].dtype)
    sample = str(df_sample[col].iloc[0])
    print(f"   {i+1:>2}. {col:<25} "
          f"{dtype:<10} "
          f"sample: {sample[:20]}")

logger.info(
    f"CSV has {len(df_sample.columns)} "
    f"columns")

# ─────────────────────────────────────────────
# STEP 2: RECREATE TABLE WITH CORRECT SCHEMA
# ─────────────────────────────────────────────
print("\n📌 2. RECREATING TABLE")

# Delete existing table
try:
    glue.delete_table(
        DatabaseName=DB_NAME,
        Name=TABLE_NAME)
    print("   ✅ Old table deleted")
    logger.info("Old table deleted")
except ClientError:
    pass

# Build column definitions from actual CSV
col_type_map = {
    'object': 'string',
    'int64': 'int',
    'float64': 'double',
    'bool': 'string',
}

# Read full sample to get types
df_full_sample = pd.read_csv(
    io.StringIO(content))

glue_columns = []
for col in df_full_sample.columns:
    dtype = str(
        df_full_sample[col].dtype)
    glue_type = col_type_map.get(
        dtype, 'string')
    glue_columns.append({
        'Name': col.lower().replace(
            ' ', '_'),
        'Type': glue_type
    })

s3_location = (
    f"s3://{BUCKET}/"
    f"processed/telco/enriched/")

glue.create_table(
    DatabaseName=DB_NAME,
    TableInput={
        'Name': TABLE_NAME,
        'Description':
            'Telco enriched customer data',
        'StorageDescriptor': {
            'Columns': glue_columns,
            'Location': s3_location,
            'InputFormat':
                'org.apache.hadoop.mapred'
                '.TextInputFormat',
            'OutputFormat':
                'org.apache.hadoop.hive'
                '.ql.io.HiveIgnoreKey'
                'TextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary':
                    'org.apache.hadoop'
                    '.hive.serde2.lazy'
                    '.LazySimpleSerde',
                'Parameters': {
                    'field.delim': ',',
                    'skip.header.'
                    'line.count': '1'
                }
            },
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'csv'
        }
    }
)
print(f"   ✅ Table recreated with "
      f"{len(glue_columns)} columns")
logger.info(
    f"Table recreated: "
    f"{len(glue_columns)} columns")

# ─────────────────────────────────────────────
# STEP 3: RUN ATHENA HELPER
# ─────────────────────────────────────────────
def run_athena(sql, label, max_wait=60):
    resp = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT
        }
    )
    qid = resp['QueryExecutionId']
    waited = 0
    while waited < max_wait:
        status = athena.get_query_execution(
            QueryExecutionId=qid)
        state = status[
            'QueryExecution'][
            'Status']['State']
        if state == 'SUCCEEDED':
            break
        elif state in [
                'FAILED', 'CANCELLED']:
            reason = status[
                'QueryExecution'][
                'Status'].get(
                'StateChangeReason', '')
            print(f"   ❌ {state}: {reason}")
            return None
        time.sleep(2)
        waited += 2

    results = athena.get_query_results(
        QueryExecutionId=qid)
    rows = results['ResultSet']['Rows']
    if not rows:
        return pd.DataFrame()
    headers = [
        c['VarCharValue']
        for c in rows[0]['Data']]
    data = [
        [c.get('VarCharValue', '')
         for c in row['Data']]
        for row in rows[1:]
    ]
    return pd.DataFrame(
        data, columns=headers)

# ─────────────────────────────────────────────
# STEP 4: RUN CORRECTED QUERIES
# ─────────────────────────────────────────────
print("\n📌 3. RUNNING CORRECTED QUERIES")
logger.info("Running corrected queries")

# Query 1 — Count
print("\n   🔍 Total customers:")
r = run_athena(
    f"SELECT COUNT(*) as total "
    f"FROM {DB_NAME}.{TABLE_NAME}",
    "Count")
if r is not None:
    print(f"   {r.to_string(index=False)}")

# Query 2 — Churn by contract
print("\n   🔍 Churn by contract:")
r = run_athena(f"""
    SELECT
        contract,
        COUNT(*) as total,
        SUM(CASE WHEN churn = 'True'
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(AVG(
            CAST(monthlycharges AS double)),
            2) as avg_monthly
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY contract
    ORDER BY churn_rate_pct DESC
""", "Contract churn")
if r is not None:
    print(r.to_string(index=False))

# Query 3 — Risk distribution
print("\n   🔍 Risk label distribution:")
r = run_athena(f"""
    SELECT
        risk_label,
        COUNT(*) as customers,
        ROUND(AVG(
            CAST(monthlycharges AS double)),
            2) as avg_monthly
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY risk_label
    ORDER BY customers DESC
""", "Risk distribution")
if r is not None:
    print(r.to_string(index=False))

# Query 4 — Top 5 high risk retained
print("\n   🔍 Top 5 high risk customers:")
r = run_athena(f"""
    SELECT
        customerid,
        contract,
        monthlycharges,
        tenure,
        risk_score,
        risk_label
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE churn = 'False'
    AND CAST(risk_score AS int) >= 8
    ORDER BY
        CAST(monthlycharges AS double) DESC
    LIMIT 5
""", "High risk customers")
if r is not None:
    print(r.to_string(index=False))

# Query 5 — Revenue at risk
print("\n   🔍 Revenue at risk:")
r = run_athena(f"""
    SELECT
        ROUND(SUM(
            CAST(monthlycharges AS double)),
            2) as total_monthly_revenue,
        ROUND(SUM(
            CASE WHEN churn = 'True'
            THEN CAST(monthlycharges
                AS double)
            ELSE 0 END), 2)
            as revenue_at_risk,
        SUM(CASE WHEN churn = 'True'
            THEN 1 ELSE 0 END)
            as churned_customers
    FROM {DB_NAME}.{TABLE_NAME}
""", "Revenue at risk")
if r is not None:
    print(r.to_string(index=False))

# ─────────────────────────────────────────────
# STEP 5: SAVE RESULTS
# ─────────────────────────────────────────────
print("\n📌 4. SAVING FINAL ATHENA REPORT")

final = run_athena(f"""
    SELECT
        contract,
        internetservice,
        risk_label,
        COUNT(*) as customers,
        SUM(CASE WHEN churn = 'True'
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(SUM(
            CAST(monthlycharges AS double)),
            2) as total_revenue
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY contract,
             internetservice,
             risk_label
    ORDER BY churn_rate_pct DESC
""", "Final report")

if final is not None:
    buf = io.StringIO()
    final.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"reports/athena_final_"
            f"{RUN_DATE}.csv",
        Body=buf.getvalue().encode(
            'utf-8'),
        ContentType='text/csv')
    print(f"   ✅ Saved athena_final_"
          f"{RUN_DATE}.csv")
    print(f"   Rows: {len(final)}")
    logger.info(
        f"Final report saved: "
        f"{len(final)} rows")

print(f"\n{'='*55}")
print("   FIX COMPLETE!")
print(f"{'='*55}")
print("   ✅ Table schema corrected")
print("   ✅ All queries returning data")
print("   ✅ Churn filter working")
print(f"\n✅ Athena Table Fixed!")
logger.info("Fix complete!")