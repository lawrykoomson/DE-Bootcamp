# ============================================
# WEEK 6 — MODULE 4: AWS Athena
# SQL Queries directly on S3 files!
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
            '../logs/module4.log',
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

# Athena output location
ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/")

s3 = boto3.client(
    's3', region_name=REGION)
athena = boto3.client(
    'athena', region_name=REGION)
glue = boto3.client(
    'glue', region_name=REGION)

print("=" * 55)
print("   MODULE 4 — AWS ATHENA")
print("   SQL QUERIES DIRECTLY ON S3 FILES!")
print("=" * 55)
print(f"\n   Bucket:  {BUCKET}")
print(f"   Region:  {REGION}")
print(f"   Output:  {ATHENA_OUTPUT}")

# ─────────────────────────────────────────────
# HELPER: RUN ATHENA QUERY
# ─────────────────────────────────────────────
def run_athena_query(
        sql, description,
        max_wait=60):
    """
    Execute an Athena query and wait
    for results. Returns a DataFrame.
    """
    logger.info(
        f"Running Athena query: "
        f"{description}")

    try:
        response = athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                'OutputLocation': ATHENA_OUTPUT
            }
        )
        query_id = response[
            'QueryExecutionId']
        logger.info(
            f"Query ID: {query_id}")

        # Wait for completion
        waited = 0
        while waited < max_wait:
            status = athena.get_query_execution(
                QueryExecutionId=query_id
            )
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
                    'StateChangeReason',
                    'Unknown')
                logger.error(
                    f"Query {state}: {reason}")
                print(f"   ❌ Query {state}: "
                      f"{reason}")
                return None
            else:
                time.sleep(2)
                waited += 2

        # Get results
        results = athena.get_query_results(
            QueryExecutionId=query_id)

        # Parse results into DataFrame
        rows = results['ResultSet']['Rows']
        if not rows:
            return pd.DataFrame()

        headers = [
            col['VarCharValue']
            for col in rows[0]['Data']]
        data = []
        for row in rows[1:]:
            data.append([
                col.get('VarCharValue', '')
                for col in row['Data']])

        df = pd.DataFrame(data,
                         columns=headers)
        logger.info(
            f"Query succeeded: "
            f"{len(df)} rows")
        return df

    except ClientError as e:
        logger.error(
            f"Athena error: {e}")
        print(f"   ❌ Error: {e}")
        return None

# ─────────────────────────────────────────────
# STEP 1: CREATE GLUE DATABASE
# ─────────────────────────────────────────────
print("\n📌 1. CREATING GLUE DATABASE")
logger.info("Creating Glue database")

DB_NAME = "telco_de_bootcamp"

try:
    glue.create_database(
        DatabaseInput={
            'Name': DB_NAME,
            'Description':
                'Telco DE Bootcamp Database — '
                'Lawrence Koomson'
        }
    )
    print(f"   ✅ Database created: {DB_NAME}")
    logger.info(
        f"Glue DB created: {DB_NAME}")

except ClientError as e:
    if 'AlreadyExistsException' in str(e):
        print(f"   ✅ Database already "
              f"exists: {DB_NAME}")
    else:
        print(f"   ❌ Error: {e}")
        logger.error(f"Glue DB error: {e}")

# ─────────────────────────────────────────────
# STEP 2: CREATE GLUE TABLE
# ─────────────────────────────────────────────
print("\n📌 2. CREATING GLUE TABLE")
logger.info("Creating Glue table")

TABLE_NAME = "telco_customers"

# Find the enriched data path
s3_data_location = (
    f"s3://{BUCKET}/processed/telco/enriched/")

try:
    glue.delete_table(
        DatabaseName=DB_NAME,
        Name=TABLE_NAME)
    logger.info(
        "Deleted existing table")
except ClientError:
    pass

try:
    glue.create_table(
        DatabaseName=DB_NAME,
        TableInput={
            'Name': TABLE_NAME,
            'Description':
                'Telco customer churn data',
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'customerid',
                     'Type': 'string'},
                    {'Name': 'gender',
                     'Type': 'string'},
                    {'Name': 'seniorcitizen',
                     'Type': 'string'},
                    {'Name': 'partner',
                     'Type': 'string'},
                    {'Name': 'dependents',
                     'Type': 'string'},
                    {'Name': 'tenure',
                     'Type': 'int'},
                    {'Name': 'internetservice',
                     'Type': 'string'},
                    {'Name': 'contract',
                     'Type': 'string'},
                    {'Name': 'paymentmethod',
                     'Type': 'string'},
                    {'Name': 'monthlycharges',
                     'Type': 'double'},
                    {'Name': 'totalcharges',
                     'Type': 'double'},
                    {'Name': 'churn',
                     'Type': 'boolean'},
                    {'Name': 'tenure_group',
                     'Type': 'string'},
                    {'Name': 'charge_band',
                     'Type': 'string'},
                    {'Name': 'risk_score',
                     'Type': 'int'},
                    {'Name': 'risk_label',
                     'Type': 'string'},
                    {'Name': 'lifetime_value',
                     'Type': 'double'},
                    {'Name': 'annual_value',
                     'Type': 'double'},
                ],
                'Location': s3_data_location,
                'InputFormat':
                    'org.apache.hadoop.'
                    'mapred.TextInputFormat',
                'OutputFormat':
                    'org.apache.hadoop.hive.'
                    'ql.io.HiveIgnoreKeyText'
                    'OutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary':
                        'org.apache.hadoop.hive.'
                        'serde2.lazy.LazySimple'
                        'SerDe',
                    'Parameters': {
                        'field.delim': ',',
                        'skip.header.'
                        'line.count': '1'
                    }
                },
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'has_encrypted_data': 'false'
            }
        }
    )
    print(f"   ✅ Table created: "
          f"{DB_NAME}.{TABLE_NAME}")
    print(f"   Location: {s3_data_location}")
    logger.info(
        f"Glue table created: {TABLE_NAME}")

except ClientError as e:
    print(f"   ❌ Table creation error: {e}")
    logger.error(
        f"Table creation failed: {e}")

# ─────────────────────────────────────────────
# STEP 3: RUN ATHENA QUERIES
# ─────────────────────────────────────────────
print("\n📌 3. RUNNING ATHENA SQL QUERIES")
print("   Querying S3 files with SQL!")
logger.info("Running Athena queries")

# Query 1 — Count records
print("\n   🔍 Query 1: Record count")
result = run_athena_query(
    f"""
    SELECT COUNT(*) as total_customers
    FROM {DB_NAME}.{TABLE_NAME}
    """,
    "Count records")
if result is not None:
    print(result.to_string(index=False))

# Query 2 — Churn by contract
print("\n   🔍 Query 2: Churn by contract")
result = run_athena_query(
    f"""
    SELECT
        contract,
        COUNT(*) as total,
        SUM(CASE WHEN churn = true
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN churn = true
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(AVG(monthlycharges), 2)
            as avg_monthly
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY contract
    ORDER BY churn_rate_pct DESC
    """,
    "Churn by contract")
if result is not None:
    print(result.to_string(index=False))

# Query 3 — Risk distribution
print("\n   🔍 Query 3: Risk distribution")
result = run_athena_query(
    f"""
    SELECT
        risk_label,
        COUNT(*) as customers,
        ROUND(AVG(monthlycharges), 2)
            as avg_monthly,
        ROUND(SUM(monthlycharges), 2)
            as total_revenue
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE churn = false
    GROUP BY risk_label
    ORDER BY customers DESC
    """,
    "Risk distribution")
if result is not None:
    print(result.to_string(index=False))

# Query 4 — Top intervention targets
print("\n   🔍 Query 4: "
      "Top intervention targets")
result = run_athena_query(
    f"""
    SELECT
        customerid,
        contract,
        internetservice,
        monthlycharges,
        tenure,
        risk_score,
        risk_label,
        lifetime_value
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE churn = false
    AND risk_score >= 8
    ORDER BY monthlycharges DESC
    LIMIT 10
    """,
    "Intervention targets")
if result is not None:
    print(result.to_string(index=False))

# ─────────────────────────────────────────────
# STEP 4: SAVE ATHENA RESULTS TO S3
# ─────────────────────────────────────────────
print("\n📌 4. SAVING ATHENA RESULTS")
logger.info("Saving Athena query results")

# Run a comprehensive analysis query
analysis = run_athena_query(
    f"""
    SELECT
        contract,
        internetservice,
        risk_label,
        COUNT(*) as customers,
        SUM(CASE WHEN churn = true
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN churn = true
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(AVG(monthlycharges), 2)
            as avg_monthly,
        ROUND(SUM(monthlycharges), 2)
            as total_revenue
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY contract,
             internetservice,
             risk_label
    ORDER BY churn_rate_pct DESC
    """,
    "Comprehensive analysis")

if analysis is not None:
    buf = io.StringIO()
    analysis.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"reports/athena_analysis_"
            f"{RUN_DATE}.csv",
        Body=buf.getvalue().encode(
            'utf-8'),
        ContentType='text/csv')
    print(f"   ✅ Saved: "
          f"athena_analysis_{RUN_DATE}.csv")
    print(f"   Rows: {len(analysis)}")
    logger.info(
        f"Athena results saved: "
        f"{len(analysis)} rows")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   Athena    → SQL directly on S3")
print("   Glue DB   → Metadata catalogue")
print("   Glue Table → Schema for S3 files")
print("   No server → Serverless queries!")
print("   Pay per   → Query (5TB free/month)")
print(f"\n✅ Module 4 Complete!")
logger.info("Module 4 Athena complete")