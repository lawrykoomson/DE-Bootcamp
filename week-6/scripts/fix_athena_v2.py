# ============================================
# WEEK 6 — FIX ATHENA V2
# Use Athena DDL to create table correctly
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
            '../logs/fix_v2.log',
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
print("   FIX ATHENA V2 — DDL METHOD")
print("=" * 55)

# ─────────────────────────────────────────────
# ATHENA QUERY RUNNER
# ─────────────────────────────────────────────
def run_athena(sql, label,
               max_wait=120):
    """Run Athena query and return results."""
    print(f"\n   ▶ {label}")
    logger.info(f"Running: {label}")

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
            logger.info(
                f"Succeeded: {label}")
            break
        elif state in [
                'FAILED', 'CANCELLED']:
            reason = status[
                'QueryExecution'][
                'Status'].get(
                'StateChangeReason', '')
            print(f"   ❌ {state}: {reason}")
            logger.error(
                f"{state}: {reason}")
            return None

        time.sleep(3)
        waited += 3

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
    df = pd.DataFrame(
        data, columns=headers)
    logger.info(
        f"Result: {len(df)} rows")
    return df

# ─────────────────────────────────────────────
# STEP 1: DELETE OLD TABLE VIA GLUE
# ─────────────────────────────────────────────
print("\n📌 1. DELETING OLD TABLE")

try:
    glue.delete_table(
        DatabaseName=DB_NAME,
        Name=TABLE_NAME)
    print("   ✅ Old table deleted")
    logger.info("Old table deleted")
except ClientError:
    print("   ✅ No existing table")

# ─────────────────────────────────────────────
# STEP 2: CREATE TABLE VIA ATHENA DDL
# ─────────────────────────────────────────────
print("\n📌 2. CREATING TABLE VIA ATHENA DDL")

S3_LOCATION = (
    f"s3://{BUCKET}/"
    f"processed/telco/enriched/")

create_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS
{DB_NAME}.{TABLE_NAME} (
    customerid       STRING,
    gender           STRING,
    seniorcitizen    STRING,
    partner          STRING,
    dependents       STRING,
    tenure           INT,
    phoneservice     STRING,
    multilines    STRING,
    internetService  STRING,
    onlineSecurity   STRING,
    onlineBackup     STRING,
    deviceProtection STRING,
    techSupport      STRING,
    streamingTV      STRING,
    streamingMovies  STRING,
    contract         STRING,
    paperlessBilling STRING,
    paymentMethod    STRING,
    monthlyCharges   DOUBLE,
    totalCharges     DOUBLE,
    churn            STRING,
    tenure_group     STRING,
    charge_band      STRING,
    risk_score       INT,
    risk_label       STRING,
    lifetime_value   DOUBLE,
    annual_value     DOUBLE,
    processed_at     STRING,
    pipeline_version STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{S3_LOCATION}'
TBLPROPERTIES (
    'skip.header.line.count'='1'
)
"""

result = run_athena(
    create_table_sql,
    "CREATE TABLE via DDL")
if result is not None:
    print("   ✅ Table created via DDL!")
    logger.info("Table created via DDL")

# ─────────────────────────────────────────────
# STEP 3: VERIFY TABLE
# ─────────────────────────────────────────────
print("\n📌 3. VERIFY TABLE")

r = run_athena(
    f"SELECT COUNT(*) as total "
    f"FROM {DB_NAME}.{TABLE_NAME}",
    "Count all rows")
if r is not None:
    print(f"   Total rows: "
          f"{r['total'].iloc[0]}")

r = run_athena(
    f"SELECT * FROM "
    f"{DB_NAME}.{TABLE_NAME} LIMIT 3",
    "Sample rows")
if r is not None:
    print(f"\n   Sample data:")
    print(r[[
        'customerid', 'contract',
        'monthlycharges', 'churn',
        'risk_label']].to_string(
        index=False))

# ─────────────────────────────────────────────
# STEP 4: RUN BUSINESS QUERIES
# ─────────────────────────────────────────────
print("\n📌 4. RUNNING BUSINESS QUERIES")

# Churn by contract
r = run_athena(f"""
    SELECT
        Contract,
        COUNT(*) as total,
        SUM(CASE WHEN Churn = 'True'
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(AVG(monthlycharges), 2)
            as avg_monthly
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
""", "Churn by contract")
if r is not None:
    print(r.to_string(index=False))

# Risk distribution
r = run_athena(f"""
    SELECT
        risk_label,
        COUNT(*) as customers,
        ROUND(AVG(monthlycharges), 2)
            as avg_monthly,
        ROUND(SUM(monthlycharges), 2)
            as total_revenue
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE Churn = 'False'
    GROUP BY risk_label
    ORDER BY customers DESC
""", "Risk distribution — retained only")
if r is not None:
    print(r.to_string(index=False))

# Revenue at risk
r = run_athena(f"""
    SELECT
        ROUND(SUM(MonthlyCharges), 2)
            as total_revenue,
        ROUND(SUM(
            CASE WHEN Churn = 'True'
            THEN MonthlyCharges
            ELSE 0 END), 2)
            as revenue_at_risk,
        SUM(CASE WHEN Churn = 'True'
            THEN 1 ELSE 0 END)
            as churned_customers,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 2)
            as churn_rate_pct
    FROM {DB_NAME}.{TABLE_NAME}
""", "Revenue at risk summary")
if r is not None:
    print(r.to_string(index=False))

# Top intervention targets
r = run_athena(f"""
    SELECT
        customerid,
        contract,
        internetService,
        monthlycharges,
        tenure,
        risk_score,
        risk_label
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE Churn = 'False'
    AND risk_score >= 8
    ORDER BY monthlycharges DESC
    LIMIT 8
""", "Top intervention targets")
if r is not None:
    print(r.to_string(index=False))

# ─────────────────────────────────────────────
# STEP 5: SAVE FINAL REPORT TO S3
# ─────────────────────────────────────────────
print("\n📌 5. SAVING FINAL ATHENA REPORT")

final = run_athena(f"""
    SELECT
        contract,
        internetService,
        risk_label,
        COUNT(*) as customers,
        SUM(CASE WHEN Churn = 'True'
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(SUM(monthlycharges), 2)
            as total_revenue,
        ROUND(SUM(
            CASE WHEN Churn = 'True'
            THEN monthlycharges
            ELSE 0 END), 2)
            as revenue_at_risk
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY contract,
             internetService,
             risk_label
    ORDER BY churn_rate_pct DESC
""", "Final comprehensive report")

if final is not None and len(final) > 0:
    buf = io.StringIO()
    final.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"reports/athena_final_"
            f"{RUN_DATE}.csv",
        Body=buf.getvalue().encode(
            'utf-8'),
        ContentType='text/csv')
    print(f"\n   ✅ Saved: "
          f"athena_final_{RUN_DATE}.csv")
    print(f"   Rows: {len(final)}")
    logger.info(
        f"Final report: {len(final)} rows")

print(f"\n{'='*55}")
print("   MODULE 4 COMPLETE!")
print(f"{'='*55}")
print("   DDL method    → Most reliable way")
print("   Athena CREATE → Defines schema")
print("   SerDe auto    → Athena handles it")
print("   SQL on S3     → No server needed!")
print(f"\n✅ Athena Working!")
logger.info("Fix v2 complete!")