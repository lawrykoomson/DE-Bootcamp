# ============================================
# WEEK 6 — CAPSTONE: AWS DE Pipeline
# S3 Data Lake + Athena Analytics
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
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "AWS Telco DE Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"

with open("../data/aws_config.json") as f:
    config = json.load(f)

BUCKET = config['bucket_name']
REGION = config['region']
RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
RUN_TS = datetime.now().strftime(
    "%Y%m%d_%H%M%S")
ATHENA_OUTPUT = (
    f"s3://{BUCKET}/athena-results/")
DB_NAME = "telco_de_bootcamp"
TABLE_NAME = "telco_customers"

os.makedirs("../logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/capstone.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

s3 = boto3.client(
    's3', region_name=REGION)
athena = boto3.client(
    'athena', region_name=REGION)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def run_athena(sql, label,
               max_wait=120):
    """Run Athena query and return DataFrame."""
    logger.info(f"Query: {label}")
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
            logger.error(
                f"Query failed: {reason}")
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
        f"Succeeded: {len(df)} rows")
    return df

def save_to_s3(df, key):
    """Save DataFrame to S3 as CSV."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode(
            'utf-8'),
        ContentType='text/csv')
    return len(buf.getvalue())

def count_s3_objects(prefix=""):
    """Count objects in S3 bucket."""
    paginator = s3.get_paginator(
        'list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET, Prefix=prefix)
    total = 0
    size = 0
    for page in pages:
        objects = page.get('Contents', [])
        total += len(objects)
        size += sum(
            o['Size'] for o in objects)
    return total, size

# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
print("=" * 55)
print(f"   {PIPELINE_NAME}")
print(f"   Version: {PIPELINE_VERSION}")
print(f"   Author:  {AUTHOR}")
print("=" * 55)

start_time = datetime.now()
logger.info("=" * 40)
logger.info(
    f"PIPELINE STARTED: {PIPELINE_NAME}")
logger.info("=" * 40)

stats = {
    "status": "RUNNING",
    "stages_completed": 0,
    "errors": []
}

# ─────────────────────────────────────────────
# STAGE 1: INFRASTRUCTURE CHECK
# ─────────────────────────────────────────────
print("\n🔧 STAGE 1: INFRASTRUCTURE CHECK")
logger.info("Stage 1: Infrastructure check")

# Check S3 bucket
try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"   ✅ S3 Bucket: {BUCKET}")
    logger.info(f"Bucket OK: {BUCKET}")
except ClientError:
    print(f"   ❌ Bucket not found!")
    exit(1)

# Count existing objects
total_obj, total_size = count_s3_objects()
print(f"   ✅ Objects:   {total_obj}")
print(f"   ✅ Size:      "
      f"{total_size/1024:.1f} KB")

# Verify Athena table
r = run_athena(
    f"SELECT COUNT(*) as n "
    f"FROM {DB_NAME}.{TABLE_NAME}",
    "Verify table")
if r is not None:
    print(f"   ✅ Athena table: "
          f"{r['n'].iloc[0]} rows")
    stats["stages_completed"] += 1
    logger.info("Stage 1 complete")
else:
    print("   ❌ Athena table not found!")
    exit(1)

# ─────────────────────────────────────────────
# STAGE 2: EXECUTIVE ANALYTICS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 2: EXECUTIVE ANALYTICS")
logger.info("Stage 2: Executive analytics")

# KPIs
kpis = run_athena(f"""
    SELECT
        COUNT(*) as total_customers,
        SUM(CASE WHEN Churn = 'True'
            THEN 1 ELSE 0 END)
            as total_churned,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 2)
            as churn_rate_pct,
        ROUND(SUM(MonthlyCharges), 2)
            as monthly_revenue,
        ROUND(SUM(
            CASE WHEN Churn = 'True'
            THEN MonthlyCharges
            ELSE 0 END), 2)
            as revenue_at_risk,
        ROUND(AVG(MonthlyCharges), 2)
            as avg_monthly_charge,
        ROUND(AVG(tenure), 1)
            as avg_tenure_months
    FROM {DB_NAME}.{TABLE_NAME}
""", "Executive KPIs")

if kpis is not None:
    print(f"\n   📊 Executive KPIs:")
    for col in kpis.columns:
        val = kpis[col].iloc[0]
        if col in ['monthly_revenue',
                   'revenue_at_risk']:
            print(f"   {col:<28} "
                  f"${float(val):,.2f}")
        elif 'pct' in col:
            print(f"   {col:<28} {val}%")
        else:
            print(f"   {col:<28} "
                  f"{float(val):,.0f}")
    stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 3: SEGMENT ANALYSIS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 3: SEGMENT ANALYSIS")
logger.info("Stage 3: Segment analysis")

# Churn by contract
contract_churn = run_athena(f"""
    SELECT
        Contract,
        COUNT(*) as customers,
        SUM(CASE WHEN Churn = 'True'
            THEN 1 ELSE 0 END) as churned,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(AVG(MonthlyCharges), 2)
            as avg_monthly,
        ROUND(SUM(MonthlyCharges), 2)
            as total_revenue
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY Contract
    ORDER BY churn_rate_pct DESC
""", "Churn by contract")

if contract_churn is not None:
    print(f"\n   📊 Churn by Contract:")
    print(contract_churn.to_string(
        index=False))

# Risk matrix
risk_matrix = run_athena(f"""
    SELECT
        Contract,
        InternetService,
        COUNT(*) as customers,
        ROUND(
            SUM(CASE WHEN Churn = 'True'
                THEN 1.0 ELSE 0 END) /
            COUNT(*) * 100, 1)
            as churn_rate_pct,
        ROUND(SUM(
            CASE WHEN Churn = 'True'
            THEN MonthlyCharges
            ELSE 0 END), 2)
            as revenue_at_risk
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY Contract, InternetService
    ORDER BY churn_rate_pct DESC
    LIMIT 6
""", "Risk matrix")

if risk_matrix is not None:
    print(f"\n   📊 Risk Matrix:")
    print(risk_matrix.to_string(
        index=False))
    stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 4: INTERVENTION TARGETING
# ─────────────────────────────────────────────
print("\n🔧 STAGE 4: INTERVENTION TARGETING")
logger.info("Stage 4: Interventions")

interventions = run_athena(f"""
    SELECT
        CASE
            WHEN risk_score >= 8
            THEN 'Immediate call'
            WHEN risk_score >= 6
            THEN 'Email campaign'
            WHEN risk_score >= 4
            THEN 'Auto-pay offer'
            ELSE 'Standard programme'
        END as action,
        COUNT(*) as customers,
        ROUND(SUM(MonthlyCharges), 2)
            as monthly_at_risk,
        ROUND(SUM(MonthlyCharges)*12, 2)
            as annual_at_risk,
        ROUND(AVG(MonthlyCharges), 2)
            as avg_monthly
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE Churn = 'False'
    GROUP BY CASE
        WHEN risk_score >= 8
        THEN 'Immediate call'
        WHEN risk_score >= 6
        THEN 'Email campaign'
        WHEN risk_score >= 4
        THEN 'Auto-pay offer'
        ELSE 'Standard programme'
    END
    ORDER BY annual_at_risk DESC
""", "Intervention summary")

if interventions is not None:
    print(f"\n   📊 Intervention Targets:")
    print(interventions.to_string(
        index=False))

# Top priority customers
top_customers = run_athena(f"""
    SELECT
        customerid,
        contract,
        internetservice,
        monthlycharges,
        tenure,
        risk_score,
        lifetime_value
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE churn = 'False'
    AND risk_score >= 8
    ORDER BY monthlycharges DESC
    LIMIT 10
""", "Top priority customers")

if top_customers is not None:
    print(f"\n   📊 Top 10 Priority Customers:")
    print(top_customers.to_string(
        index=False))
    stats["stages_completed"] += 1

# ─────────────────────────────────────────────
# STAGE 5: SAVE ALL OUTPUTS TO S3
# ─────────────────────────────────────────────
print("\n🔧 STAGE 5: SAVING OUTPUTS TO S3")
logger.info("Stage 5: Saving outputs")

outputs = []

if contract_churn is not None:
    key = (f"reports/capstone/"
           f"contract_churn_{RUN_DATE}.csv")
    save_to_s3(contract_churn, key)
    outputs.append(key)
    print(f"   ✅ contract_churn_{RUN_DATE}.csv")

if risk_matrix is not None:
    key = (f"reports/capstone/"
           f"risk_matrix_{RUN_DATE}.csv")
    save_to_s3(risk_matrix, key)
    outputs.append(key)
    print(f"   ✅ risk_matrix_{RUN_DATE}.csv")

if interventions is not None:
    key = (f"reports/capstone/"
           f"interventions_{RUN_DATE}.csv")
    save_to_s3(interventions, key)
    outputs.append(key)
    print(f"   ✅ interventions_{RUN_DATE}.csv")

if top_customers is not None:
    key = (f"reports/capstone/"
           f"top_customers_{RUN_DATE}.csv")
    save_to_s3(top_customers, key)
    outputs.append(key)
    print(f"   ✅ top_customers_{RUN_DATE}.csv")

# ─────────────────────────────────────────────
# STAGE 6: FINAL REPORT
# ─────────────────────────────────────────────
print("\n🔧 STAGE 6: FINAL PIPELINE REPORT")
logger.info("Stage 6: Final report")

duration = (
    datetime.now() - start_time
).total_seconds()
stats["status"] = "SUCCESS"

final_obj, final_size = count_s3_objects()

report = {
    "pipeline_name": PIPELINE_NAME,
    "version": PIPELINE_VERSION,
    "author": AUTHOR,
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "duration_seconds": round(
        duration, 2),
    "aws_config": {
        "bucket": BUCKET,
        "region": REGION,
        "athena_db": DB_NAME,
        "athena_table": TABLE_NAME
    },
    "pipeline_stats": stats,
    "data_lake": {
        "total_objects": final_obj,
        "total_size_kb": round(
            final_size/1024, 1)
    },
    "business_summary": {
        "total_customers": int(
            kpis['total_customers'].iloc[0]
        ) if kpis is not None else 0,
        "churn_rate_pct": float(
            kpis['churn_rate_pct'].iloc[0]
        ) if kpis is not None else 0,
        "monthly_revenue": float(
            kpis['monthly_revenue'].iloc[0]
        ) if kpis is not None else 0,
        "revenue_at_risk": float(
            kpis['revenue_at_risk'].iloc[0]
        ) if kpis is not None else 0,
    },
    "outputs_saved": outputs,
    "aws_services_used": [
        "S3 — Simple Storage Service",
        "Glue — Data Catalog",
        "Athena — Serverless SQL",
        "IAM — Identity and Access"
    ]
}

report_key = (
    f"reports/capstone/"
    f"pipeline_report_{RUN_DATE}.json")
s3.put_object(
    Bucket=BUCKET,
    Key=report_key,
    Body=json.dumps(
        report, indent=4).encode('utf-8'),
    ContentType='application/json')
print(f"   ✅ pipeline_report_"
      f"{RUN_DATE}.json")

logger.info(
    f"Pipeline complete in "
    f"{duration:.2f}s")

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   WEEK 6 AWS CAPSTONE RESULTS")
print("=" * 55)
print(f"\n   Pipeline: {PIPELINE_NAME}")
print(f"   Status:   ✅ SUCCESS")
print(f"   Duration: {duration:.2f}s")
print(f"   Stages:   "
      f"{stats['stages_completed']}/4 ✅")

if kpis is not None:
    print(f"\n   📊 Business Summary:")
    print(f"   Customers:  "
          f"{kpis['total_customers'].iloc[0]:>10}")
    print(f"   Churn Rate: "
          f"{kpis['churn_rate_pct'].iloc[0]:>9}%")
    print(f"   Revenue:    "
          f"${float(kpis['monthly_revenue'].iloc[0]):>10,.2f}/mo")
    print(f"   At Risk:    "
          f"${float(kpis['revenue_at_risk'].iloc[0]):>10,.2f}/mo")

print(f"\n   ☁️  AWS Data Lake:")
print(f"   Bucket:  {BUCKET}")
print(f"   Objects: {final_obj}")
print(f"   Size:    {final_size/1024:.1f} KB")

print(f"\n   🔧 Services Used:")
for svc in report['aws_services_used']:
    print(f"   ✅ {svc}")

print(f"\n   📁 Outputs: "
      f"{len(outputs)} files saved")

print(f"\n✅ Week 6 AWS Capstone Complete!")
print(f"   Cloud DE Pipeline — "
      f"Production Ready!")
logger.info("Week 6 capstone complete!")