# ============================================
# WEEK 7 — CAPSTONE: AWS Advanced DE Pipeline
# Lambda + Glue + Step Functions + S3
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import json
import logging
import os
import time
import io
from datetime import datetime
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "AWS Advanced DE Pipeline"
PIPELINE_VERSION = "2.0.0"
AUTHOR = "Lawrence Koomson"

with open(
        "../../week-6/data/aws_config.json"
) as f:
    config = json.load(f)

with open(
        "../data/lambda_config.json"
) as f:
    lambda_config = json.load(f)

BUCKET = config['bucket_name']
REGION = config['region']
ROLE_ARN = lambda_config['role_arn']
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
lambda_client = boto3.client(
    'lambda', region_name=REGION)
sfn = boto3.client(
    'stepfunctions', region_name=REGION)
glue = boto3.client(
    'glue', region_name=REGION)
athena = boto3.client(
    'athena', region_name=REGION)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def count_s3_objects(prefix=""):
    paginator = s3.get_paginator(
        'list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET, Prefix=prefix)
    total = 0
    size = 0
    for page in pages:
        objs = page.get('Contents', [])
        total += len(objs)
        size += sum(
            o['Size'] for o in objs)
    return total, size

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
            return None
        time.sleep(3)
        waited += 3
    results = athena.get_query_results(
        QueryExecutionId=qid)
    rows = results['ResultSet']['Rows']
    if not rows:
        return None
    headers = [
        c['VarCharValue']
        for c in rows[0]['Data']]
    data = [
        [c.get('VarCharValue', '')
         for c in row['Data']]
        for row in rows[1:]
    ]
    import pandas as pd
    return pd.DataFrame(
        data, columns=headers)

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
    f"CAPSTONE STARTED: {PIPELINE_NAME}")
logger.info("=" * 40)

stats = {
    "status": "RUNNING",
    "stages": {},
    "errors": []
}

# ─────────────────────────────────────────────
# STAGE 1: INFRASTRUCTURE HEALTH CHECK
# ─────────────────────────────────────────────
print("\n🔧 STAGE 1: INFRASTRUCTURE CHECK")
logger.info("Stage 1: Infrastructure check")

# S3 check
try:
    s3.head_bucket(Bucket=BUCKET)
    total_obj, total_size = count_s3_objects()
    print(f"   ✅ S3 Bucket: {BUCKET}")
    print(f"   ✅ Objects:   {total_obj}")
    print(f"   ✅ Size:      "
          f"{total_size/1024:.1f} KB")
    stats["stages"]["s3"] = "✅ HEALTHY"
except ClientError:
    print(f"   ❌ S3 Bucket not found!")
    exit(1)

# Lambda check
funcs = lambda_client.list_functions()
bootcamp_funcs = [
    f for f in funcs['Functions']
    if 'de-bootcamp' in
    f['FunctionName']]
print(f"   ✅ Lambda functions: "
      f"{len(bootcamp_funcs)}")
stats["stages"]["lambda"] = (
    f"✅ {len(bootcamp_funcs)} functions")

# Glue check
jobs = glue.list_jobs()
bootcamp_jobs = [
    j for j in jobs.get('JobNames', [])
    if 'bootcamp' in j.lower()
    or 'telco' in j.lower()]
print(f"   ✅ Glue jobs:     "
      f"{len(bootcamp_jobs)}")
stats["stages"]["glue"] = (
    f"✅ {len(bootcamp_jobs)} jobs")

# Step Functions check
machines = sfn.list_state_machines()
bootcamp_sms = [
    sm for sm in machines[
        'stateMachines']
    if 'bootcamp' in sm['name']]
print(f"   ✅ State machines: "
      f"{len(bootcamp_sms)}")
stats["stages"]["stepfunctions"] = (
    f"✅ {len(bootcamp_sms)} machines")

logger.info("Stage 1 complete")

# ─────────────────────────────────────────────
# STAGE 2: RUN STEP FUNCTIONS PIPELINE
# ─────────────────────────────────────────────
print("\n🔧 STAGE 2: STEP FUNCTIONS PIPELINE")
logger.info("Stage 2: Step Functions pipeline")

if bootcamp_sms:
    SM_ARN = bootcamp_sms[0][
        'stateMachineArn']

    pipeline_input = {
        "bucket": BUCKET,
        "prefix": "raw/customers/",
        "pipeline_name": PIPELINE_NAME,
        "run_date": RUN_DATE,
        "run_id": RUN_TS
    }

    execution = sfn.start_execution(
        stateMachineArn=SM_ARN,
        name=f"capstone-{RUN_TS}",
        input=json.dumps(pipeline_input)
    )
    EXEC_ARN = execution['executionArn']
    print(f"   ✅ Pipeline started: "
          f"capstone-{RUN_TS}")

    # Monitor
    max_wait = 120
    waited = 0
    sfn_status = None

    while waited < max_wait:
        exec_info = sfn.describe_execution(
            executionArn=EXEC_ARN)
        sfn_status = exec_info['status']
        if sfn_status in [
                'SUCCEEDED', 'FAILED',
                'TIMED_OUT', 'ABORTED']:
            break
        time.sleep(10)
        waited += 10

    if sfn_status == 'SUCCEEDED':
        output = json.loads(
            exec_info.get('output', '{}'))
        metrics = output.get('metrics', {})
        print(f"   ✅ Pipeline SUCCEEDED!")
        print(f"   Files:    "
              f"{metrics.get('files_processed', 0)}")
        print(f"   Records:  "
              f"{metrics.get('records_extracted', 0):,}")
        print(f"   Valid:    "
              f"{metrics.get('records_valid', 0):,}")
        stats["stages"]["pipeline"] = (
            "✅ SUCCEEDED")
        logger.info(
            "Step Functions succeeded")
    else:
        print(f"   ⚠️ Pipeline: {sfn_status}")
        stats["stages"]["pipeline"] = (
            f"⚠️ {sfn_status}")

# ─────────────────────────────────────────────
# STAGE 3: INVOKE LAMBDA ANALYTICS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 3: LAMBDA ANALYTICS")
logger.info("Stage 3: Lambda analytics")

# Invoke churn detector
test_event = {
    "bucket": BUCKET,
    "key": "raw/customers/sample_customers.csv"
}

try:
    response = lambda_client.invoke(
        FunctionName='de-bootcamp-churn_detector',
        InvocationType='RequestResponse',
        Payload=json.dumps(test_event)
    )
    result = json.loads(
        response['Payload'].read())
    analysis = result.get('analysis', {})

    print(f"   ✅ Churn Detector Lambda:")
    print(f"   Customers: "
          f"{analysis.get('total_customers')}")
    print(f"   Churned:   "
          f"{analysis.get('churned')}")
    print(f"   Churn rate: "
          f"{analysis.get('churn_rate_pct')}%")
    stats["stages"]["lambda_analytics"] = (
        f"✅ {analysis.get('churn_rate_pct')}% churn")
    logger.info(
        "Lambda analytics complete")

except ClientError as e:
    print(f"   ❌ Lambda error: {e}")
    stats["stages"]["lambda_analytics"] = (
        "❌ FAILED")

# ─────────────────────────────────────────────
# STAGE 4: ATHENA ANALYTICS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 4: ATHENA ANALYTICS")
logger.info("Stage 4: Athena analytics")

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
            as revenue_at_risk
    FROM {DB_NAME}.{TABLE_NAME}
""", "KPIs")

if kpis is not None:
    print(f"   ✅ Athena KPIs:")
    print(f"   Customers:  "
          f"{kpis['total_customers'].iloc[0]:>10}")
    print(f"   Churn rate: "
          f"{kpis['churn_rate_pct'].iloc[0]:>9}%")
    print(f"   Revenue:    "
          f"${float(kpis['monthly_revenue'].iloc[0]):>10,.2f}")
    print(f"   At risk:    "
          f"${float(kpis['revenue_at_risk'].iloc[0]):>10,.2f}")
    stats["stages"]["athena"] = (
        f"✅ {kpis['churn_rate_pct'].iloc[0]}% churn")
    logger.info("Athena analytics complete")

# ─────────────────────────────────────────────
# STAGE 5: EVENT-DRIVEN TEST
# ─────────────────────────────────────────────
print("\n🔧 STAGE 5: EVENT-DRIVEN TEST")
logger.info("Stage 5: Event-driven test")

test_csv = f"""customerID,Contract,MonthlyCharges,Churn
CAP001,Month-to-month,95.50,1
CAP002,One year,65.00,0
CAP003,Month-to-month,105.00,1
CAP004,Two year,45.00,0
CAP005,Month-to-month,78.00,0
CAP006,Month-to-month,88.00,1
CAP007,One year,72.00,0
CAP008,Two year,55.00,0
"""

test_key = (
    f"raw/incoming/capstone_test_{RUN_TS}.csv")

s3.put_object(
    Bucket=BUCKET,
    Key=test_key,
    Body=test_csv.encode('utf-8'),
    ContentType='text/csv')

print(f"   ✅ Test file uploaded: {test_key}")
print(f"   ⏳ Waiting for auto-trigger...")
time.sleep(20)

processed_key = test_key.replace(
    "raw/incoming/",
    "processed/auto-processed/")

try:
    s3.head_object(
        Bucket=BUCKET,
        Key=processed_key)
    print(f"   ✅ AUTO-TRIGGERED! "
          f"File processed automatically!")
    stats["stages"]["event_driven"] = (
        "✅ AUTO-TRIGGERED")
    logger.info(
        "Event-driven trigger working!")
except ClientError:
    print(f"   ⏳ Still processing...")
    stats["stages"]["event_driven"] = (
        "⏳ Processing")

# ─────────────────────────────────────────────
# STAGE 6: FINAL REPORT
# ─────────────────────────────────────────────
print("\n🔧 STAGE 6: FINAL REPORT")
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
    "duration_seconds": round(duration, 2),
    "aws_config": {
        "bucket": BUCKET,
        "region": REGION,
        "lambda_functions": len(
            bootcamp_funcs),
        "glue_jobs": len(bootcamp_jobs),
        "state_machines": len(bootcamp_sms)
    },
    "pipeline_stats": stats,
    "data_lake": {
        "total_objects": final_obj,
        "total_size_kb": round(
            final_size/1024, 1)
    },
    "aws_services_used": [
        "S3 — Data Lake Storage",
        "Lambda — Serverless Functions",
        "Glue — Managed ETL Jobs",
        "Athena — Serverless SQL",
        "Step Functions — Orchestration",
        "IAM — Security & Access"
    ]
}

report_key = (
    f"reports/capstone/"
    f"week7_report_{RUN_DATE}.json")
s3.put_object(
    Bucket=BUCKET,
    Key=report_key,
    Body=json.dumps(
        report, indent=4).encode('utf-8'),
    ContentType='application/json')

print(f"   ✅ Report saved: {report_key}")
logger.info(
    f"Capstone complete in {duration:.2f}s")

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   WEEK 7 AWS ADVANCED CAPSTONE")
print("=" * 55)
print(f"\n   Pipeline: {PIPELINE_NAME}")
print(f"   Status:   ✅ SUCCESS")
print(f"   Duration: {duration:.2f}s")

print(f"\n   🔧 AWS Services:")
for svc in report['aws_services_used']:
    print(f"   ✅ {svc}")

print(f"\n   📊 Stage Results:")
for stage, result in stats[
        "stages"].items():
    print(f"   {stage:<20} {result}")

print(f"\n   ☁️  Data Lake:")
print(f"   Objects: {final_obj}")
print(f"   Size:    {final_size/1024:.1f} KB")

print(f"\n✅ Week 7 AWS Advanced Complete!")
print(f"   Full Serverless DE Pipeline — "
      f"Production Ready!")
logger.info("Week 7 capstone complete!")