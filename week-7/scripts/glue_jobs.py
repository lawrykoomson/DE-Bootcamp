# ============================================
# WEEK 7 — MODULE 2: AWS Glue Jobs
# Managed ETL Service on AWS
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

glue = boto3.client(
    'glue', region_name=REGION)
s3 = boto3.client(
    's3', region_name=REGION)

print("=" * 55)
print("   MODULE 2 — AWS GLUE JOBS")
print("   MANAGED ETL SERVICE ON AWS")
print("=" * 55)
print(f"\n   Region: {REGION}")
print(f"   Bucket: {BUCKET}")

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN GLUE JOBS
# ─────────────────────────────────────────────
print("\n📌 1. WHAT ARE AWS GLUE JOBS?")
print("""
   AWS Glue Jobs:
   ┌─────────────────────────────────┐
   │  Managed ETL on AWS             │
   │  Write PySpark or Python code   │
   │  AWS handles the cluster        │
   │  Scales to petabytes            │
   │  Pay only when running          │
   └─────────────────────────────────┘

   Glue Job vs Lambda:
   Lambda  → Small fast functions (<15min)
   Glue    → Large ETL jobs (hours of data)

   Real use cases:
   ✅ Process 1TB of customer data daily
   ✅ Join multiple large datasets
   ✅ Convert CSV to Parquet format
   ✅ Run nightly data warehouse loads
""")

# ─────────────────────────────────────────────
# STEP 2: CREATE GLUE ETL SCRIPT
# ─────────────────────────────────────────────
print("\n📌 2. CREATING GLUE ETL SCRIPT")
logger.info("Creating Glue ETL script")

glue_script = f'''
# ============================================
# AWS Glue ETL Job — Telco Churn Pipeline
# Runs on AWS managed Spark cluster
# Author: Lawrence Koomson
# ============================================

import sys
import json
import boto3
from datetime import datetime

# In real Glue jobs you would use:
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext

# For this bootcamp we simulate the job
# using standard Python + boto3

print("=" * 50)
print("GLUE JOB: Telco Churn ETL Pipeline")
print("=" * 50)

BUCKET = "{BUCKET}"
RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")
RUN_TS = datetime.now().strftime(
    "%Y%m%d_%H%M%S")

s3 = boto3.client("s3", region_name="{REGION}")

# Step 1: List source files
print("\\nStep 1: Scanning source files...")
paginator = s3.get_paginator(
    "list_objects_v2")
pages = paginator.paginate(
    Bucket=BUCKET,
    Prefix="raw/customers/")

source_files = []
for page in pages:
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            source_files.append(obj["Key"])

print(f"Found {{len(source_files)}} source files")
for f in source_files:
    print(f"  - {{f}}")

# Step 2: Process each file
print("\\nStep 2: Processing files...")
all_records = []
for file_key in source_files:
    import io, csv
    response = s3.get_object(
        Bucket=BUCKET, Key=file_key)
    content = response["Body"].read(
        ).decode("utf-8")
    reader = csv.DictReader(
        io.StringIO(content))
    records = list(reader)
    all_records.extend(records)
    print(f"  Loaded {{len(records)}} "
          f"records from {{file_key}}")

print(f"Total records: {{len(all_records)}}")

# Step 3: Transform
print("\\nStep 3: Transforming data...")
transformed = []
for record in all_records:
    try:
        transformed.append({{
            "customerID": record.get(
                "customerID", "").strip(),
            "Contract": record.get(
                "Contract", "").strip(),
            "MonthlyCharges": float(
                record.get(
                    "MonthlyCharges", 0)
                or 0),
            "Churn": record.get(
                "Churn", "").strip(),
            "processed_by": "glue-job",
            "processed_at": datetime.now(
                ).strftime(
                "%Y-%m-%d %H:%M:%S"),
            "job_run_id": RUN_TS
        }})
    except Exception as e:
        print(f"  Skipped record: {{e}}")

print(f"Transformed: {{len(transformed)}} records")

# Step 4: Write output
print("\\nStep 4: Writing output to S3...")
import csv as csv_module
output = io.StringIO()
if transformed:
    writer = csv_module.DictWriter(
        output,
        fieldnames=transformed[0].keys())
    writer.writeheader()
    writer.writerows(transformed)

    output_key = (
        f"processed/glue-output/"
        f"year={{datetime.now().year}}/"
        f"glue_output_{{RUN_TS}}.csv")

    s3.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=output.getvalue().encode(
            "utf-8"),
        ContentType="text/csv",
        Metadata={{
            "job": "telco-churn-etl",
            "records": str(len(transformed)),
            "run_ts": RUN_TS
        }}
    )
    print(f"Output written to: {{output_key}}")
    print(f"Records written: {{len(transformed)}}")

# Step 5: Job report
report = {{
    "job_name": "telco-churn-etl",
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "source_files": len(source_files),
    "total_records": len(all_records),
    "transformed_records": len(transformed),
    "output_key": output_key
    if transformed else None,
    "status": "SUCCEEDED"
}}

s3.put_object(
    Bucket=BUCKET,
    Key=f"reports/glue/glue_report_{{RUN_TS}}.json",
    Body=json.dumps(
        report, indent=4).encode("utf-8"),
    ContentType="application/json"
)
print(f"\\nJob report saved to S3")
print(f"Status: {{report['status']}}")
print("=" * 50)
print("GLUE JOB COMPLETE!")
print("=" * 50)
'''

# Upload Glue script to S3
script_key = (
    f"glue-scripts/"
    f"telco_churn_etl.py")
s3.put_object(
    Bucket=BUCKET,
    Key=script_key,
    Body=glue_script.encode('utf-8'),
    ContentType='text/plain')
print(f"   ✅ Script uploaded to S3:")
print(f"   s3://{BUCKET}/{script_key}")
logger.info(
    f"Script uploaded: {script_key}")

# ─────────────────────────────────────────────
# STEP 3: CREATE GLUE JOB
# ─────────────────────────────────────────────
print("\n📌 3. CREATING GLUE JOB")
logger.info("Creating Glue job")

JOB_NAME = "de-bootcamp-telco-etl"

try:
    glue.delete_job(JobName=JOB_NAME)
    print(f"   🔄 Deleted existing job")
    time.sleep(2)
except ClientError:
    pass

try:
    response = glue.create_job(
        Name=JOB_NAME,
        Description=(
            "Telco Churn ETL Pipeline — "
            "DE Bootcamp Lawrence Koomson"),
        Role=ROLE_ARN,
        Command={
            'Name': 'pythonshell',
            'ScriptLocation': (
                f"s3://{BUCKET}/"
                f"{script_key}"),
            'PythonVersion': '3.9'
        },
        DefaultArguments={
            '--job-language': 'python',
            '--TempDir': (
                f"s3://{BUCKET}/glue-temp/"),
            '--enable-metrics': 'true',
        },
        MaxRetries=0,
        Timeout=10,
        MaxCapacity=0.0625,
        Tags={
            'Project': 'DE-Bootcamp',
            'Owner': 'Lawrence-Koomson',
            'Week': 'Week-7'
        }
    )
    print(f"   ✅ Glue job created: "
          f"{JOB_NAME}")
    logger.info(
        f"Glue job created: {JOB_NAME}")

except ClientError as e:
    print(f"   ❌ Error: {e}")
    logger.error(f"Glue job error: {e}")

# ─────────────────────────────────────────────
# STEP 4: RUN GLUE JOB
# ─────────────────────────────────────────────
print("\n📌 4. RUNNING GLUE JOB")
logger.info("Starting Glue job run")

try:
    run_response = glue.start_job_run(
        JobName=JOB_NAME,
        Arguments={
            '--bucket': BUCKET,
            '--region': REGION,
        }
    )
    run_id = run_response['JobRunId']
    print(f"   ✅ Job started!")
    print(f"   Run ID: {run_id}")
    logger.info(
        f"Job run started: {run_id}")

    # Monitor job status
    print(f"\n   ⏳ Monitoring job...")
    max_wait = 300
    waited = 0
    final_status = None

    while waited < max_wait:
        run_info = glue.get_job_run(
            JobName=JOB_NAME,
            RunId=run_id)
        state = run_info[
            'JobRun']['JobRunState']

        print(f"   Status: {state} "
              f"({waited}s elapsed)")

        if state in [
                'SUCCEEDED', 'FAILED',
                'STOPPED', 'ERROR',
                'TIMEOUT']:
            final_status = state
            break

        time.sleep(15)
        waited += 15

    if final_status == 'SUCCEEDED':
        print(f"\n   ✅ Job SUCCEEDED!")
        exec_time = run_info[
            'JobRun'].get(
            'ExecutionTime', 0)
        print(f"   Execution time: "
              f"{exec_time}s")
        logger.info(
            f"Job succeeded in "
            f"{exec_time}s")
    else:
        print(f"\n   ⚠️ Job status: "
              f"{final_status}")
        error = run_info[
            'JobRun'].get(
            'ErrorMessage', '')
        if error:
            print(f"   Error: {error}")
        logger.warning(
            f"Job ended: {final_status}")

except ClientError as e:
    print(f"   ❌ Job run failed: {e}")
    logger.error(
        f"Job run failed: {e}")

# ─────────────────────────────────────────────
# STEP 5: CHECK OUTPUTS
# ─────────────────────────────────────────────
print("\n📌 5. CHECKING GLUE OUTPUTS")
logger.info("Checking Glue outputs")

# Check for output files
paginator = s3.get_paginator(
    'list_objects_v2')
pages = paginator.paginate(
    Bucket=BUCKET,
    Prefix="processed/glue-output/")

glue_outputs = []
for page in pages:
    glue_outputs.extend(
        page.get('Contents', []))

if glue_outputs:
    print(f"   ✅ Glue output files:")
    for obj in glue_outputs:
        print(f"   📄 {obj['Key']}")
        print(f"       Size: "
              f"{obj['Size']:,} bytes")
else:
    print("   ⏳ Output files not yet "
          "available")

# Check for reports
pages = paginator.paginate(
    Bucket=BUCKET,
    Prefix="reports/glue/")
glue_reports = []
for page in pages:
    glue_reports.extend(
        page.get('Contents', []))

if glue_reports:
    print(f"\n   ✅ Glue report files:")
    for obj in glue_reports:
        print(f"   📄 {obj['Key']}")

# ─────────────────────────────────────────────
# STEP 6: LIST ALL GLUE JOBS
# ─────────────────────────────────────────────
print("\n📌 6. ALL GLUE JOBS")

response = glue.list_jobs()
all_jobs = response.get('JobNames', [])
bootcamp_jobs = [
    j for j in all_jobs
    if 'bootcamp' in j.lower()
    or 'telco' in j.lower()]

print(f"   DE Bootcamp Glue jobs:")
for job_name in bootcamp_jobs:
    job_detail = glue.get_job(
        JobName=job_name)['Job']
    print(f"   🔧 {job_name}")
    print(f"      Role: "
          f"{job_detail['Role'][-30:]}")
    print(f"      Created: "
          f"{job_detail['CreatedOn'].strftime('%Y-%m-%d')}")

logger.info(
    f"Found {len(bootcamp_jobs)} "
    f"bootcamp Glue jobs")

print(f"\n{'='*55}")
print("   MODULE 2 SUMMARY")
print(f"{'='*55}")
print("   Glue Script    → Python ETL code")
print("   Glue Job       → Managed runner")
print("   start_job_run()→ Execute the job")
print("   get_job_run()  → Monitor status")
print("   pythonshell    → Standard Python")
print("   glueContext    → Spark for big data")
print(f"\n✅ Module 2 Complete!")
logger.info("Module 2 Glue complete")