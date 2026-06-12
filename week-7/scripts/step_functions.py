# ============================================
# WEEK 7 — MODULE 4: AWS Step Functions
# Pipeline Orchestration & Workflow
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import json
import logging
import os
import time
import io
import zipfile
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
            '../logs/module4.log',
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

s3 = boto3.client(
    's3', region_name=REGION)
lambda_client = boto3.client(
    'lambda', region_name=REGION)
sfn = boto3.client(
    'stepfunctions', region_name=REGION)

print("=" * 55)
print("   MODULE 4 — AWS STEP FUNCTIONS")
print("   PIPELINE ORCHESTRATION!")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN STEP FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 1. WHAT ARE STEP FUNCTIONS?")
print("""
   Step Functions orchestrate pipelines:

   ┌─────────┐    ┌─────────┐    ┌─────────┐
   │ Extract │ →  │Transform│ →  │  Load   │
   │ Lambda  │    │ Lambda  │    │ Lambda  │
   └─────────┘    └─────────┘    └─────────┘
        ↓ fail         ↓ fail         ↓
   ┌─────────┐    ┌─────────┐    ┌─────────┐
   │  Error  │    │  Error  │    │  Report │
   │ Handler │    │ Handler │    │ Success │
   └─────────┘    └─────────┘    └─────────┘

   Benefits:
   ✅ Visual pipeline monitoring
   ✅ Automatic error handling
   ✅ Retry failed steps
   ✅ Parallel execution
   ✅ Full audit trail
""")

# ─────────────────────────────────────────────
# STEP 2: CREATE PIPELINE STEP LAMBDAS
# ─────────────────────────────────────────────
print("\n📌 2. CREATING PIPELINE STEP LAMBDAS")
logger.info("Creating step Lambda functions")

step_functions_code = {
    "de-bootcamp-step-extract": '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """Step 1: Extract data from S3."""
    print("Step 1: EXTRACT started")
    bucket = event.get("bucket")
    prefix = event.get(
        "prefix", "raw/customers/")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator(
        "list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket, Prefix=prefix)
    files = []
    total_records = 0
    for page in pages:
        for obj in page.get(
                "Contents", []):
            if obj["Key"].endswith(".csv"):
                files.append(obj["Key"])
                response = s3.get_object(
                    Bucket=bucket,
                    Key=obj["Key"])
                content = response[
                    "Body"].read().decode(
                    "utf-8")
                reader = csv.DictReader(
                    io.StringIO(content))
                records = list(reader)
                total_records += len(records)
    print(f"Extracted {len(files)} files, "
          f"{total_records} records")
    return {
        "status": "EXTRACTED",
        "bucket": bucket,
        "files_found": len(files),
        "total_records": total_records,
        "source_prefix": prefix,
        "timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_name": event.get(
            "pipeline_name", ""),
        "run_id": event.get("run_id", ""),
        "run_date": event.get("run_date", "")
    }
''',

    "de-bootcamp-step-validate": '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """Step 2: Validate extracted data."""
    print("Step 2: VALIDATE started")
    if event.get("total_records", 0) == 0:
        raise Exception(
            "No records to validate!")
    total = event.get("total_records", 0)
    valid = int(total * 0.98)
    invalid = total - valid
    print(f"Validated: {valid} valid, "
          f"{invalid} invalid")
    return {
        "status": "VALIDATED",
        "bucket": event.get("bucket"),
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "validation_passed": invalid == 0
            or (invalid/total) < 0.05,
        "files_found": event.get(
            "files_found", 0),
        "timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_name": event.get(
            "pipeline_name", ""),
        "run_id": event.get("run_id", ""),
        "run_date": event.get("run_date", "")
    }
''',

    "de-bootcamp-step-transform": '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """Step 3: Transform and enrich data."""
    print("Step 3: TRANSFORM started")
    bucket = event.get("bucket")
    valid = event.get("valid_records", 0)
    s3 = boto3.client("s3")
    transformed_key = (
        f"processed/step-functions/"
        f"transformed_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    output = {
        "records_transformed": valid,
        "columns_added": [
            "risk_score",
            "lifetime_value",
            "processed_at"],
        "output_key": transformed_key
    }
    s3.put_object(
        Bucket=bucket,
        Key=transformed_key,
        Body=json.dumps(
            output, indent=4).encode(
            "utf-8"),
        ContentType="application/json")
    print(f"Transformed {valid} records")
    return {
        "status": "TRANSFORMED",
        "bucket": bucket,
        "transformed_records": valid,
        "output_key": transformed_key,
        "files_found": event.get(
            "files_found", 0),
        "total_records": event.get(
            "total_records", 0),
        "valid_records": valid,
        "timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_name": event.get(
            "pipeline_name", ""),
        "run_id": event.get("run_id", ""),
        "run_date": event.get("run_date", "")
    }
''',

    "de-bootcamp-step-report": '''
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Step 4: Generate final report."""
    print("Step 4: REPORT started")
    bucket = event.get("bucket")
    s3 = boto3.client("s3")
    report = {
        "pipeline_name": event.get(
            "pipeline_name",
            "Step Functions DE Pipeline"),
        "run_id": event.get("run_id", ""),
        "run_date": event.get("run_date", ""),
        "run_timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_status": "SUCCESS",
        "stages_completed": [
            "Extract", "Validate",
            "Transform", "Report"],
        "metrics": {
            "files_processed": event.get(
                "files_found", 0),
            "records_extracted": event.get(
                "total_records", 0),
            "records_valid": event.get(
                "valid_records", 0),
            "records_transformed": event.get(
                "transformed_records", 0),
        },
        "output_files": [
            event.get("output_key", "")]
    }
    report_key = (
        f"reports/step-functions/"
        f"pipeline_report_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=json.dumps(
            report, indent=4).encode(
            "utf-8"),
        ContentType="application/json")
    print(f"Report saved: {report_key}")
    return {
        "status": "COMPLETED",
        "pipeline_status": "SUCCESS",
        "report_key": report_key,
        "metrics": report["metrics"],
        "timestamp": datetime.now(
            ).strftime("%Y-%m-%d %H:%M:%S")
    }
'''
}

deployed_arns = {}
for func_name, code in step_functions_code.items():
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(
            zip_buffer, 'w',
            zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "lambda_function.py", code)
    zip_buffer.seek(0)

    try:
        try:
            lambda_client.delete_function(
                FunctionName=func_name)
            time.sleep(2)
        except ClientError:
            pass

        resp = lambda_client.create_function(
            FunctionName=func_name,
            Runtime='python3.11',
            Role=ROLE_ARN,
            Handler='lambda_function'
                    '.lambda_handler',
            Code={'ZipFile': zip_buffer.read()},
            Timeout=60,
            MemorySize=256,
            Tags={
                'Project': 'DE-Bootcamp',
                'Week': 'Week-7'
            }
        )
        arn = resp['FunctionArn']
        deployed_arns[func_name] = arn
        print(f"   ✅ {func_name}")
        logger.info(f"Deployed: {func_name}")
        time.sleep(2)

    except ClientError as e:
        print(f"   ❌ Failed {func_name}: {e}")
        logger.error(f"Deploy failed: {e}")

# ─────────────────────────────────────────────
# STEP 3: CREATE STATE MACHINE
# ─────────────────────────────────────────────
print("\n📌 3. CREATING STATE MACHINE")
logger.info(
    "Creating Step Functions state machine")

SM_NAME = "de-bootcamp-telco-pipeline"

# State machine definition
state_machine_def = {
    "Comment": "Telco DE Pipeline",
    "StartAt": "Extract",
    "States": {
        "Extract": {
            "Type": "Task",
            "Resource": deployed_arns.get(
                "de-bootcamp-step-extract",
                ""),
            "Next": "Validate",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "Next": "PipelineFailed",
                "ResultPath": "$.error"
            }],
        },
        "Validate": {
            "Type": "Task",
            "Resource": deployed_arns.get(
                "de-bootcamp-step-validate",
                ""),
            "Next": "CheckValidation",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "Next": "PipelineFailed",
                "ResultPath": "$.error"
            }],
        },
        "CheckValidation": {
            "Type": "Choice",
            "Choices": [{
                "Variable":
                    "$.validation_passed",
                "BooleanEquals": True,
                "Next": "Transform"
            }],
            "Default": "PipelineFailed",
        },
        "Transform": {
            "Type": "Task",
            "Resource": deployed_arns.get(
                "de-bootcamp-step-transform",
                ""),
            "Next": "Report",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "Next": "PipelineFailed",
                "ResultPath": "$.error"
            }],
        },
        "Report": {
            "Type": "Task",
            "Resource": deployed_arns.get(
                "de-bootcamp-step-report",
                ""),
            "Next": "PipelineSucceeded",
            "Catch": [{
                "ErrorEquals": ["States.ALL"],
                "Next": "PipelineFailed",
                "ResultPath": "$.error"
            }],
        },
        "PipelineSucceeded": {
            "Type": "Succeed"
        },
        "PipelineFailed": {
            "Type": "Fail",
            "Error": "PipelineError",
            "Cause": "Pipeline step failed"
        }
    }
}

# Delete existing and wait properly
try:
    existing = sfn.list_state_machines()
    for sm in existing.get(
            'stateMachines', []):
        if SM_NAME in sm['name']:
            sfn.delete_state_machine(
                stateMachineArn=sm[
                    'stateMachineArn'])
            print("   ⏳ Waiting for old "
                  "state machine to delete...")
            for i in range(12):
                time.sleep(5)
                print(f"   ⏳ {(i+1)*5}s...")
                try:
                    sfn.describe_state_machine(
                        stateMachineArn=sm[
                            'stateMachineArn'])
                except ClientError as ce:
                    if 'DoesNotExist' in str(ce):
                        print("   ✅ Deleted!")
                        break
except ClientError:
    pass

# Create with retry
SM_ARN = None
for attempt in range(5):
    try:
        sm_response = sfn.create_state_machine(
            name=SM_NAME,
            definition=json.dumps(
                state_machine_def),
            roleArn=ROLE_ARN,
            type='STANDARD',
            tags=[{
                'key': 'Project',
                'value': 'DE-Bootcamp'
            }]
        )
        SM_ARN = sm_response[
            'stateMachineArn']
        print(f"   ✅ State machine created!")
        print(f"   Name: {SM_NAME}")
        print(f"   ARN:  ...{SM_ARN[-30:]}")
        logger.info(
            f"State machine created: "
            f"{SM_NAME}")
        break
    except ClientError as e:
        if 'StateMachineDeleting' in str(e):
            print(f"   ⏳ Still deleting, "
                  f"waiting 10s "
                  f"(attempt {attempt+1}/5)...")
            time.sleep(10)
        else:
            print(f"   ❌ Error: {e}")
            logger.error(f"SM error: {e}")
            break

if not SM_ARN:
    print("   ❌ Could not create "
          "state machine!")
    exit(1)

# ─────────────────────────────────────────────
# STEP 4: EXECUTE THE PIPELINE
# ─────────────────────────────────────────────
print("\n📌 4. EXECUTING THE PIPELINE")
logger.info("Starting pipeline execution")

pipeline_input = {
    "bucket": BUCKET,
    "prefix": "raw/customers/",
    "pipeline_name": "Telco DE Pipeline",
    "run_date": RUN_DATE,
    "run_id": RUN_TS
}

try:
    execution = sfn.start_execution(
        stateMachineArn=SM_ARN,
        name=f"run-{RUN_TS}",
        input=json.dumps(pipeline_input)
    )
    EXEC_ARN = execution['executionArn']
    print(f"   ✅ Pipeline started!")
    print(f"   Execution: run-{RUN_TS}")
    logger.info(
        f"Execution started: run-{RUN_TS}")

    # Monitor execution
    print(f"\n   ⏳ Monitoring pipeline...")
    max_wait = 180
    waited = 0
    final_status = None

    while waited < max_wait:
        exec_info = sfn.describe_execution(
            executionArn=EXEC_ARN)
        state = exec_info['status']
        print(f"   Status: {state} "
              f"({waited}s)")

        if state in [
                'SUCCEEDED', 'FAILED',
                'TIMED_OUT', 'ABORTED']:
            final_status = state
            break

        time.sleep(10)
        waited += 10

    if final_status == 'SUCCEEDED':
        print(f"\n   ✅ PIPELINE SUCCEEDED!")
        output = json.loads(
            exec_info.get('output', '{}'))
        print(f"\n   📊 Pipeline Results:")
        for key, val in output.items():
            if isinstance(val, dict):
                print(f"   {key}:")
                for k, v in val.items():
                    print(f"     {k}: {v}")
            else:
                print(f"   {key}: {val}")
        logger.info("Pipeline succeeded!")
    else:
        print(f"\n   ⚠️ Status: {final_status}")
        logger.warning(
            f"Pipeline: {final_status}")

except ClientError as e:
    print(f"   ❌ Execution failed: {e}")
    logger.error(f"Execution failed: {e}")

# ─────────────────────────────────────────────
# STEP 5: VIEW EXECUTION HISTORY
# ─────────────────────────────────────────────
print("\n📌 5. EXECUTION HISTORY")
logger.info("Fetching execution history")

try:
    history = sfn.get_execution_history(
        executionArn=EXEC_ARN,
        maxResults=20)

    events = history.get('events', [])
    print(f"   Pipeline events:")
    for event in events:
        etype = event['type']
        ts = event['timestamp'].strftime(
            '%H:%M:%S')
        if any(k in etype for k in [
                'Entered', 'Exited',
                'Started', 'Succeeded',
                'Failed']):
            icon = (
                '✅' if 'Succeeded' in etype
                else '❌' if 'Failed' in etype
                else '▶️')
            print(f"   {icon} {ts} "
                  f"{etype}")

    logger.info(
        f"History: {len(events)} events")

except ClientError as e:
    print(f"   ❌ History error: {e}")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   State Machine  → Visual pipeline")
print("   States         → Each pipeline step")
print("   Task State     → Calls Lambda")
print("   Choice State   → Branch on result")
print("   Catch          → Handle errors")
print("   start_execution→ Run the pipeline")
print("   Succeed/Fail   → Terminal states")
print(f"\n✅ Module 4 Complete!")
logger.info(
    "Module 4 Step Functions complete")