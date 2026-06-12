# ============================================
# WEEK 7 — MODULE 3: Event-Driven Pipelines
# S3 Events + Lambda Triggers
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
            '../logs/module3.log',
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

print("=" * 55)
print("   MODULE 3 — EVENT-DRIVEN PIPELINES")
print("   S3 EVENTS + LAMBDA TRIGGERS")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN EVENT-DRIVEN PIPELINES
# ─────────────────────────────────────────────
print("\n📌 1. WHAT IS EVENT-DRIVEN?")
print("""
   Traditional scheduled pipeline:
   ┌──────────────────────────────┐
   │  Run every night at 2am      │
   │  Process all files           │
   │  Even if no new files exist  │
   └──────────────────────────────┘

   Event-driven pipeline:
   ┌──────────────────────────────┐
   │  New file arrives in S3      │
   │       ↓                      │
   │  S3 fires an event           │
   │       ↓                      │
   │  Lambda triggered instantly  │
   │       ↓                      │
   │  Data processed immediately  │
   └──────────────────────────────┘

   Benefits:
   ✅ Real-time processing
   ✅ No wasted compute
   ✅ Scales automatically
   ✅ Zero maintenance
""")

# ─────────────────────────────────────────────
# STEP 2: CREATE TRIGGER LAMBDA FUNCTION
# ─────────────────────────────────────────────
print("\n📌 2. CREATING TRIGGER LAMBDA")
logger.info("Creating trigger Lambda")

trigger_code = '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """
    Auto-triggered when new CSV file
    lands in S3 raw/incoming/ folder.
    Validates and processes the file.
    """
    print("S3 Trigger Lambda fired!")
    print(f"Event: {json.dumps(event)}")

    s3 = boto3.client("s3")
    results = []

    # Process each S3 event record
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"]["size"]

        print(f"New file detected!")
        print(f"  Bucket: {bucket}")
        print(f"  Key:    {key}")
        print(f"  Size:   {size} bytes")

        try:
            # Read the new file
            response = s3.get_object(
                Bucket=bucket, Key=key)
            content = response[
                "Body"].read().decode("utf-8")
            reader = csv.DictReader(
                io.StringIO(content))
            records = list(reader)

            # Quick validation
            total = len(records)
            valid = 0
            invalid = 0
            issues = []

            for i, rec in enumerate(
                    records, 1):
                if (rec.get("customerID")
                        and rec.get(
                            "MonthlyCharges")):
                    valid += 1
                else:
                    invalid += 1
                    issues.append(
                        f"Row {i}: missing fields")

            # Churn analysis
            churned = sum(
                1 for r in records
                if r.get("Churn", "")
                in ["1", "True", "Yes"])

            churn_rate = round(
                churned / total * 100, 1
            ) if total > 0 else 0

            # Write processed output
            output_key = key.replace(
                "raw/incoming/",
                "processed/auto-processed/")

            output = io.StringIO()
            if records:
                fieldnames = list(
                    records[0].keys())
                fieldnames.append(
                    "auto_processed_at")

                writer = csv.DictWriter(
                    output,
                    fieldnames=fieldnames)
                writer.writeheader()

                for rec in records:
                    rec["auto_processed_at"] = \
                        datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S")
                    writer.writerow(rec)

                s3.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body=output.getvalue(
                        ).encode("utf-8"),
                    ContentType="text/csv",
                    Metadata={
                        "source": key,
                        "records": str(total),
                        "trigger": "s3-event"
                    }
                )

            result = {
                "source_file": key,
                "output_file": output_key,
                "total_records": total,
                "valid": valid,
                "invalid": invalid,
                "churned": churned,
                "churn_rate_pct": churn_rate,
                "processed_at": datetime.now(
                    ).strftime(
                    "%Y-%m-%d %H:%M:%S"),
                "status": "SUCCESS"
            }
            results.append(result)

            print(f"Processing complete:")
            print(f"  Records: {total}")
            print(f"  Valid:   {valid}")
            print(f"  Churn:   {churn_rate}%")
            print(f"  Output:  {output_key}")

        except Exception as e:
            print(f"Error processing {key}: {e}")
            results.append({
                "source_file": key,
                "error": str(e),
                "status": "FAILED"
            })

    return {
        "statusCode": 200,
        "processed_files": len(results),
        "results": results
    }
'''

# Deploy trigger Lambda
TRIGGER_FUNC = "de-bootcamp-s3-trigger"

zip_buffer = io.BytesIO()
with zipfile.ZipFile(
        zip_buffer, 'w',
        zipfile.ZIP_DEFLATED) as zf:
    zf.writestr(
        "lambda_function.py",
        trigger_code)
zip_buffer.seek(0)

try:
    try:
        lambda_client.delete_function(
            FunctionName=TRIGGER_FUNC)
        time.sleep(2)
    except ClientError:
        pass

    response = lambda_client.create_function(
        FunctionName=TRIGGER_FUNC,
        Runtime='python3.11',
        Role=ROLE_ARN,
        Handler='lambda_function.lambda_handler',
        Code={'ZipFile': zip_buffer.read()},
        Description=(
            'Auto-triggered by S3 file uploads'),
        Timeout=60,
        MemorySize=256,
        Tags={
            'Project': 'DE-Bootcamp',
            'Week': 'Week-7'
        }
    )
    print(f"   ✅ Trigger Lambda deployed: "
          f"{TRIGGER_FUNC}")
    logger.info(
        f"Deployed: {TRIGGER_FUNC}")

    # Wait for active
    waiter = lambda_client.get_waiter(
        'function_active')
    waiter.wait(FunctionName=TRIGGER_FUNC)
    print(f"   ✅ Lambda is active!")

except ClientError as e:
    print(f"   ❌ Deploy failed: {e}")
    logger.error(f"Deploy failed: {e}")

# ─────────────────────────────────────────────
# STEP 3: ADD S3 TRIGGER PERMISSION
# ─────────────────────────────────────────────
print("\n📌 3. ADDING S3 TRIGGER PERMISSION")
logger.info("Adding S3 trigger permission")

try:
    lambda_client.add_permission(
        FunctionName=TRIGGER_FUNC,
        StatementId='s3-trigger-permission',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=(
            f"arn:aws:s3:::{BUCKET}"),
    )
    print(f"   ✅ S3 trigger permission added")
    logger.info("S3 permission added")

except ClientError as e:
    if 'ResourceConflictException' in str(e):
        print(f"   ✅ Permission already exists")
    else:
        print(f"   ❌ Permission error: {e}")

# ─────────────────────────────────────────────
# STEP 4: CONFIGURE S3 EVENT NOTIFICATION
# ─────────────────────────────────────────────
print("\n📌 4. CONFIGURING S3 EVENT TRIGGER")
logger.info("Configuring S3 notification")

func_arn = lambda_client.get_function(
    FunctionName=TRIGGER_FUNC
)['Configuration']['FunctionArn']

try:
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [{
                'LambdaFunctionArn': func_arn,
                'Events': [
                    's3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [{
                            'Name': 'prefix',
                            'Value':
                                'raw/incoming/'
                        }, {
                            'Name': 'suffix',
                            'Value': '.csv'
                        }]
                    }
                }
            }]
        }
    )
    print(f"   ✅ S3 trigger configured!")
    print(f"   Watches: "
          f"s3://{BUCKET}/raw/incoming/*.csv")
    print(f"   Triggers: {TRIGGER_FUNC}")
    logger.info("S3 notification configured")

except ClientError as e:
    print(f"   ❌ Notification error: {e}")
    logger.error(
        f"Notification error: {e}")

# ─────────────────────────────────────────────
# STEP 5: TEST THE TRIGGER
# ─────────────────────────────────────────────
print("\n📌 5. TESTING THE EVENT TRIGGER")
logger.info("Testing S3 event trigger")

print("   Uploading test file to trigger "
      "the Lambda...")

test_csv = """customerID,Contract,MonthlyCharges,Churn
AUTO001,Month-to-month,89.50,1
AUTO002,One year,65.00,0
AUTO003,Month-to-month,99.00,1
AUTO004,Two year,45.00,0
AUTO005,Month-to-month,75.00,0
"""

test_key = (
    f"raw/incoming/"
    f"auto_test_{RUN_TS}.csv")

s3.put_object(
    Bucket=BUCKET,
    Key=test_key,
    Body=test_csv.encode('utf-8'),
    ContentType='text/csv')

print(f"   ✅ Test file uploaded:")
print(f"   {test_key}")
print(f"   ⏳ Waiting for Lambda to "
      f"auto-trigger (15s)...")

time.sleep(15)

# Check if processed file exists
processed_key = test_key.replace(
    "raw/incoming/",
    "processed/auto-processed/")

try:
    s3.head_object(
        Bucket=BUCKET,
        Key=processed_key)
    print(f"\n   ✅ AUTO-TRIGGERED! "
          f"File was processed!")
    print(f"   Output: {processed_key}")
    logger.info(
        "Event trigger working!")

    # Read processed file
    response = s3.get_object(
        Bucket=BUCKET,
        Key=processed_key)
    content = response[
        'Body'].read().decode('utf-8')
    lines = content.strip().split('\n')
    print(f"   Records in output: "
          f"{len(lines)-1}")
    print(f"   Header: {lines[0]}")

except ClientError:
    print(f"\n   ⏳ Trigger still processing"
          f" — check S3 console!")
    print(f"   Expected output key:")
    print(f"   {processed_key}")

# ─────────────────────────────────────────────
# STEP 6: MANUALLY INVOKE WITH S3 EVENT
# ─────────────────────────────────────────────
print("\n📌 6. MANUALLY INVOKE WITH S3 EVENT")
logger.info("Manual invocation with S3 event")

# Simulate what S3 sends to Lambda
s3_event = {
    "Records": [{
        "s3": {
            "bucket": {"name": BUCKET},
            "object": {
                "key": test_key,
                "size": len(test_csv)
            }
        }
    }]
}

print(f"   Invoking with simulated "
      f"S3 event...")

try:
    response = lambda_client.invoke(
        FunctionName=TRIGGER_FUNC,
        InvocationType='RequestResponse',
        Payload=json.dumps(s3_event)
    )

    result = json.loads(
        response['Payload'].read())
    print(f"\n   ✅ Lambda Result:")
    print(f"   Status:  "
          f"{response['StatusCode']}")
    print(f"   Files processed: "
          f"{result.get('processed_files')}")

    for r in result.get('results', []):
        print(f"\n   File: "
              f"{r.get('source_file')}")
        print(f"   Records: "
              f"{r.get('total_records')}")
        print(f"   Churn rate: "
              f"{r.get('churn_rate_pct')}%")
        print(f"   Status: "
              f"{r.get('status')}")

    logger.info(
        "Manual invocation successful")

except ClientError as e:
    print(f"   ❌ Invocation failed: {e}")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   S3 Events      → Detect new files")
print("   Lambda trigger → Auto-process files")
print("   add_permission → Allow S3 to invoke")
print("   put_bucket_notification → Wire up")
print("   Event-driven   → Real-time pipelines")
print("   Zero polling   → No wasted compute")
print(f"\n✅ Module 3 Complete!")
logger.info("Module 3 complete")