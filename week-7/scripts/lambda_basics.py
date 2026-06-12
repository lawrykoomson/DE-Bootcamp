# ============================================
# WEEK 7 — MODULE 1: AWS Lambda
# Serverless Python Functions on AWS
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import json
import logging
import os
import time
import zipfile
import io
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs(
    "../lambda_functions", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/module1.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

with open(
        "../../week-6/data/aws_config.json"
) as f:
    config = json.load(f)

BUCKET = config['bucket_name']
REGION = config['region']
RUN_DATE = datetime.now().strftime(
    "%Y-%m-%d")

lambda_client = boto3.client(
    'lambda', region_name=REGION)
iam = boto3.client(
    'iam', region_name=REGION)
s3 = boto3.client(
    's3', region_name=REGION)

print("=" * 55)
print("   MODULE 1 — AWS LAMBDA")
print("   SERVERLESS PYTHON FUNCTIONS!")
print("=" * 55)
print(f"\n   Region: {REGION}")
print(f"   Bucket: {BUCKET}")

# ─────────────────────────────────────────────
# STEP 1: EXPLAIN LAMBDA
# ─────────────────────────────────────────────
print("\n📌 1. WHAT IS AWS LAMBDA?")
print("""
   Traditional server:
   ┌─────────────────────────────┐
   │  Server running 24/7        │
   │  You pay even when idle     │
   │  You manage the server      │
   └─────────────────────────────┘

   AWS Lambda:
   ┌─────────────────────────────┐
   │  Code runs only when called │
   │  You pay per execution only │
   │  AWS manages everything     │
   │  Scales automatically       │
   └─────────────────────────────┘

   Perfect for:
   ✅ ETL triggers when new file arrives
   ✅ Data validation on upload
   ✅ Sending alerts when churn detected
   ✅ Scheduled data processing
""")

# ─────────────────────────────────────────────
# STEP 2: CREATE IAM ROLE FOR LAMBDA
# ─────────────────────────────────────────────
print("\n📌 2. CREATING IAM ROLE FOR LAMBDA")
logger.info("Creating Lambda IAM role")

ROLE_NAME = "lambda-de-bootcamp-role"

# Trust policy — allows Lambda to use role
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {
            "Service": "lambda.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }]
}

try:
    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(
            trust_policy),
        Description=(
            "Lambda role for DE Bootcamp")
    )
    role_arn = role['Role']['Arn']
    print(f"   ✅ Role created: {ROLE_NAME}")
    logger.info(
        f"Role created: {role_arn}")

    # Attach S3 and CloudWatch policies
    policies = [
        "arn:aws:iam::aws:policy/"
        "AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/"
        "CloudWatchLogsFullAccess",
        "arn:aws:iam::aws:policy/"
        "AmazonAthenaFullAccess",
    ]
    for policy in policies:
        iam.attach_role_policy(
            RoleName=ROLE_NAME,
            PolicyArn=policy)
    print(f"   ✅ Policies attached")

    # Wait for role to propagate
    print(f"   ⏳ Waiting for role "
          f"to propagate...")
    time.sleep(10)

except ClientError as e:
    if 'EntityAlreadyExists' in str(e):
        role = iam.get_role(
            RoleName=ROLE_NAME)
        role_arn = role['Role']['Arn']
        print(f"   ✅ Role exists: "
              f"{ROLE_NAME}")
        logger.info(
            f"Role exists: {role_arn}")
    else:
        print(f"   ❌ Error: {e}")
        logger.error(f"Role error: {e}")
        exit(1)

print(f"   ARN: {role_arn}")

# ─────────────────────────────────────────────
# STEP 3: CREATE LAMBDA FUNCTION CODE
# ─────────────────────────────────────────────
print("\n📌 3. WRITING LAMBDA FUNCTION CODE")
logger.info("Creating Lambda function code")

# Lambda function 1 — Churn detector
churn_detector_code = '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that analyses
    customer churn risk from S3 file.
    Triggered manually or by S3 event.
    """
    print("Churn Detector Lambda started!")

    # Get S3 details from event
    bucket = event.get(
        'bucket',
        'lawrence-de-bootcamp-20260611')
    key = event.get(
        'key',
        'raw/customers/sample_customers.csv')

    s3 = boto3.client('s3')

    try:
        # Read CSV from S3
        response = s3.get_object(
            Bucket=bucket, Key=key)
        content = response[
            'Body'].read().decode('utf-8')
        reader = csv.DictReader(
            io.StringIO(content))
        customers = list(reader)

        # Analyse churn risk
        total = len(customers)
        churned = sum(
            1 for c in customers
            if c.get('Churn', '').strip()
            in ['1', 'True', 'Yes'])
        churn_rate = round(
            churned / total * 100, 2
        ) if total > 0 else 0

        # High risk customers
        high_risk = [
            c for c in customers
            if c.get('Contract', '')
            == 'Month-to-month'
            and c.get('Churn', '') != '1'
        ]

        result = {
            'statusCode': 200,
            'pipeline': 'ChurnDetector',
            'timestamp': datetime.now(
                ).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'source': {
                'bucket': bucket,
                'key': key
            },
            'analysis': {
                'total_customers': total,
                'churned': churned,
                'churn_rate_pct': churn_rate,
                'high_risk_retained':
                    len(high_risk)
            },
            'status': 'SUCCESS'
        }

        print(f"Analysis complete: "
              f"{total} customers, "
              f"{churn_rate}% churn rate")
        return result

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'status': 'FAILED'
        }
'''

# Lambda function 2 — Data validator
validator_code = '''
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that validates
    data quality of uploaded CSV files.
    """
    print("Data Validator Lambda started!")

    bucket = event.get('bucket', '')
    key = event.get('key', '')

    if not bucket or not key:
        return {
            'statusCode': 400,
            'error': 'Missing bucket or key',
            'status': 'FAILED'
        }

    s3 = boto3.client('s3')

    try:
        response = s3.get_object(
            Bucket=bucket, Key=key)
        content = response[
            'Body'].read().decode('utf-8')
        reader = csv.DictReader(
            io.StringIO(content))
        records = list(reader)

        issues = []
        valid = 0
        invalid = 0

        required_fields = [
            'customerID',
            'MonthlyCharges',
            'Contract']

        for i, record in enumerate(
                records, 1):
            record_issues = []

            for field in required_fields:
                if not record.get(field, ''):
                    record_issues.append(
                        f"Missing {field}")

            try:
                charge = float(
                    record.get(
                        'MonthlyCharges', 0))
                if charge <= 0:
                    record_issues.append(
                        "Invalid charge")
            except ValueError:
                record_issues.append(
                    "Non-numeric charge")

            if record_issues:
                invalid += 1
                issues.append({
                    'row': i,
                    'issues': record_issues
                })
            else:
                valid += 1

        return {
            'statusCode': 200,
            'pipeline': 'DataValidator',
            'timestamp': datetime.now(
                ).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'validation': {
                'total_records': len(records),
                'valid': valid,
                'invalid': invalid,
                'issues_found': len(issues),
                'sample_issues': issues[:3]
            },
            'status': 'PASSED'
            if invalid == 0 else 'WARNINGS'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'error': str(e),
            'status': 'FAILED'
        }
'''

# Save Lambda code locally
functions = {
    "churn_detector": churn_detector_code,
    "data_validator": validator_code,
}

for name, code in functions.items():
    path = f"../lambda_functions/{name}.py"
    with open(path, 'w') as f:
        f.write(code)
    print(f"   ✅ Written: {name}.py")

logger.info("Lambda functions written")

# ─────────────────────────────────────────────
# STEP 4: PACKAGE AND DEPLOY LAMBDA
# ─────────────────────────────────────────────
print("\n📌 4. DEPLOYING LAMBDA FUNCTIONS")
logger.info("Deploying Lambda functions")

deployed = []

for func_name, code in functions.items():
    full_name = f"de-bootcamp-{func_name}"

    # Create ZIP package in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(
            zip_buffer, 'w',
            zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "lambda_function.py", code)
    zip_buffer.seek(0)
    zip_bytes = zip_buffer.read()

    try:
        # Delete if exists
        try:
            lambda_client.delete_function(
                FunctionName=full_name)
            print(f"   🔄 Updating: "
                  f"{full_name}")
            time.sleep(2)
        except ClientError:
            print(f"   ➕ Creating: "
                  f"{full_name}")

        # Create Lambda function
        response = lambda_client.create_function(
            FunctionName=full_name,
            Runtime='python3.11',
            Role=role_arn,
            Handler='lambda_function'
                    '.lambda_handler',
            Code={'ZipFile': zip_bytes},
            Description=(
                f"DE Bootcamp — "
                f"{func_name}"),
            Timeout=30,
            MemorySize=128,
            Environment={
                'Variables': {
                    'BUCKET': BUCKET,
                    'ENV': 'learning'
                }
            },
            Tags={
                'Project': 'DE-Bootcamp',
                'Owner': 'Lawrence-Koomson',
                'Week': 'Week-7'
            }
        )

        func_arn = response[
            'FunctionArn']
        print(f"   ✅ Deployed: "
              f"{full_name}")
        print(f"      ARN: "
              f"{func_arn[-40:]}")
        deployed.append(full_name)
        logger.info(
            f"Deployed: {full_name}")

        # Wait for active state
        time.sleep(3)

    except ClientError as e:
        print(f"   ❌ Deploy failed "
              f"{full_name}: {e}")
        logger.error(
            f"Deploy failed: {e}")

# ─────────────────────────────────────────────
# STEP 5: INVOKE LAMBDA FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 5. INVOKING LAMBDA FUNCTIONS")
logger.info("Invoking Lambda functions")

# Test payload
test_event = {
    "bucket": BUCKET,
    "key": "raw/customers/sample_customers.csv"
}

for func_name in deployed:
    print(f"\n   🚀 Invoking: {func_name}")

    try:
        # Wait for function to be ready
        waiter = lambda_client.get_waiter(
            'function_active')
        waiter.wait(
            FunctionName=func_name)

        response = lambda_client.invoke(
            FunctionName=func_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )

        status = response['StatusCode']
        payload = json.loads(
            response['Payload'].read())

        print(f"   HTTP Status: {status}")
        print(f"   Result:")

        if isinstance(payload, dict):
            for key, val in payload.items():
                if isinstance(val, dict):
                    print(f"   {key}:")
                    for k, v in val.items():
                        print(f"     {k}: {v}")
                else:
                    print(
                        f"   {key}: {val}")

        logger.info(
            f"Invoked {func_name}: "
            f"status={status}")

    except ClientError as e:
        print(f"   ❌ Invoke failed: {e}")
        logger.error(
            f"Invoke failed: {e}")

# ─────────────────────────────────────────────
# STEP 6: LIST LAMBDA FUNCTIONS
# ─────────────────────────────────────────────
print("\n📌 6. LISTING LAMBDA FUNCTIONS")

response = lambda_client.list_functions()
functions_list = response.get(
    'Functions', [])

bootcamp_funcs = [
    f for f in functions_list
    if 'de-bootcamp' in
    f['FunctionName']]

print(f"\n   DE Bootcamp functions:")
for func in bootcamp_funcs:
    print(f"   ⚡ {func['FunctionName']}")
    print(f"      Runtime: "
          f"{func['Runtime']}")
    print(f"      Memory:  "
          f"{func['MemorySize']} MB")
    print(f"      Timeout: "
          f"{func['Timeout']}s")
    print(f"      Modified: "
          f"{func['LastModified'][:10]}")

logger.info(
    f"Found {len(bootcamp_funcs)} "
    f"bootcamp functions")

# Save config
lambda_config = {
    "role_arn": role_arn,
    "functions": [
        f['FunctionArn']
        for f in bootcamp_funcs],
    "bucket": BUCKET,
    "region": REGION,
    "created_at": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S")
}
with open(
        "../data/lambda_config.json",
        'w') as f:
    json.dump(lambda_config, f, indent=4)
print(f"\n   ✅ Config saved: "
      f"lambda_config.json")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   IAM Role     → Permissions for Lambda")
print("   ZIP package  → Bundle Python code")
print("   create_function() → Deploy to AWS")
print("   invoke()     → Run Lambda function")
print("   RequestResponse → Synchronous call")
print("   Serverless   → No server to manage!")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 Lambda complete")