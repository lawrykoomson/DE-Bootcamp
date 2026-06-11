# ============================================
# WEEK 6 — MODULE 1: S3 Cloud Storage
# Buckets, Objects, Upload, Download
# GetSkills Network DE Bootcamp
# ============================================

import boto3
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from botocore.exceptions import (
    ClientError, NoCredentialsError)

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data", exist_ok=True)

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

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
REGION = "eu-north-1"
BUCKET_NAME = (
    f"lawrence-de-bootcamp-"
    f"{datetime.now().strftime('%Y%m%d')}"
)

print("=" * 55)
print("   MODULE 1 — S3 CLOUD STORAGE")
print("   BUCKETS | OBJECTS | UPLOAD | DOWNLOAD")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: CONNECT TO AWS
# ─────────────────────────────────────────────
print("\n📌 1. CONNECTING TO AWS")

try:
    session = boto3.Session(
        region_name=REGION)
    s3 = session.client('s3')
    sts = session.client('sts')

    identity = sts.get_caller_identity()
    print(f"   ✅ Connected to AWS!")
    print(f"   Account:  "
          f"{identity['Account']}")
    print(f"   Region:   {REGION}")
    logger.info(
        f"Connected to AWS account "
        f"{identity['Account']}")

except NoCredentialsError:
    print("   ❌ No AWS credentials found!")
    print("   Run: aws configure")
    exit(1)
except Exception as e:
    print(f"   ❌ Connection failed: {e}")
    exit(1)

# ─────────────────────────────────────────────
# STEP 2: CREATE S3 BUCKET
# ─────────────────────────────────────────────
print(f"\n📌 2. CREATING S3 BUCKET")
print(f"   Bucket: {BUCKET_NAME}")
logger.info(
    f"Creating bucket: {BUCKET_NAME}")

try:
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION
        }
    )
    print(f"   ✅ Bucket created: "
          f"{BUCKET_NAME}")
    logger.info(
        f"Bucket created: {BUCKET_NAME}")

except ClientError as e:
    error_code = e.response[
        'Error']['Code']
    if error_code in [
            'BucketAlreadyOwnedByYou',
            'BucketAlreadyExists']:
        print(f"   ✅ Bucket already exists "
              f"— continuing!")
        logger.info(
            "Bucket already exists")
    else:
        print(f"   ❌ Error: {e}")
        logger.error(
            f"Bucket creation failed: {e}")
        exit(1)

# ─────────────────────────────────────────────
# STEP 3: LIST ALL BUCKETS
# ─────────────────────────────────────────────
print("\n📌 3. LISTING ALL S3 BUCKETS")

response = s3.list_buckets()
buckets = response.get('Buckets', [])

print(f"   Total buckets: {len(buckets)}")
for bucket in buckets:
    print(f"   🪣 {bucket['Name']:<45} "
          f"Created: "
          f"{bucket['CreationDate'].strftime('%Y-%m-%d')}")
logger.info(
    f"Found {len(buckets)} buckets")

# ─────────────────────────────────────────────
# STEP 4: CREATE SAMPLE FILES TO UPLOAD
# ─────────────────────────────────────────────
print("\n📌 4. CREATING SAMPLE DATA FILES")

# Create a sample CSV
csv_content = """customerID,Contract,MonthlyCharges,Churn
7590-VHVEG,Month-to-month,29.85,0
5575-GNVDE,One year,56.95,0
3668-QPYBK,Month-to-month,53.85,1
7795-CFOCW,One year,42.30,0
9237-HQITU,Month-to-month,70.70,1
9305-CDSKC,Month-to-month,99.65,1
1452-KIOVK,Month-to-month,89.10,0
6713-OKOMC,Month-to-month,29.75,0
7892-POOKP,Month-to-month,104.80,1
0280-XJGEX,Month-to-month,103.70,1
"""

# Create a sample JSON report
json_content = json.dumps({
    "pipeline": "Telco DE Pipeline",
    "run_date": datetime.now().strftime(
        "%Y-%m-%d"),
    "total_customers": 7043,
    "churn_rate": 26.54,
    "revenue_at_risk": 139130.85,
    "status": "SUCCESS"
}, indent=4)

# Create a sample README
readme_content = """# DE Bootcamp — S3 Data Lake
## Lawrence Koomson

This S3 bucket contains data engineering
pipeline outputs for the Telco Churn project.

### Folder Structure
- raw/       → Raw ingested data
- processed/ → Cleaned and transformed data
- reports/   → JSON and CSV reports
- logs/      → Pipeline execution logs
"""

# Save files locally
files = {
    "../data/sample_customers.csv": csv_content,
    "../data/pipeline_report.json": json_content,
    "../data/README.md": readme_content,
}

for path, content in files.items():
    with open(path, 'w',
              encoding='utf-8') as f:
        f.write(content)
    size = os.path.getsize(path)
    print(f"   ✅ Created: "
          f"{Path(path).name:<35} "
          f"{size} bytes")

# ─────────────────────────────────────────────
# STEP 5: UPLOAD FILES TO S3
# ─────────────────────────────────────────────
print("\n📌 5. UPLOADING FILES TO S3")
logger.info("Uploading files to S3")

uploads = [
    ("../data/sample_customers.csv",
     "raw/customers/sample_customers.csv"),
    ("../data/pipeline_report.json",
     "reports/pipeline_report.json"),
    ("../data/README.md",
     "README.md"),
]

for local_path, s3_key in uploads:
    try:
        s3.upload_file(
            local_path,
            BUCKET_NAME,
            s3_key)
        size = os.path.getsize(local_path)
        print(f"   ✅ Uploaded: "
              f"{s3_key:<45} "
              f"{size} bytes")
        logger.info(
            f"Uploaded: {s3_key}")
    except ClientError as e:
        print(f"   ❌ Upload failed "
              f"{s3_key}: {e}")
        logger.error(
            f"Upload failed {s3_key}: {e}")

# ─────────────────────────────────────────────
# STEP 6: LIST OBJECTS IN BUCKET
# ─────────────────────────────────────────────
print("\n📌 6. LISTING OBJECTS IN BUCKET")

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME)
objects = response.get('Contents', [])

print(f"   Objects in {BUCKET_NAME}:")
total_size = 0
for obj in objects:
    size = obj['Size']
    total_size += size
    print(f"   📄 {obj['Key']:<45} "
          f"{size:>8} bytes")

print(f"\n   Total objects: {len(objects)}")
print(f"   Total size:    "
      f"{total_size:,} bytes")
logger.info(
    f"Bucket has {len(objects)} objects, "
    f"{total_size:,} bytes total")

# ─────────────────────────────────────────────
# STEP 7: DOWNLOAD FROM S3
# ─────────────────────────────────────────────
print("\n📌 7. DOWNLOADING FROM S3")

download_path = "../data/downloaded_report.json"

try:
    s3.download_file(
        BUCKET_NAME,
        "reports/pipeline_report.json",
        download_path)
    size = os.path.getsize(download_path)
    print(f"   ✅ Downloaded: "
          f"pipeline_report.json "
          f"({size} bytes)")

    with open(download_path, 'r') as f:
        downloaded = json.load(f)
    print(f"   Pipeline:  "
          f"{downloaded['pipeline']}")
    print(f"   Churn:     "
          f"{downloaded['churn_rate']}%")
    print(f"   Status:    "
          f"{downloaded['status']}")
    logger.info(
        "File downloaded successfully")

except ClientError as e:
    print(f"   ❌ Download failed: {e}")
    logger.error(
        f"Download failed: {e}")

# ─────────────────────────────────────────────
# STEP 8: GENERATE PRESIGNED URL
# ─────────────────────────────────────────────
print("\n📌 8. GENERATING PRESIGNED URL")
print("   A temporary URL anyone can use")
print("   to access a private S3 file!")

try:
    url = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': BUCKET_NAME,
            'Key': 'reports/pipeline_report.json'
        },
        ExpiresIn=3600)
    print(f"\n   ✅ Presigned URL generated!")
    print(f"   Expires in: 1 hour")
    print(f"   URL: {url[:80]}...")
    logger.info(
        "Presigned URL generated")

except ClientError as e:
    print(f"   ❌ URL generation failed: {e}")

# ─────────────────────────────────────────────
# STEP 9: ADD BUCKET TAGS
# ─────────────────────────────────────────────
print("\n📌 9. TAGGING S3 BUCKET")

try:
    s3.put_bucket_tagging(
        Bucket=BUCKET_NAME,
        Tagging={
            'TagSet': [
                {'Key': 'Project',
                 'Value': 'DE-Bootcamp'},
                {'Key': 'Owner',
                 'Value': 'Lawrence-Koomson'},
                {'Key': 'Environment',
                 'Value': 'Learning'},
                {'Key': 'Week',
                 'Value': 'Week-6'},
            ]
        }
    )
    print(f"   ✅ Tags added to bucket!")
    print(f"   Project:     DE-Bootcamp")
    print(f"   Owner:       Lawrence-Koomson")
    print(f"   Environment: Learning")
    logger.info("Bucket tagged successfully")

except ClientError as e:
    print(f"   ❌ Tagging failed: {e}")

# Save bucket name for future modules
config = {
    "bucket_name": BUCKET_NAME,
    "region": REGION,
    "created_at": datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S")
}
with open("../data/aws_config.json",
          'w') as f:
    json.dump(config, f, indent=4)
print(f"\n   ✅ Config saved: "
      f"aws_config.json")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   boto3.client('s3') → Connect to S3")
print("   create_bucket()    → Create bucket")
print("   upload_file()      → Upload to S3")
print("   download_file()    → Download from S3")
print("   list_objects_v2()  → List objects")
print("   generate_presigned_url() → Share files")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 S3 basics complete")