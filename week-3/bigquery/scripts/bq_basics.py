# ============================================
# WEEK 3 — BIGQUERY MODULE 1: BQ Basics
# Connect, Query Public Datasets, Load Data
# GetSkills Network DE Bootcamp
# ============================================

import os
import logging
from datetime import datetime
from google.cloud import bigquery

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
# YOUR PROJECT ID
# ─────────────────────────────────────────────
# Find this in your BigQuery console —
# it's the name next to the Sandbox label
# e.g. "de-bootcamp-sandbox" or similar
PROJECT_ID = "de-bootcamp-sandbox"

print("=" * 55)
print("   MODULE 1 — BIGQUERY BASICS")
print("   CONNECT | QUERY | PUBLIC DATASETS")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: CONNECT TO BIGQUERY
# ─────────────────────────────────────────────
print("\n📌 1. CONNECTING TO BIGQUERY")
logger.info("Connecting to BigQuery")

client = bigquery.Client(
    project=PROJECT_ID)

print(f"   ✅ Connected to BigQuery!")
print(f"   Project: {client.project}")
print(f"   Location: US (default)")
logger.info(
    f"Connected: project={client.project}")

# ─────────────────────────────────────────────
# STEP 2: WHAT IS BIGQUERY?
# ─────────────────────────────────────────────
print("\n📌 2. WHAT IS BIGQUERY?")
print("""
   BigQuery is Google's serverless
   cloud data warehouse:

   ┌─────────────────────────────────┐
   │  No servers to manage           │
   │  SQL queries on massive data    │
   │  Petabyte-scale analytics       │
   │  Pay per query (not per server) │
   │  Built-in ML capabilities       │
   └─────────────────────────────────┘

   BigQuery vs Athena:
   BigQuery  → Google Cloud (GCP)
   Athena    → AWS

   Both do the same thing —
   SQL queries directly on files
   with no database server!

   BigQuery Sandbox:
   ✅ Free — no credit card
   ✅ 1 TB queries/month
   ✅ 10 GB storage
   ✅ Access to public datasets
""")

# ─────────────────────────────────────────────
# STEP 3: QUERY A PUBLIC DATASET
# ─────────────────────────────────────────────
print("\n📌 3. QUERYING PUBLIC DATASETS")
print("   BigQuery has hundreds of free")
print("   public datasets to practise on!")
logger.info("Querying public datasets")

# Query 1 — World population data
print("\n   🔍 World population by country:")
query1 = """
    SELECT
        word,
        corpus,
        word_count
    FROM
        `bigquery-public-data.samples.shakespeare`
    WHERE word_count > 500
    ORDER BY word_count DESC
    LIMIT 10
"""

query_job = client.query(query1)
results = query_job.result()

print(f"   {'Word':<20} {'Corpus':<25} "
      f"{'Count':>8}")
print("   " + "-" * 55)
for row in results:
    print(f"   {row.word:<20} "
          f"{row.corpus:<25} "
          f"{row.word_count:>8,}")

logger.info("Public dataset query complete")

# ─────────────────────────────────────────────
# STEP 4: QUERY AFRICA-SPECIFIC DATA
# ─────────────────────────────────────────────
print("\n📌 4. GITHUB ACTIVITY DATA")
logger.info("Querying GitHub public data")

print("\n   🔍 Top programming languages "
      "on GitHub:")
query2 = """
    SELECT
        language,
        COUNT(*) as repo_count
    FROM
        `bigquery-public-data.github_repos.languages`
    WHERE language IS NOT NULL
    GROUP BY language
    ORDER BY repo_count DESC
    LIMIT 10
"""

try:
    query_job2 = client.query(query2)
    results2 = query_job2.result()
    rows = list(results2)

    if rows:
        print(f"\n   {'Language':<25} "
              f"{'Repos':>12}")
        print("   " + "-" * 40)
        for row in rows:
            print(
                f"   {row.language:<25} "
                f"{row.repo_count:>12,}")
    else:
        print("   No results returned")
    logger.info(
        f"GitHub data: {len(rows)} languages")

except Exception as e:
    print(f"   ⚠️ Skipping: {e}")
    logger.warning(f"GitHub query: {e}")

# ─────────────────────────────────────────────
# STEP 5: CREATE YOUR OWN DATASET
# ─────────────────────────────────────────────
print("\n📌 5. CREATING YOUR OWN DATASET")
logger.info("Creating BigQuery dataset")

DATASET_ID = "telco_de_bootcamp"
dataset_ref = bigquery.Dataset(
    f"{PROJECT_ID}.{DATASET_ID}")
dataset_ref.location = "US"
dataset_ref.description = (
    "Telco Churn DE Bootcamp Dataset — "
    "Lawrence Koomson")

try:
    dataset = client.create_dataset(
        dataset_ref, exists_ok=True)
    print(f"   ✅ Dataset created: "
          f"{DATASET_ID}")
    print(f"   Location: {dataset.location}")
    print(f"   Project:  {dataset.project}")
    logger.info(
        f"Dataset created: {DATASET_ID}")

except Exception as e:
    print(f"   ❌ Error: {e}")
    logger.error(f"Dataset error: {e}")

# ─────────────────────────────────────────────
# STEP 6: LIST DATASETS IN YOUR PROJECT
# ─────────────────────────────────────────────
print("\n📌 6. LISTING YOUR DATASETS")

datasets = list(
    client.list_datasets())
print(f"   Datasets in {PROJECT_ID}:")
for ds in datasets:
    print(f"   📦 {ds.dataset_id}")
logger.info(
    f"Found {len(datasets)} datasets")

# ─────────────────────────────────────────────
# STEP 7: BQ QUERY JOB DETAILS
# ─────────────────────────────────────────────
print("\n📌 7. UNDERSTANDING QUERY JOBS")
print("""
   Every BigQuery query is a JOB:

   client.query(sql)    → Submits job
   job.result()         → Wait for result
   job.job_id           → Unique job ID
   job.total_bytes_processed → Cost info

   Query jobs are:
   ✅ Asynchronous by default
   ✅ Trackable by job ID
   ✅ Auditable in Cloud Console
   ✅ Retryable on failure
""")

print("\n   🔍 Query job details:")
simple_query = """
    SELECT COUNT(*) as total
    FROM `bigquery-public-data.samples.shakespeare`
"""
job = client.query(simple_query)
result = job.result()
for row in result:
    print(f"   Shakespeare words: "
          f"{row.total:,}")

print(f"   Job ID:    {job.job_id}")
print(f"   Bytes:     "
      f"{job.total_bytes_processed:,}")
print(f"   MB:        "
      f"{job.total_bytes_processed/1024/1024:.4f}")
logger.info(
    f"Job {job.job_id}: "
    f"{job.total_bytes_processed:,} bytes")

print(f"\n{'='*55}")
print("   MODULE 1 SUMMARY")
print(f"{'='*55}")
print("   bigquery.Client()  → Connect to BQ")
print("   client.query(sql)  → Run a query")
print("   job.result()       → Get results")
print("   Public datasets    → Free data!")
print("   create_dataset()   → Your own DB")
print("   Sandbox            → Free, no card!")
print(f"\n✅ Module 1 Complete!")
logger.info("Module 1 BQ basics complete")