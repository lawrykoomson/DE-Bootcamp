# ============================================
# MODULE 5 — Error Handling & Resilience
# GetSkills Network DE Bootcamp — Week 1
# ============================================

import csv
import json
import logging
import os
from datetime import datetime

# ─────────────────────────────────────────────
# 1. SET UP LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log',
                            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

print("=" * 55)
print("   MODULE 5 — ERROR HANDLING & RESILIENCE")
print("=" * 55)

# ─────────────────────────────────────────────
# 2. BASIC TRY / EXCEPT
# ─────────────────────────────────────────────
print("\n📌 1. BASIC TRY / EXCEPT")

def safe_divide(a, b):
    """Safely divide two numbers."""
    try:
        result = a / b
        return result
    except ZeroDivisionError:
        logger.error(f"Division by zero: {a} / {b}")
        return None
    except TypeError as e:
        logger.error(f"Type error in division: {e}")
        return None

def safe_convert_to_int(value):
    """Safely convert a value to integer."""
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Cannot convert '{value}' to int: {e}")
        return None

# Test safe operations
print(f"\n   10 / 2 = {safe_divide(10, 2)}")
print(f"   10 / 0 = {safe_divide(10, 0)}")
print(f"   Convert '42' = {safe_convert_to_int('42')}")
print(f"   Convert 'abc' = {safe_convert_to_int('abc')}")
print(f"   Convert None = {safe_convert_to_int(None)}")

# ─────────────────────────────────────────────
# 3. CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────
print("\n📌 2. CUSTOM EXCEPTIONS")

class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass

class ValidationError(PipelineError):
    """Raised when data validation fails."""
    pass

class FileIngestionError(PipelineError):
    """Raised when file reading fails."""
    pass

def validate_project_record(record):
    """
    Validate a project record.
    Raises ValidationError if invalid.
    """
    required_fields = [
        "project_id", "service",
        "cost", "status"]

    for field in required_fields:
        if field not in record or not record[field]:
            raise ValidationError(
                f"Missing required field: '{field}'")

    valid_statuses = [
        "Completed", "Delayed", "In Progress"]
    if record["status"] not in valid_statuses:
        raise ValidationError(
            f"Invalid status: '{record['status']}'. "
            f"Must be one of {valid_statuses}")

    if float(record.get("cost", 0)) <= 0:
        raise ValidationError(
            f"Invalid cost: {record['cost']}. "
            f"Must be greater than 0")

    return True

# Test custom exceptions
test_records = [
    {"project_id": "PRJ_001",
     "service": "Data Analytics",
     "cost": 22000, "status": "Completed"},
    {"project_id": "PRJ_002",
     "service": "Software Dev",
     "cost": -500, "status": "Completed"},
    {"project_id": "",
     "service": "Cloud Migration",
     "cost": 28000, "status": "Unknown"},
]

print(f"\n   Testing record validation:")
for i, record in enumerate(test_records, 1):
    try:
        validate_project_record(record)
        logger.info(
            f"Record {i} ({record.get('project_id', 'N/A')}) "
            f"validated successfully")
        print(f"   Record {i}: ✅ Valid")
    except ValidationError as e:
        logger.warning(f"Record {i} failed: {e}")
        print(f"   Record {i}: ❌ Invalid — {e}")

# ─────────────────────────────────────────────
# 4. TRY / EXCEPT / FINALLY
# ─────────────────────────────────────────────
print("\n📌 3. TRY / EXCEPT / FINALLY")

def read_csv_safe(filepath):
    """
    Safely read a CSV file.
    Always logs whether it succeeded or failed.
    """
    records = []
    logger.info(f"Attempting to read: {filepath}")

    try:
        with open(filepath, 'r',
                  encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))
        logger.info(
            f"Successfully loaded {len(records)} "
            f"records from {filepath}")
        return records

    except FileNotFoundError:
        logger.error(
            f"File not found: {filepath}")
        return []

    except PermissionError:
        logger.error(
            f"Permission denied: {filepath}")
        return []

    except Exception as e:
        logger.error(
            f"Unexpected error reading {filepath}: {e}")
        return []

    finally:
        logger.info(
            f"Finished processing: {filepath}")

# Test with existing and non-existing files
print(f"\n   Reading existing file:")
records = read_csv_safe("../module-4/raw_projects.csv")
print(f"   Loaded {len(records)} records")

print(f"\n   Reading non-existing file:")
records = read_csv_safe("missing_file.csv")
print(f"   Loaded {len(records)} records")

# ─────────────────────────────────────────────
# 5. RESILIENT PIPELINE WITH LOGGING
# ─────────────────────────────────────────────
print("\n📌 4. RESILIENT PIPELINE FUNCTION")

def run_pipeline(input_file, output_file):
    """
    A resilient data pipeline with full
    error handling and logging.
    """
    pipeline_start = datetime.now()
    logger.info("=" * 40)
    logger.info("PIPELINE STARTED")
    logger.info(f"Input:  {input_file}")
    logger.info(f"Output: {output_file}")
    logger.info("=" * 40)

    stats = {
        "total_read": 0,
        "valid": 0,
        "invalid": 0,
        "errors": []
    }

    try:
        # Step 1: Read input file
        logger.info("Step 1: Reading input file")
        raw_records = read_csv_safe(input_file)

        if not raw_records:
            raise FileIngestionError(
                f"No records loaded from {input_file}")

        stats["total_read"] = len(raw_records)
        logger.info(
            f"Step 1 complete: {len(raw_records)} "
            f"records loaded")

        # Step 2: Validate records
        logger.info("Step 2: Validating records")
        valid_records = []

        for record in raw_records:
            try:
                validate_project_record(record)
                valid_records.append(record)
                stats["valid"] += 1
            except ValidationError as e:
                stats["invalid"] += 1
                stats["errors"].append(str(e))
                logger.warning(
                    f"Skipping invalid record "
                    f"{record.get('project_id')}: {e}")

        logger.info(
            f"Step 2 complete: {stats['valid']} valid, "
            f"{stats['invalid']} invalid")

        # Step 3: Write output
        logger.info("Step 3: Writing output file")
        if valid_records:
            with open(output_file, 'w',
                      newline='',
                      encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=valid_records[0].keys())
                writer.writeheader()
                writer.writerows(valid_records)
            logger.info(
                f"Step 3 complete: {len(valid_records)}"
                f" records written to {output_file}")

    except FileIngestionError as e:
        logger.error(f"Pipeline failed: {e}")
        stats["errors"].append(str(e))

    except Exception as e:
        logger.error(
            f"Unexpected pipeline error: {e}")
        stats["errors"].append(str(e))

    finally:
        duration = (
            datetime.now() - pipeline_start
        ).total_seconds()
        logger.info(
            f"Pipeline finished in {duration:.2f}s")
        logger.info(f"Stats: {stats}")

    return stats

# Run the pipeline
print(f"\n   Running resilient pipeline...")
result = run_pipeline(
    "../module-4/raw_projects.csv",
    "validated_projects.csv"
)

print(f"\n   Pipeline Results:")
print(f"   Total read:  {result['total_read']}")
print(f"   Valid:       {result['valid']}")
print(f"   Invalid:     {result['invalid']}")

# Check log file was created
if os.path.exists("pipeline.log"):
    size = os.path.getsize("pipeline.log")
    print(f"\n   📄 Log file created: pipeline.log "
          f"({size} bytes)")

print(f"\n{'='*55}")
print("   MODULE 5 SUMMARY")
print(f"{'='*55}")
print("   try/except      → Catch and handle errors")
print("   except finally  → Always runs (cleanup)")
print("   Custom errors   → ValidationError, etc")
print("   logging module  → Professional log files")
print("   Resilient funcs → Never crash silently")
print(f"\n✅ Module 5 Complete!")