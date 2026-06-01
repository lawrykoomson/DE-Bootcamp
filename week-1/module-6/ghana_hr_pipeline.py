# ============================================
# MODULE 6 — CAPSTONE: Ghana HR Data Pipeline
# GetSkills Network DE Bootcamp — Week 1
# A complete production-ready data pipeline
# ============================================

import csv
import json
import logging
import os
from datetime import datetime

# ─────────────────────────────────────────────
# PIPELINE CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Ghana HR Data Ingestion Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"
INPUT_FILE = "data/raw_hr_data.csv"
OUTPUT_FILE = "data/clean_hr_data.csv"
REPORT_FILE = "data/pipeline_report.json"
LOG_FILE = "data/pipeline.log"

VALID_DEPARTMENTS = [
    "Engineering", "Sales", "Marketing",
    "Finance", "HR", "Operations", "IT"
]
VALID_GENDERS = ["Male", "Female", "Other"]
MIN_SALARY = 500
MAX_SALARY = 50000

# ─────────────────────────────────────────────
# SET UP LOGGING
# ─────────────────────────────────────────────
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────
class PipelineError(Exception):
    pass

class ValidationError(PipelineError):
    pass

class FileIngestionError(PipelineError):
    pass

# ─────────────────────────────────────────────
# STEP 1: CREATE RAW DATA
# ─────────────────────────────────────────────
def create_raw_data():
    """Create sample raw HR data with
    intentional quality issues."""
    raw_data = [
        {"emp_id": "EMP001", "name": "Kofi Mensah",
         "department": "Engineering",
         "salary": "8500", "gender": "Male",
         "years_experience": "5",
         "performance_score": "4.2",
         "is_active": "Yes"},
        {"emp_id": "EMP002", "name": "  Ama Asante  ",
         "department": "sales",
         "salary": "6200", "gender": "Female",
         "years_experience": "3",
         "performance_score": "3.8",
         "is_active": "Yes"},
        {"emp_id": "EMP003", "name": "Kwame Boateng",
         "department": "Finance",
         "salary": "-500",
         "gender": "Male",
         "years_experience": "7",
         "performance_score": "4.5",
         "is_active": "Yes"},
        {"emp_id": "EMP004", "name": "Abena Owusu",
         "department": "Marketing",
         "salary": "7100", "gender": "Female",
         "years_experience": "4",
         "performance_score": "4.0",
         "is_active": "No"},
        {"emp_id": "", "name": "Yaw Darko",
         "department": "IT",
         "salary": "9200", "gender": "Male",
         "years_experience": "6",
         "performance_score": "3.5",
         "is_active": "Yes"},
        {"emp_id": "EMP006",
         "name": "Efua Mensah",
         "department": "HR",
         "salary": "5800", "gender": "Female",
         "years_experience": "2",
         "performance_score": "4.1",
         "is_active": "Yes"},
        {"emp_id": "EMP007", "name": "Kojo Asare",
         "department": "Operations",
         "salary": "7500", "gender": "Male",
         "years_experience": "8",
         "performance_score": "4.7",
         "is_active": "Yes"},
        {"emp_id": "EMP008",
         "name": "Akosua Frimpong",
         "department": "Engineering",
         "salary": "9500", "gender": "Female",
         "years_experience": "5",
         "performance_score": "4.3",
         "is_active": "Yes"},
        {"emp_id": "EMP009", "name": "Nana Adu",
         "department": "UNKNOWN_DEPT",
         "salary": "6000", "gender": "Male",
         "years_experience": "3",
         "performance_score": "3.2",
         "is_active": "Yes"},
        {"emp_id": "EMP010",
         "name": "Adwoa Boateng",
         "department": "Finance",
         "salary": "8800", "gender": "Female",
         "years_experience": "6",
         "performance_score": "4.6",
         "is_active": "No"},
    ]
    return raw_data

# ─────────────────────────────────────────────
# STEP 2: WRITE RAW DATA TO CSV
# ─────────────────────────────────────────────
def write_raw_csv(data, filepath):
    """Write raw data to CSV file."""
    try:
        fieldnames = list(data[0].keys())
        with open(filepath, 'w', newline='',
                  encoding='utf-8') as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        logger.info(
            f"Raw data written to {filepath} "
            f"({len(data)} records)")
        return True
    except Exception as e:
        logger.error(
            f"Failed to write raw CSV: {e}")
        return False

# ─────────────────────────────────────────────
# STEP 3: READ CSV
# ─────────────────────────────────────────────
def read_csv(filepath):
    """Safely read CSV file."""
    try:
        records = []
        with open(filepath, 'r',
                  encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(dict(row))
        logger.info(
            f"Loaded {len(records)} records "
            f"from {filepath}")
        return records
    except FileNotFoundError:
        raise FileIngestionError(
            f"Input file not found: {filepath}")
    except Exception as e:
        raise FileIngestionError(
            f"Error reading {filepath}: {e}")

# ─────────────────────────────────────────────
# STEP 4: VALIDATE RECORD
# ─────────────────────────────────────────────
def validate_record(record):
    """Validate a single HR record."""
    if not record.get("emp_id", "").strip():
        raise ValidationError(
            "Missing employee ID")

    if not record.get("name", "").strip():
        raise ValidationError(
            "Missing employee name")

    dept = record.get(
    "department", "").strip().upper()
    if dept not in [d.upper() for d in VALID_DEPARTMENTS]:
        raise ValidationError(
            f"Invalid department: "
            f"'{record['department']}'")

    try:
        salary = float(record.get("salary", 0))
        if salary < MIN_SALARY or salary > MAX_SALARY:
            raise ValidationError(
                f"Salary {salary} out of range "
                f"({MIN_SALARY}-{MAX_SALARY})")
    except ValueError:
        raise ValidationError(
            f"Invalid salary: "
            f"'{record['salary']}'")

    return True

# ─────────────────────────────────────────────
# STEP 5: TRANSFORM RECORD
# ─────────────────────────────────────────────
def transform_record(record):
    """Clean and transform a single HR record."""
    return {
        "emp_id": record["emp_id"].strip().upper(),
        "name": record["name"].strip().title(),
        "department": record[
            "department"].strip().title(),
        "salary_ghs": float(record["salary"]),
        "gender": record["gender"].strip().title(),
        "years_experience": int(
            record["years_experience"]),
        "performance_score": float(
            record["performance_score"]),
        "is_active": record[
            "is_active"].strip() == "Yes",
        "salary_band": (
            "Senior" if float(
                record["salary"]) >= 8000
            else "Mid" if float(
                record["salary"]) >= 5000
            else "Junior"
        ),
        "processed_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
    }

# ─────────────────────────────────────────────
# STEP 6: WRITE CLEAN CSV
# ─────────────────────────────────────────────
def write_clean_csv(records, filepath):
    """Write cleaned records to CSV."""
    try:
        if not records:
            logger.warning("No records to write")
            return False
        fieldnames = list(records[0].keys())
        with open(filepath, 'w', newline='',
                  encoding='utf-8') as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        logger.info(
            f"Clean data written to {filepath} "
            f"({len(records)} records)")
        return True
    except Exception as e:
        logger.error(
            f"Failed to write clean CSV: {e}")
        return False

# ─────────────────────────────────────────────
# STEP 7: GENERATE REPORT
# ─────────────────────────────────────────────
def generate_report(clean_records, stats):
    """Generate a JSON pipeline report."""
    if not clean_records:
        return stats

    salaries = [
        r["salary_ghs"] for r in clean_records]
    dept_counts = {}
    for r in clean_records:
        dept = r["department"]
        dept_counts[dept] = dept_counts.get(
            dept, 0) + 1

    report = {
        "pipeline_name": PIPELINE_NAME,
        "version": PIPELINE_VERSION,
        "author": AUTHOR,
        "run_timestamp": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"),
        "processing_stats": stats,
        "hr_summary": {
            "total_employees": len(clean_records),
            "active_employees": sum(
                1 for r in clean_records
                if r["is_active"]),
            "avg_salary_ghs": round(
                sum(salaries) / len(salaries), 2),
            "max_salary_ghs": max(salaries),
            "min_salary_ghs": min(salaries),
            "avg_performance": round(
                sum(r["performance_score"]
                    for r in clean_records) /
                len(clean_records), 2),
            "departments": dept_counts,
            "salary_bands": {
                "Senior": sum(
                    1 for r in clean_records
                    if r["salary_band"] == "Senior"),
                "Mid": sum(
                    1 for r in clean_records
                    if r["salary_band"] == "Mid"),
                "Junior": sum(
                    1 for r in clean_records
                    if r["salary_band"] == "Junior"),
            }
        }
    }

    with open(REPORT_FILE, 'w',
              encoding='utf-8') as f:
        json.dump(report, f, indent=4)

    logger.info(f"Report written to {REPORT_FILE}")
    return report

# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def run_pipeline():
    """
    Main pipeline orchestrator.
    Runs all steps with full error handling.
    """
    start_time = datetime.now()

    logger.info("=" * 45)
    logger.info(f"  {PIPELINE_NAME}")
    logger.info(f"  Version: {PIPELINE_VERSION}")
    logger.info(f"  Author:  {AUTHOR}")
    logger.info("=" * 45)

    stats = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "rejected_records": [],
        "status": "RUNNING"
    }

    try:
        # Step 1 — Create and write raw data
        logger.info("▶ Step 1: Creating raw data")
        raw_data = create_raw_data()
        write_raw_csv(raw_data, INPUT_FILE)
        logger.info(
            f"✅ Step 1 complete: "
            f"{len(raw_data)} records created")

        # Step 2 — Read raw CSV
        logger.info("▶ Step 2: Reading raw CSV")
        records = read_csv(INPUT_FILE)
        stats["total_records"] = len(records)
        logger.info(
            f"✅ Step 2 complete: "
            f"{len(records)} records loaded")

        # Step 3 — Validate and Transform
        logger.info(
            "▶ Step 3: Validating & transforming")
        clean_records = []

        for record in records:
            emp_id = record.get(
                "emp_id", "UNKNOWN")
            try:
                validate_record(record)
                clean = transform_record(record)
                clean_records.append(clean)
                stats["valid_records"] += 1
                logger.info(
                    f"  ✅ {emp_id} — valid")
            except ValidationError as e:
                stats["invalid_records"] += 1
                stats["rejected_records"].append({
                    "emp_id": emp_id,
                    "reason": str(e)
                })
                logger.warning(
                    f"  ❌ {emp_id} — "
                    f"rejected: {e}")

        logger.info(
            f"✅ Step 3 complete: "
            f"{stats['valid_records']} valid, "
            f"{stats['invalid_records']} rejected")

        # Step 4 — Write clean output
        logger.info("▶ Step 4: Writing clean data")
        write_clean_csv(clean_records, OUTPUT_FILE)
        logger.info("✅ Step 4 complete")

        # Step 5 — Generate report
        logger.info("▶ Step 5: Generating report")
        report = generate_report(
            clean_records, stats)
        logger.info("✅ Step 5 complete")

        stats["status"] = "SUCCESS"

    except FileIngestionError as e:
        logger.error(f"Pipeline failed: {e}")
        stats["status"] = "FAILED"

    except Exception as e:
        logger.error(
            f"Unexpected error: {e}")
        stats["status"] = "FAILED"

    finally:
        duration = (
            datetime.now() - start_time
        ).total_seconds()
        logger.info("=" * 45)
        logger.info(
            f"PIPELINE {stats['status']} "
            f"in {duration:.2f}s")
        logger.info("=" * 45)

    return stats

# ─────────────────────────────────────────────
# RUN & DISPLAY RESULTS
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 Starting Ghana HR Pipeline...")
    print("=" * 55)

    results = run_pipeline()

    print("\n" + "=" * 55)
    print("   PIPELINE RESULTS SUMMARY")
    print("=" * 55)
    print(f"\n   Status:           {results['status']}")
    print(f"   Total Records:    {results['total_records']}")
    print(f"   Valid:            {results['valid_records']}")
    print(f"   Invalid/Rejected: {results['invalid_records']}")

    if results["rejected_records"]:
        print(f"\n   Rejected Records:")
        for r in results["rejected_records"]:
            print(f"   ❌ {r['emp_id']:<10} {r['reason']}")

    print(f"\n   Output Files:")
    for f in [INPUT_FILE, OUTPUT_FILE,
              REPORT_FILE, LOG_FILE]:
        if os.path.exists(f):
            size = os.path.getsize(f)
            print(f"   📄 {f:<35} {size} bytes")

    print(f"\n✅ Week 1 Capstone Complete!")
    print(f"   Ghana HR Pipeline is production ready!")