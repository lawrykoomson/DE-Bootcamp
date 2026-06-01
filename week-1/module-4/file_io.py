# ============================================
# MODULE 4 — File I/O for Data Engineering
# GetSkills Network DE Bootcamp — Week 1
# ============================================

import csv
import json
import os
from datetime import datetime

print("=" * 55)
print("   MODULE 4 — FILE I/O FOR DATA ENGINEERS")
print("=" * 55)

# ─────────────────────────────────────────────
# 1. CREATE SAMPLE RAW DATA
# ─────────────────────────────────────────────
print("\n📌 1. CREATING SAMPLE RAW DATA")

raw_projects = [
    {"project_id": "PRJ_001", "client_id": "C001",
     "service": "Data Analytics", "cost": 22000,
     "duration": 30, "satisfaction": 4,
     "repeat_client": "Yes", "status": "Completed"},
    {"project_id": "PRJ_002", "client_id": "C002",
     "service": "Software Dev", "cost": 50000,
     "duration": 65, "satisfaction": 2,
     "repeat_client": "No", "status": "Delayed"},
    {"project_id": "PRJ_003", "client_id": "C003",
     "service": "Cloud Migration", "cost": 28000,
     "duration": 25, "satisfaction": 5,
     "repeat_client": "Yes", "status": "Completed"},
    {"project_id": "PRJ_004", "client_id": "C004",
     "service": "Cybersecurity Audit", "cost": 18000,
     "duration": 18, "satisfaction": 3,
     "repeat_client": "No", "status": "Completed"},
    {"project_id": "PRJ_005", "client_id": "C005",
     "service": "IT Support Package", "cost": 10000,
     "duration": 7, "satisfaction": 4,
     "repeat_client": "No", "status": "Completed"},
    {"project_id": "PRJ_006", "client_id": "C006",
     "service": "Website Development", "cost": 8000,
     "duration": 10, "satisfaction": 3,
     "repeat_client": "No", "status": "Delayed"},
    {"project_id": "PRJ_007", "client_id": "C007",
     "service": "IT Infrastructure", "cost": 35000,
     "duration": 40, "satisfaction": 4,
     "repeat_client": "Yes", "status": "Completed"},
]

print(f"   Created {len(raw_projects)} sample project records")

# ─────────────────────────────────────────────
# 2. WRITE CSV FILE
# ─────────────────────────────────────────────
print("\n📌 2. WRITING CSV FILE")

csv_filename = "raw_projects.csv"
fieldnames = list(raw_projects[0].keys())

with open(csv_filename, 'w', newline='',
          encoding='utf-8') as csvfile:
    writer = csv.DictWriter(
        csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(raw_projects)

print(f"   ✅ CSV written:  {csv_filename}")
print(f"   Rows written:    {len(raw_projects)}")
print(f"   Columns:         {fieldnames}")

# ─────────────────────────────────────────────
# 3. READ CSV FILE
# ─────────────────────────────────────────────
print("\n📌 3. READING CSV FILE")

loaded_projects = []
with open(csv_filename, 'r',
          encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        loaded_projects.append(dict(row))

print(f"   ✅ CSV loaded:   {csv_filename}")
print(f"   Rows loaded:     {len(loaded_projects)}")
print(f"\n   First record:")
for key, value in loaded_projects[0].items():
    print(f"   {key:<20} {value}")

# ─────────────────────────────────────────────
# 4. TRANSFORM THE DATA
# ─────────────────────────────────────────────
print("\n📌 4. TRANSFORMING DATA")

cleaned_projects = []
for record in loaded_projects:
    cleaned = {
        "project_id": record["project_id"].strip(),
        "client_id": record["client_id"].strip(),
        "service": record["service"].strip(),
        "cost_ghs": int(record["cost"]),
        "duration_days": int(record["duration"]),
        "satisfaction_score": float(
            record["satisfaction"]),
        "is_repeat_client": record[
            "repeat_client"] == "Yes",
        "completion_status": record[
            "status"].strip(),
        "processed_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
    }
    cleaned_projects.append(cleaned)

print(f"   ✅ Transformed {len(cleaned_projects)} records")
print(f"\n   Sample cleaned record:")
for key, value in cleaned_projects[0].items():
    print(f"   {key:<25} {value}")

# ─────────────────────────────────────────────
# 5. WRITE CLEAN CSV
# ─────────────────────────────────────────────
print("\n📌 5. WRITING CLEAN CSV")

clean_csv = "clean_projects.csv"
clean_fields = list(cleaned_projects[0].keys())

with open(clean_csv, 'w', newline='',
          encoding='utf-8') as csvfile:
    writer = csv.DictWriter(
        csvfile, fieldnames=clean_fields)
    writer.writeheader()
    writer.writerows(cleaned_projects)

print(f"   ✅ Clean CSV written: {clean_csv}")

# ─────────────────────────────────────────────
# 6. WRITE JSON REPORT
# ─────────────────────────────────────────────
print("\n📌 6. WRITING JSON REPORT")

total_cost = sum(p["cost_ghs"] for p in cleaned_projects)
completed = [p for p in cleaned_projects
             if p["completion_status"] == "Completed"]
delayed = [p for p in cleaned_projects
           if p["completion_status"] == "Delayed"]

report = {
    "pipeline_name": "TechSolutions Project Pipeline",
    "run_timestamp": datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"),
    "summary": {
        "total_projects": len(cleaned_projects),
        "total_revenue_ghs": total_cost,
        "avg_cost_ghs": round(
            total_cost / len(cleaned_projects), 2),
        "completed_projects": len(completed),
        "delayed_projects": len(delayed),
        "completion_rate_pct": round(
            len(completed) /
            len(cleaned_projects) * 100, 1)
    },
    "status": "SUCCESS"
}

json_filename = "pipeline_report.json"
with open(json_filename, 'w',
          encoding='utf-8') as jsonfile:
    json.dump(report, jsonfile,
              indent=4, ensure_ascii=False)

print(f"   ✅ JSON report written: {json_filename}")

# Read back and display
with open(json_filename, 'r',
          encoding='utf-8') as jsonfile:
    loaded_report = json.load(jsonfile)

print(f"\n   Report contents:")
print(f"   Pipeline:    {loaded_report['pipeline_name']}")
print(f"   Timestamp:   {loaded_report['run_timestamp']}")
print(f"   Projects:    {loaded_report['summary']['total_projects']}")
print(f"   Revenue:     GHS {loaded_report['summary']['total_revenue_ghs']:,}")
print(f"   Completion:  {loaded_report['summary']['completion_rate_pct']}%")
print(f"   Status:      {loaded_report['status']}")

# ─────────────────────────────────────────────
# 7. LIST FILES CREATED
# ─────────────────────────────────────────────
print(f"\n📌 7. FILES CREATED IN THIS MODULE")
files = [csv_filename, clean_csv, json_filename]
for f in files:
    size = os.path.getsize(f)
    print(f"   📄 {f:<30} {size} bytes")

print(f"\n{'='*55}")
print("   MODULE 4 SUMMARY")
print(f"{'='*55}")
print("   open() + csv.DictWriter → Write CSV files")
print("   open() + csv.DictReader → Read CSV files")
print("   json.dump()             → Write JSON files")
print("   json.load()             → Read JSON files")
print("   os.path.getsize()       → Check file sizes")
print(f"\n✅ Module 4 Complete!")