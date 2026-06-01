# ============================================
# MODULE 3 — Control Flow & Functions
# GetSkills Network DE Bootcamp — Week 1
# ============================================

print("=" * 55)
print("   MODULE 3 — CONTROL FLOW & FUNCTIONS")
print("=" * 55)

# ─────────────────────────────────────────────
# 1. IF / ELIF / ELSE — Decision making
# ─────────────────────────────────────────────
print("\n📌 1. IF / ELIF / ELSE")

def classify_project(cost):
    """Classify a project by its cost."""
    if cost >= 50000:
        return "Large Project"
    elif cost >= 25000:
        return "Medium Project"
    elif cost >= 10000:
        return "Small Project"
    else:
        return "Micro Project"

def classify_satisfaction(score):
    """Classify satisfaction score."""
    if score >= 4.5:
        return "⭐ Excellent"
    elif score >= 3.5:
        return "✅ Good"
    elif score >= 2.5:
        return "🟡 Average"
    else:
        return "🔴 Poor"

# Test the functions
test_costs = [5000, 15000, 35000, 55000]
test_scores = [5.0, 4.0, 3.0, 2.0]

print("\n   Project Classification:")
for cost in test_costs:
    print(f"   GHS {cost:,} → {classify_project(cost)}")

print("\n   Satisfaction Classification:")
for score in test_scores:
    print(f"   Score {score} → {classify_satisfaction(score)}")

# ─────────────────────────────────────────────
# 2. FOR LOOPS — Processing collections
# ─────────────────────────────────────────────
print("\n📌 2. FOR LOOPS")

projects = [
    {"id": "PRJ_001", "service": "Data Analytics",
     "cost": 22000, "status": "Completed"},
    {"id": "PRJ_002", "service": "Software Dev",
     "cost": 50000, "status": "Delayed"},
    {"id": "PRJ_003", "service": "Cloud Migration",
     "cost": 28000, "status": "Completed"},
    {"id": "PRJ_004", "service": "Cybersecurity",
     "cost": 18000, "status": "In Progress"},
    {"id": "PRJ_005", "service": "IT Support",
     "cost": 10000, "status": "Delayed"},
]

print("\n   Processing all projects:")
total = 0
completed = 0
delayed = 0

for project in projects:
    total += project["cost"]
    if project["status"] == "Completed":
        completed += 1
    elif project["status"] == "Delayed":
        delayed += 1
    print(f"   {project['id']} | {project['service']:<20} | "
          f"GHS {project['cost']:,} | {project['status']}")

print(f"\n   Total Revenue:    GHS {total:,}")
print(f"   Completed:        {completed}")
print(f"   Delayed:          {delayed}")

# ─────────────────────────────────────────────
# 3. WHILE LOOPS — Retry logic in pipelines
# ─────────────────────────────────────────────
print("\n📌 3. WHILE LOOPS")

print("\n   Simulating pipeline retry logic:")
max_retries = 3
attempt = 1
success = False

while attempt <= max_retries:
    print(f"   Attempt {attempt}/{max_retries} — connecting to data source...")
    if attempt == 3:
        success = True
        print(f"   ✅ Connection successful on attempt {attempt}!")
        break
    else:
        print(f"   ⚠️  Attempt {attempt} failed — retrying...")
    attempt += 1

if not success:
    print("   ❌ All retries failed — pipeline aborted")

# ─────────────────────────────────────────────
# 4. FUNCTIONS — Reusable pipeline components
# ─────────────────────────────────────────────
print("\n📌 4. FUNCTIONS")

def validate_record(record):
    """
    Validate a single data record.
    Returns True if valid, False if invalid.
    """
    errors = []

    if not record.get("id"):
        errors.append("Missing ID")
    if not record.get("service"):
        errors.append("Missing service type")
    if record.get("cost", 0) <= 0:
        errors.append("Invalid cost")
    if record.get("status") not in [
            "Completed", "Delayed", "In Progress"]:
        errors.append("Invalid status")

    if errors:
        return False, errors
    return True, []

def calculate_pipeline_stats(projects):
    """
    Calculate summary statistics for a list
    of project records.
    """
    if not projects:
        return {}

    total_cost = sum(p["cost"] for p in projects)
    avg_cost = total_cost / len(projects)
    completed = sum(
        1 for p in projects
        if p["status"] == "Completed")
    delayed = sum(
        1 for p in projects
        if p["status"] == "Delayed")

    return {
        "total_projects": len(projects),
        "total_revenue": total_cost,
        "avg_cost": round(avg_cost, 2),
        "completed": completed,
        "delayed": delayed,
        "completion_rate": round(
            completed / len(projects) * 100, 1)
    }

# Test validate_record
print("\n   Testing record validation:")
good_record = {
    "id": "PRJ_001",
    "service": "Data Analytics",
    "cost": 22000,
    "status": "Completed"
}
bad_record = {
    "id": "",
    "service": "Data Analytics",
    "cost": -500,
    "status": "Unknown"
}

is_valid, errors = validate_record(good_record)
print(f"   Good record valid: {is_valid}")

is_valid, errors = validate_record(bad_record)
print(f"   Bad record valid:  {is_valid}")
print(f"   Errors found:      {errors}")

# Test calculate_pipeline_stats
print("\n   Pipeline Statistics:")
stats = calculate_pipeline_stats(projects)
for key, value in stats.items():
    print(f"   {key:<20} {value}")

# ─────────────────────────────────────────────
# 5. LIST COMPREHENSIONS — Pythonic filtering
# ─────────────────────────────────────────────
print("\n📌 5. LIST COMPREHENSIONS")

# Filter only completed projects
completed_projects = [
    p for p in projects
    if p["status"] == "Completed"]

# Extract all project IDs
project_ids = [p["id"] for p in projects]

# Get costs above GHS 20,000
high_value = [
    p["cost"] for p in projects
    if p["cost"] >= 20000]

print(f"   Completed projects:  {len(completed_projects)}")
print(f"   All project IDs:     {project_ids}")
print(f"   High value costs:    {high_value}")

print(f"\n{'='*55}")
print("   MODULE 3 SUMMARY")
print(f"{'='*55}")
print("   if/elif/else → Route data based on conditions")
print("   for loops    → Process every record in a dataset")
print("   while loops  → Retry logic for resilient pipelines")
print("   functions    → Reusable validation & transform steps")
print("   list comps   → Fast pythonic filtering")
print(f"\n✅ Module 3 Complete!")