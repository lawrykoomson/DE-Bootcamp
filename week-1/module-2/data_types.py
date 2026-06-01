# ============================================
# MODULE 2 — Data Types & Variables
# GetSkills Network DE Bootcamp — Week 1
# ============================================

print("=" * 55)
print("   MODULE 2 — DATA TYPES FOR DATA ENGINEERS")
print("=" * 55)

# ─────────────────────────────────────────────
# 1. STRINGS — Used for names, IDs, labels
# ─────────────────────────────────────────────
print("\n📌 1. STRINGS")
client_id = "CLIENT_001"
client_name = "Kofi Mensah"
service_type = "Data Analytics"
status = "active"

print(f"   Client ID:    {client_id}")
print(f"   Client Name:  {client_name}")
print(f"   Service:      {service_type}")
print(f"   Type check:   {type(client_id)}")

# String methods used in data pipelines
print(f"\n   Uppercase:    {client_name.upper()}")
print(f"   Lowercase:    {status.lower()}")
print(f"   Replace:      {service_type.replace('Analytics', 'Engineering')}")
print(f"   Strip spaces: {'  messy data  '.strip()}")
print(f"   Split:        {'John,Mary,Kofi'.split(',')}")

# ─────────────────────────────────────────────
# 2. INTEGERS & FLOATS — Used for amounts, counts
# ─────────────────────────────────────────────
print("\n📌 2. INTEGERS & FLOATS")
project_count = 100
total_revenue = 2752500
avg_satisfaction = 3.58
delay_rate = 0.26

print(f"   Project Count:    {project_count} (type: {type(project_count).__name__})")
print(f"   Total Revenue:    GHS {total_revenue:,} (type: {type(total_revenue).__name__})")
print(f"   Avg Satisfaction: {avg_satisfaction} (type: {type(avg_satisfaction).__name__})")
print(f"   Delay Rate:       {delay_rate * 100:.1f}%")

# Math operations
print(f"\n   Revenue per project: GHS {total_revenue / project_count:,.2f}")
print(f"   Delayed projects:    {int(project_count * delay_rate)}")
print(f"   Rounded avg:         {round(avg_satisfaction, 1)}")

# ─────────────────────────────────────────────
# 3. BOOLEANS — Used for flags and conditions
# ─────────────────────────────────────────────
print("\n📌 3. BOOLEANS")
is_repeat_client = True
is_delayed = False
is_active = True

print(f"   Repeat Client:  {is_repeat_client} (type: {type(is_repeat_client).__name__})")
print(f"   Is Delayed:     {is_delayed}")
print(f"   Is Active:      {is_active}")
print(f"   Not Delayed:    {not is_delayed}")
print(f"   Repeat AND Active: {is_repeat_client and is_active}")

# ─────────────────────────────────────────────
# 4. LISTS — Used for collections of records
# ─────────────────────────────────────────────
print("\n📌 4. LISTS")
service_types = [
    "Software Dev",
    "Cloud Migration",
    "Data Analytics",
    "Cybersecurity Audit",
    "IT Infrastructure",
    "IT Support Package",
    "Website Development"
]

print(f"   Services:       {service_types}")
print(f"   Count:          {len(service_types)}")
print(f"   First:          {service_types[0]}")
print(f"   Last:           {service_types[-1]}")
print(f"   First 3:        {service_types[:3]}")

# List operations
service_types.append("AI Consulting")
print(f"   After append:   {len(service_types)} services")
service_types.remove("AI Consulting")
print(f"   After remove:   {len(service_types)} services")

# ─────────────────────────────────────────────
# 5. DICTIONARIES — Used for data records
# ─────────────────────────────────────────────
print("\n📌 5. DICTIONARIES")
project_record = {
    "project_id": "PRJ_001",
    "client_id": "CLIENT_001",
    "service_type": "Data Analytics",
    "cost_ghs": 22000,
    "duration_days": 30,
    "satisfaction": 4,
    "repeat_client": True,
    "status": "Completed"
}

print(f"   Project ID:     {project_record['project_id']}")
print(f"   Service:        {project_record['service_type']}")
print(f"   Cost:           GHS {project_record['cost_ghs']:,}")
print(f"   Status:         {project_record['status']}")
print(f"   Keys:           {list(project_record.keys())}")
print(f"   Values count:   {len(project_record)}")

# Update a value
project_record["status"] = "Invoiced"
print(f"   Updated status: {project_record['status']}")

# ─────────────────────────────────────────────
# 6. TUPLES — Used for fixed/constant data
# ─────────────────────────────────────────────
print("\n📌 6. TUPLES")
pipeline_config = ("Ghana_HR_Pipeline", "1.0.0", "production")
valid_statuses = ("Completed", "Delayed", "In Progress")

print(f"   Pipeline config: {pipeline_config}")
print(f"   Valid statuses:  {valid_statuses}")
print(f"   Pipeline name:   {pipeline_config[0]}")
print(f"   Is valid status: {'Completed' in valid_statuses}")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
print(f"\n{'='*55}")
print("   DATA TYPES SUMMARY")
print(f"{'='*55}")
print(f"   str   → Client names, IDs, service types")
print(f"   int   → Counts, project IDs, durations")
print(f"   float → Revenue amounts, rates, scores")
print(f"   bool  → Flags like is_active, is_delayed")
print(f"   list  → Collections of records or values")
print(f"   dict  → Individual data records (like a row)")
print(f"   tuple → Fixed config that never changes")
print(f"\n✅ Module 2 Complete!")