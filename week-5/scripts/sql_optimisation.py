# ============================================
# WEEK 5 — MODULE 5: Query Optimisation
# INDEXES | EXPLAIN | QUERY PLANNING
# GetSkills Network DE Bootcamp
# ============================================

import sqlite3
import pandas as pd
import time
import logging
import os
from pathlib import Path

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
            '../logs/module5.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

W5_DB = Path("../data/telco_w5.db")
conn = sqlite3.connect(str(W5_DB))
cursor = conn.cursor()

print("=" * 55)
print("   MODULE 5 — QUERY OPTIMISATION")
print("   INDEXES | EXPLAIN | BENCHMARKING")
print("=" * 55)

def run_query(sql, description):
    result = pd.read_sql_query(sql, conn)
    print(f"\n   🔍 {description}")
    print(result.to_string(index=False))
    logger.info(
        f"'{description}': {len(result)} rows")
    return result

def benchmark_query(sql, label, runs=100):
    """Time a query over multiple runs."""
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        pd.read_sql_query(sql, conn)
        end = time.perf_counter()
        times.append(end - start)
    avg_ms = sum(times) / len(times) * 1000
    min_ms = min(times) * 1000
    max_ms = max(times) * 1000
    return avg_ms, min_ms, max_ms

# ─────────────────────────────────────────────
# STEP 1: CHECK EXISTING INDEXES
# ─────────────────────────────────────────────
print("\n📌 1. EXISTING INDEXES")
logger.info("Checking existing indexes")

run_query("""
    SELECT name, tbl_name, sql
    FROM sqlite_master
    WHERE type = 'index'
    AND tbl_name = 'customers'
    ORDER BY name
""", "Existing indexes on customers table")

# ─────────────────────────────────────────────
# STEP 2: EXPLAIN QUERY PLAN
# ─────────────────────────────────────────────
print("\n📌 2. EXPLAIN QUERY PLAN")
print("   Shows HOW SQLite will run your query")

plans = [
    ("Scan without index",
     "SELECT * FROM customers WHERE Churn = 1"),
    ("Filter on Contract",
     "SELECT * FROM customers WHERE Contract = 'Month-to-month'"),
    ("Multi-column filter",
     "SELECT * FROM customers WHERE Contract = 'Month-to-month' AND Churn = 1"),
]

for label, sql in plans:
    plan = pd.read_sql_query(
        f"EXPLAIN QUERY PLAN {sql}", conn)
    print(f"\n   Query: {label}")
    print(f"   Plan:  "
          f"{plan['detail'].iloc[0]}")

# ─────────────────────────────────────────────
# STEP 3: BENCHMARK WITHOUT INDEXES
# ─────────────────────────────────────────────
print("\n📌 3. BENCHMARKING WITHOUT INDEXES")
logger.info("Benchmarking without indexes")

benchmark_queries = {
    "Churn filter": """
        SELECT COUNT(*) FROM customers
        WHERE Churn = 1""",
    "Contract filter": """
        SELECT COUNT(*) FROM customers
        WHERE Contract = 'Month-to-month'""",
    "Multi-filter": """
        SELECT COUNT(*) FROM customers
        WHERE Contract = 'Month-to-month'
        AND Churn = 1
        AND MonthlyCharges >= 90""",
    "GROUP BY Contract": """
        SELECT Contract, COUNT(*), AVG(MonthlyCharges)
        FROM customers
        GROUP BY Contract""",
}

print(f"\n   {'Query':<20} "
      f"{'Avg (ms)':>10} "
      f"{'Min (ms)':>10} "
      f"{'Max (ms)':>10}")
print("   " + "-" * 55)

before_times = {}
for label, sql in benchmark_queries.items():
    avg, mn, mx = benchmark_query(sql, label)
    before_times[label] = avg
    print(f"   {label:<20} "
          f"{avg:>10.3f} "
          f"{mn:>10.3f} "
          f"{mx:>10.3f}")

# ─────────────────────────────────────────────
# STEP 4: CREATE INDEXES
# ─────────────────────────────────────────────
print("\n📌 4. CREATING INDEXES")
logger.info("Creating indexes")

indexes = [
    ("idx_customers_churn",
     "CREATE INDEX IF NOT EXISTS "
     "idx_customers_churn "
     "ON customers(Churn)"),
    ("idx_customers_contract",
     "CREATE INDEX IF NOT EXISTS "
     "idx_customers_contract "
     "ON customers(Contract)"),
    ("idx_customers_contract_churn",
     "CREATE INDEX IF NOT EXISTS "
     "idx_customers_contract_churn "
     "ON customers(Contract, Churn)"),
    ("idx_customers_monthly",
     "CREATE INDEX IF NOT EXISTS "
     "idx_customers_monthly "
     "ON customers(MonthlyCharges)"),
    ("idx_customers_tenure",
     "CREATE INDEX IF NOT EXISTS "
     "idx_customers_tenure "
     "ON customers(tenure)"),
]

for name, sql in indexes:
    start = time.perf_counter()
    cursor.execute(sql)
    elapsed = (
        time.perf_counter() - start) * 1000
    print(f"   ✅ {name:<40} "
          f"{elapsed:.1f}ms")

conn.commit()
logger.info(
    f"Created {len(indexes)} indexes")

# ─────────────────────────────────────────────
# STEP 5: EXPLAIN PLAN AFTER INDEXES
# ─────────────────────────────────────────────
print("\n📌 5. QUERY PLANS AFTER INDEXING")

for label, sql in plans:
    plan = pd.read_sql_query(
        f"EXPLAIN QUERY PLAN {sql}", conn)
    print(f"\n   Query: {label}")
    print(f"   Plan:  "
          f"{plan['detail'].iloc[0]}")

# ─────────────────────────────────────────────
# STEP 6: BENCHMARK WITH INDEXES
# ─────────────────────────────────────────────
print("\n📌 6. BENCHMARKING WITH INDEXES")
logger.info("Benchmarking with indexes")

print(f"\n   {'Query':<20} "
      f"{'Before':>10} "
      f"{'After':>10} "
      f"{'Speedup':>10}")
print("   " + "-" * 55)

for label, sql in benchmark_queries.items():
    avg, mn, mx = benchmark_query(sql, label)
    before = before_times[label]
    speedup = before / avg if avg > 0 else 1
    indicator = (
        "🚀" if speedup > 1.5
        else "✅" if speedup > 1.0
        else "➡️")
    print(f"   {label:<20} "
          f"{before:>8.3f}ms "
          f"{avg:>8.3f}ms "
          f"{indicator} {speedup:>4.1f}x")

# ─────────────────────────────────────────────
# STEP 7: BEST PRACTICES
# ─────────────────────────────────────────────
print("\n📌 7. QUERY OPTIMISATION BEST PRACTICES")

best_practices = [
    ("✅ Use SELECT specific columns",
     "Avoid SELECT * in production",
     """SELECT customerID, Contract,
        MonthlyCharges FROM customers
        WHERE Churn = 1 LIMIT 5"""),

    ("✅ Filter early with WHERE",
     "Reduce rows before GROUP BY",
     """SELECT Contract,
        COUNT(*) as churned
        FROM customers
        WHERE Churn = 1
        GROUP BY Contract"""),

    ("✅ Use CTEs for readability",
     "Break complex queries into steps",
     """WITH churned AS (
        SELECT * FROM customers
        WHERE Churn = 1)
        SELECT Contract,
        COUNT(*) as count
        FROM churned
        GROUP BY Contract"""),

    ("✅ Avoid functions on indexed columns",
     "UPPER(Contract) prevents index use",
     """SELECT * FROM customers
        WHERE Contract =
        'Month-to-month'
        LIMIT 3"""),
]

for title, tip, sql in best_practices:
    print(f"\n   {title}")
    print(f"   💡 Tip: {tip}")
    result = pd.read_sql_query(sql, conn)
    print(f"   Result: {len(result)} rows")

# ─────────────────────────────────────────────
# STEP 8: INDEX SUMMARY
# ─────────────────────────────────────────────
print("\n📌 8. ALL INDEXES IN DATABASE")

run_query("""
    SELECT name, tbl_name
    FROM sqlite_master
    WHERE type = 'index'
    AND tbl_name = 'customers'
    ORDER BY name
""", "All indexes on customers table")

conn.close()
logger.info("Module 5 optimisation complete")

print(f"\n{'='*55}")
print("   MODULE 5 SUMMARY")
print(f"{'='*55}")
print("   EXPLAIN QUERY PLAN → See how query runs")
print("   CREATE INDEX       → Speed up lookups")
print("   Composite index    → Multi-column speed")
print("   Benchmark          → Measure before/after")
print("   SELECT specific    → Avoid SELECT *")
print("   Filter early       → WHERE before GROUP")
print("   CTEs               → Readable pipelines")
print(f"\n✅ Module 5 Complete!")
logger.info("Module 5 complete")