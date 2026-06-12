# 🛠️ Data Engineering Bootcamp
## GetSkills Network & Thrive Africa

A hands-on Data Engineering bootcamp covering Python,
Pandas, BigQuery, SQL, AWS, Azure and Apache Spark.

---

## 📅 Progress Tracker

| Week | Topic | Status |
|------|-------|--------|
| Week 0 | Linux & Bash | ✅ Complete |
| Week 1 | Python for Data Engineering | ✅ Complete |
| Week 2 | Pandas for Data Engineering | ✅ Complete |
| Week 3 | REST APIs for Data Engineering | ✅ Complete |
| Weeks 4-5 | SQL Foundations & SQL for DE | ✅ Complete |
| Week 6 | AWS Cloud for Data Engineering | ✅ Complete |
| Week 7 | AWS Advanced — Lambda, Glue, Step Functions | ✅ Complete | 
| Weeks 7-10 | AWS Advanced | ⏳ Coming |
| Weeks 8-12 | Azure Cloud | ⏳ Coming |
| Weeks 10-14 | Apache Spark | ⏳ Coming |
| Final | Capstone Project | ⏳ Coming|

---

## 🐍 Week 1 — Python for Data Engineering

### Modules Completed

| Module | File | What Was Built |
|--------|------|----------------|
| Module 1 | hello_de.py | First DE script — pipeline metadata |
| Module 2 | data_types.py | All 7 Python data types for DE |
| Module 3 | control_flow.py | if/else, loops, functions, validation |
| Module 4 | file_io.py | CSV and JSON read/write pipeline |
| Module 5 | error_handling.py | try/except, logging, custom errors |
| Module 6 | ghana_hr_pipeline.py | Complete ETL pipeline — Capstone |

### 🏆 Week 1 Capstone — Ghana HR Data Pipeline
- ✅ 10 records ingested
- ✅ 7 valid records processed
- ❌ 3 records correctly rejected
- ⏱️ Completed in 0.02 seconds
- 📄 4 output files generated

---

## 🐼 Week 2 — Pandas for Data Engineering

**Dataset:** Telco Customer Churn
(7,043 customers, 21 columns, real Kaggle data)

### Modules Completed

| Module | File | What Was Built |
|--------|------|----------------|
| Module 1 | explore.py | Dataset profiling — shape, dtypes, nulls |
| Module 2 | clean.py | TotalCharges fix, type casting, feature engineering |
| Module 3 | analyse.py | Filtering, groupby, churn analysis |
| Module 4 | merge.py | Joins, concat, enriched datasets |
| Module 5 | visualise.py | 6-chart dashboard + correlation heatmap |
| Module 6 | telco_pipeline.py | Complete ETL + Analysis Capstone |

### 🏆 Week 2 Capstone — Telco Churn Analysis Pipeline

A complete production-ready pipeline analysing
customer churn for a telecom company.

**Pipeline Results:**
- ✅ 7,043 customers processed in 3.55 seconds
- 📊 26.54% overall churn rate identified
- 💰 $139,130.85 monthly revenue at risk
- 🔴 Month-to-month = 42.7% churn (highest risk)
- 🟢 Two year contract = 2.8% churn (safest)
- 🔴 New customers (0-12 months) = 47.4% churn
- 🔴 Fiber optic = 41.9% churn rate
- 📄 4 output files generated automatically

**Key Recommendations:**
1. Convert month-to-month to longer contracts
2. Fix first 12 months customer experience
3. Investigate Fiber optic service quality
4. Offer auto-pay incentives
5. Target senior citizens with dedicated support

**Skills Demonstrated:**
- Pandas DataFrames and Series
- Data cleaning and type casting
- Filtering and aggregation with groupby
- Merging and joining multiple DataFrames
- Matplotlib and Seaborn visualisations
- Complete ETL pipeline design

---

## 🌐 Week 3 — REST APIs for Data Engineering

**APIs Used:** REST Countries, Open-Meteo Weather,
World Bank — all free, no API key required!

### Scripts Completed

| Script | File | What Was Built |
|--------|------|----------------|
| Script 1 | api_basics.py | First API calls — Countries + GDP |
| Script 2 | weather_pipeline.py | Live weather for 8 Ghana cities |
| Script 3 | ghana_intelligence_pipeline.py | Full ETL Capstone |

### 🏆 Week 3 Capstone — Ghana Economic Intelligence Pipeline

A complete pipeline combining 3 REST APIs into
one production-ready data product.

**Pipeline Results:**
- ✅ 3 APIs called successfully
- ✅ 5 West African countries profiled
- ✅ 15 GDP records fetched from World Bank
- ✅ 5 Ghana cities with live weather data
- ⏱️ Completed in 38.63 seconds
- 📄 5 output files generated

**Live Data Discovered:**
- Ghana GDP 2024: $82.31 Billion
- Ghana GDP per Capita: $2,439.37
- Nigeria largest economy: $252.3B
- Ghana avg temperature: 23.9°C
- 4 out of 5 cities raining at time of run!

**Skills Demonstrated:**
- requests library for HTTP calls
- REST API consumption and JSON parsing
- Error handling for network failures
- Combining multiple APIs with Pandas
- ETL pipeline with Extract → Transform → Load

## 🗄️ Week 4 — SQL Foundations for Data Engineering

**Database:** SQLite with Telco Customer Churn dataset
**Tool:** Python sqlite3 + Pandas

### Modules Completed

| Module | File | SQL Skills |
|--------|------|------------|
| Module 1 | sql_basics.py | SELECT, WHERE, ORDER BY, LIMIT, LIKE |
| Module 2 | sql_aggregations.py | GROUP BY, COUNT, SUM, AVG, HAVING |
| Module 3 | sql_joins.py | INNER JOIN, LEFT JOIN, Multi-table |
| Module 4 | sql_subqueries_ctes.py | Subqueries, CTEs, Risk scoring |
| Module 5 | sql_window_functions.py | ROW_NUMBER, RANK, NTILE, SUM OVER |
| Module 6 | sql_capstone.py | Complete SQL Analysis Pipeline |

### 🏆 Week 4 Capstone — Telco SQL Analysis Pipeline

A complete SQL-driven analytics pipeline using
SQLite, Python and Pandas.

**Pipeline Results:**
- ✅ 7,043 customers analysed with pure SQL
- 📊 26.54% overall churn rate confirmed
- 💰 $139,130.85 monthly revenue at risk
- 🔴 Month-to-month + Fiber optic = 54.6% churn
- ⚠️ Month 1 churn rate = 62% — critical finding
- 🎯 15 high-value intervention targets identified
- 💎 Diamond tier customers have only 13.8% churn
- 📄 9 risk segments mapped with revenue at risk

**Key SQL Concepts Mastered:**
- SELECT, WHERE, ORDER BY, LIMIT, LIKE
- GROUP BY, COUNT, SUM, AVG, MIN, MAX, HAVING
- INNER JOIN, LEFT JOIN, multi-table joins
- Subqueries (scalar, WHERE, IN)
- CTEs (WITH clause) — single and chained
- Window functions — ROW_NUMBER, RANK,
  DENSE_RANK, NTILE, SUM OVER, AVG OVER
- PARTITION BY and ORDER BY in windows
- CASE WHEN for conditional logic
- Risk scoring models in pure SQL

## 🗄️ Week 5 — SQL for Data Engineering

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | sql_advanced_patterns.py | UNION, PIVOT, COALESCE, String functions |
| Module 2 | data_modelling.py | Star Schema — FACT + DIM tables |
| Module 3 | sql_etl.py | INSERT, UPDATE, DELETE, UPSERT, Transactions |
| Module 4 | sql_views.py | 5 Production Views + Stored Procedures |
| Module 5 | sql_optimisation.py | Indexes, EXPLAIN, Benchmarking |
| Module 6 | sql_de_capstone.py | Complete SQL DE Pipeline |

### 🏆 Week 5 Capstone — SQL Data Engineering Pipeline v2

**Pipeline Results:**
- ✅ 17 database objects — tables + views
- ✅ 6 data quality checks — all passed
- ⏱️ 2.46 seconds end-to-end
- 🎯 5,174 customers targeted for retention
- 💰 $3.8M annual revenue protected
- 🔴 924 customers need immediate call
- 📄 5 output files generated

**SQL DE Skills Mastered:**
- Advanced CASE WHEN and PIVOT patterns
- Star Schema — FACT and DIM table design
- Staging → Validate → Insert pipeline
- INSERT, UPDATE, DELETE, UPSERT
- COMMIT and ROLLBACK transactions
- CREATE VIEW — 5 production views
- EXPLAIN QUERY PLAN analysis
- CREATE INDEX — 5 indexes with benchmarking

## ⚡ Week 7 — AWS Advanced Data Engineering

### Modules Completed

| Module | File | AWS Service |
|--------|------|-------------|
| Module 1 | lambda_basics.py | AWS Lambda — Serverless functions |
| Module 2 | glue_jobs.py | AWS Glue — Managed ETL jobs |
| Module 3 | event_driven_pipeline.py | S3 Events + Lambda triggers |
| Module 4 | step_functions.py | AWS Step Functions — Orchestration |
| Capstone | aws_advanced_capstone.py | Full serverless DE pipeline |

### 🏆 Week 7 Capstone — AWS Advanced DE Pipeline

**Pipeline Results:**
- ✅ 6 out of 6 stages completed
- ⏱️ 73.39 seconds end-to-end
- ☁️ 89 objects in AWS S3 data lake
- ⚡ 7 Lambda functions deployed
- 🔧 1 Glue ETL job — 7,053 records in 24s
- 🔄 Step Functions pipeline — SUCCEEDED
- 🎯 Event-driven trigger — AUTO-TRIGGERED!
- 🔍 Athena SQL — $139,130.85 at risk

**AWS Services Used:**
- S3 — Data lake storage
- Lambda — Serverless Python functions
- Glue — Managed ETL service
- Athena — Serverless SQL on S3
- Step Functions — Pipeline orchestration
- IAM — Security and access management

## 👤 Author
**Lawrence Koomson**
Data Engineering Student | Accra, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrence-koomson-689774266)
🐙 [GitHub](https://github.com/lawrykoomson)