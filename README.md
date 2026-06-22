# 🚀 Data Engineering Bootcamp
**GetSkills Network & Thrive Africa**
**Student: Lawrence Koomson**
**GitHub: [lawrykoomson](https://github.com/lawrykoomson)**
**LinkedIn: [lawrence-koomson-689774266](https://linkedin.com/in/lawrence-koomson-689774266)**

---

## 📅 Progress Tracker

| Week | Topic | Status |
|------|-------|--------|
| Week 0 | Linux & Bash | ⏳ Pending |
| Week 1 | Python for Data Engineering | ✅ Complete |
| Week 2 | Pandas for Data Engineering | ✅ Complete |
| Week 3 | REST APIs + BigQuery for DE | ✅ Complete |
| Weeks 4-5 | SQL Foundations & SQL for DE | ✅ Complete |
| Week 6 | AWS Cloud for Data Engineering | ✅ Complete |
| Week 7 | AWS Advanced — Lambda, Glue, Step Functions | ✅ Complete |
| Week 8 | Azure Cloud for Data Engineering | ⏸️ Paused — Account setup pending |
| Week 9 | Apache Spark for Data Engineering | ✅ Complete |
| **Final** | **Telco Churn Intelligence Platform** | ✅ **Complete** |

---

## 🏆 Final Capstone — Telco Churn Intelligence Platform

A production-grade, end-to-end Data Engineering pipeline connecting four cloud platforms and data sources into one unified system.

### Pipeline Architecture

Local CSV → AWS S3 Data Lake → Google BigQuery → Apache Spark
↓ ↓ ↓ ↓
REST APIs → Transformation → Risk Scoring → Visualizations → Master Report

### Modules Completed

| Module | File | Technologies |
|--------|------|-------------|
| Module 1 | capstone_ingestion.py | Python, boto3, BigQuery, REST APIs |
| Module 2 | capstone_transform.py | Pandas, NumPy, risk scoring engine |
| Module 3 | capstone_spark.py | PySpark, Spark SQL, window functions |
| Module 4 | capstone_visualize.py | Matplotlib — 5 executive charts |
| Module 5 | capstone_final_report.py | JSON reporting, master summary |

### 🏆 Capstone Results

| Metric | Value |
|--------|-------|
| Total customers analysed | 7,043 |
| Churn rate | 26.54% |
| Monthly revenue | $456,116.60 |
| Annual revenue modelled | $5,473,399.20 |
| Monthly revenue at risk | $139,130.85 |
| Critical risk customers | 2,490 |
| Immediate intervention needed | 924 customers |
| Executive charts generated | 5 |
| Pipeline modules | 5/5 ✅ |

### Technology Stack

| Component | Technology |
|-----------|-----------|
| Data Ingestion | Python, Pandas, boto3 |
| Cloud Data Lake | AWS S3 (eu-north-1) |
| Cloud Data Warehouse | Google BigQuery Sandbox |
| Distributed Processing | Apache Spark 4.1.2 (8 cores) |
| REST APIs | Open-Meteo, REST Countries |
| Visualizations | Matplotlib (5 charts) |
| Reporting | JSON + CSV outputs |

### Key Business Findings

- **Highest churn segment:** Month-to-month + Fiber optic = **54.6% churn rate**
- **Lowest churn segment:** Two-year + No internet = **0.8% churn rate**
- **Top intervention:** 924 customers flagged for immediate phone call
- **Revenue protection:** $139,130.85/month recoverable with targeted retention

---

## 🐍 Week 1 — Python for Data Engineering

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | hello_de.py | Python basics, print, variables |
| Module 2 | data_types.py | Strings, integers, lists, dicts |
| Module 3 | control_flow.py | If/else, loops, functions |
| Module 4 | file_io.py | Read/write CSV and JSON files |
| Module 5 | error_handling.py | Try/except, logging |
| Module 6 | ghana_hr_pipeline.py | Complete HR data pipeline |

### 🏆 Week 1 Capstone — Ghana HR Pipeline

| Metric | Result |
|--------|--------|
| Records processed | 10 |
| Valid records | 7 |
| Rejected records | 3 |
| Runtime | 0.02 seconds |

---

## 🐼 Week 2 — Pandas for Data Engineering

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | explore.py | DataFrame exploration |
| Module 2 | clean.py | Data cleaning, TotalCharges fix |
| Module 3 | analyse.py | Aggregations, groupby |
| Module 4 | merge.py | Joining DataFrames |
| Module 5 | visualise.py | Matplotlib charts |
| Module 6 | telco_pipeline.py | Complete Telco pipeline |

### 🏆 Week 2 Capstone — Telco Churn Pipeline

| Metric | Result |
|--------|--------|
| Rows processed | 7,043 |
| Churn rate | 26.54% |
| Revenue at risk | $139,130.85/month |
| Runtime | 3.55 seconds |

---

## 🌐 Week 3 — REST APIs + BigQuery for DE

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | api_basics.py | requests, REST APIs, JSON parsing |
| Module 2 | weather_pipeline.py | Open-Meteo API, 5 cities |
| Module 3 | ghana_intelligence_pipeline.py | REST Countries + World Bank APIs |
| BQ Module 1 | bq_basics.py | Connect to BQ, public datasets |
| BQ Module 2 | bq_python_client.py | Load CSV, autodetect schema, query |
| BQ Module 3 | bq_advanced_queries.py | Parameterised, JOINs, CTEs, RANK |
| BQ Capstone | bq_api_capstone.py | BigQuery + REST API pipeline |

### 🏆 Week 3 Capstone — BigQuery + REST API Pipeline

| Metric | Result |
|--------|--------|
| Rows loaded to BigQuery | 7,043 |
| BQ tables created | 3 |
| APIs integrated | Open-Meteo + REST Countries |
| Fiber optic churn rate | 41.89% |
| Annual revenue at risk | $5,473,399.20 |
| Runtime | 82.47 seconds |

**BigQuery Skills:**
- BigQuery Sandbox — no credit card needed
- autodetect schema for CSV loading
- Parameterised queries with @parameter syntax
- JOINs, CTEs, window functions, COUNTIF, STDDEV
- load_table_from_dataframe() and job.to_dataframe()

---

## 🗄️ Weeks 4-5 — SQL Foundations & SQL for DE

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | sql_basics.py | SELECT, WHERE, ORDER BY, LIMIT |
| Module 2 | sql_aggregations.py | GROUP BY, HAVING, COUNT, SUM, AVG |
| Module 3 | sql_joins.py | INNER, LEFT, RIGHT, FULL joins |
| Module 4 | sql_subqueries_ctes.py | Subqueries, CTEs, nested SELECT |
| Module 5 | sql_window_functions.py | RANK, DENSE_RANK, PARTITION BY |
| Module 6 | sql_capstone.py | Week 4 pipeline |
| Module 1 | sql_advanced_patterns.py | UNION, PIVOT, COALESCE |
| Module 2 | data_modelling.py | Star Schema — FACT + DIM tables |
| Module 3 | sql_etl.py | INSERT, UPDATE, DELETE, UPSERT |
| Module 4 | sql_views.py | 5 Production views |
| Module 5 | sql_optimisation.py | Indexes, EXPLAIN, Benchmarking |
| Module 6 | sql_de_capstone.py | Complete SQL DE Pipeline |

### 🏆 Week 5 Capstone — SQL Data Engineering Pipeline

| Metric | Result |
|--------|--------|
| Database objects created | 17 |
| Data quality checks | 6/6 passed |
| Customers targeted | 5,174 |
| Annual revenue protected | $3.8M |
| Immediate calls flagged | 924 |
| Runtime | 2.46 seconds |

---

## ☁️ Week 6 — AWS Cloud for Data Engineering

### Modules Completed

| Module | File | AWS Service |
|--------|------|-------------|
| Module 1 | s3_basics.py | S3 — buckets, upload, download |
| Module 2 | s3_data_lake.py | S3 — partitioning, data lake |
| Module 3 | s3_etl_pipeline.py | S3 — Extract, Transform, Load |
| Module 4 | athena_queries.py | Athena — serverless SQL on S3 |
| Capstone | aws_de_capstone.py | Full AWS DE pipeline |

### 🏆 Week 6 Capstone — AWS Telco DE Pipeline

| Metric | Result |
|--------|--------|
| Pipeline stages | 4/4 |
| S3 objects | 63 |
| Data size | 5.5 MB (eu-north-1) |
| Customers analysed | 7,043 |
| Revenue at risk | $139,130.85 |
| Runtime | 36.48 seconds |

---

## ⚡ Week 7 — AWS Advanced Data Engineering

### Modules Completed

| Module | File | AWS Service |
|--------|------|-------------|
| Module 1 | lambda_basics.py | Lambda — serverless functions |
| Module 2 | glue_jobs.py | Glue — managed ETL jobs |
| Module 3 | event_driven_pipeline.py | S3 Events + Lambda triggers |
| Module 4 | step_functions.py | Step Functions — orchestration |
| Capstone | aws_advanced_capstone.py | Full serverless DE pipeline |

### 🏆 Week 7 Capstone — AWS Advanced DE Pipeline

| Metric | Result |
|--------|--------|
| Pipeline stages | 6/6 |
| S3 objects | 89 |
| Lambda functions deployed | 7 |
| Glue ETL runtime | 24 seconds |
| Step Functions status | SUCCEEDED |
| Event-driven trigger | AUTO-TRIGGERED ✅ |
| Runtime | 73.39 seconds |

---

## 🔵 Week 8 — Azure Cloud for Data Engineering

**Status: ⏸️ Paused — Physical debit/credit card required for Azure account setup. Virtual/prepaid cards not supported.**

Will resume once a supported payment method is available.

---

## ⚡ Week 9 — Apache Spark for Data Engineering

### Modules Completed

| Module | File | Skills |
|--------|------|--------|
| Module 1 | spark_basics.py | SparkSession, DataFrames, RDDs |
| Module 2 | spark_dataframe_ops.py | Joins, window functions, when/otherwise |
| Module 3 | spark_sql.py | spark.sql(), CTEs, subqueries, views |
| Module 4 | spark_performance.py | Caching, broadcast joins, explain plans |
| Capstone | spark_de_capstone.py | Complete distributed DE pipeline |

### 🏆 Week 9 Capstone — Spark Telco DE Pipeline

| Metric | Result |
|--------|--------|
| Pipeline stages | 5/5 |
| Rows processed | 7,043 |
| CPU cores used | 8 |
| Data quality checks | 5/5 passed |
| Critical risk customers | 2,490 |
| Revenue at risk | $139,130.85 |
| Cache speedup | 1.7x faster |
| Runtime | 40.28 seconds |

**Environment Setup:**
Configured a full local Spark cluster on Windows from scratch — Java 17, Hadoop winutils, Python version alignment, and JVM worker debugging.

---

## 🛠️ Full Tech Stack

| Category | Technologies |
|----------|-------------|
| **Languages** | Python 3.11, SQL |
| **Libraries** | Pandas, NumPy, Matplotlib, Requests, boto3, PySpark |
| **Databases** | SQLite, Google BigQuery |
| **Cloud — AWS** | S3, Lambda, Glue, Athena, Step Functions, IAM |
| **Cloud — GCP** | BigQuery Sandbox (de-bootcamp-sandbox) |
| **Cloud — Azure** | ⏸️ Pending setup |
| **Big Data** | Apache Spark 4.1.2, Java 17, Hadoop |
| **Version Control** | Git, GitHub |
| **Tools** | VS Code, AWS CLI, gcloud CLI |
| **APIs** | Open-Meteo, REST Countries, World Bank |

---

## 📁 Repository Structure

DE-Bootcamp/
├── week-1/ # Python for DE
├── week-2/ # Pandas for DE
│ └── data/processed/ # telco_clean.csv (shared dataset)
├── week-3/ # REST APIs + BigQuery
│ ├── scripts/ # API scripts
│ └── bigquery/ # BigQuery modules
├── week-4/ # SQL Foundations
├── week-5/ # SQL for DE
├── week-6/ # AWS Cloud
├── week-7/ # AWS Advanced
├── week-9/ # Apache Spark
├── final_capstone/ # Final Capstone Project
│ ├── scripts/ # 5 pipeline modules
│ ├── data/ # Processed datasets
│ ├── reports/ # JSON + CSV reports
│ └── charts/ # 5 executive charts
└── README.md

---

## 🔑 Consistent Results Across All Tools

One dataset, validated across every technology:

| Tool | Churn Rate | Revenue at Risk |
|------|-----------|-----------------|
| Pandas (Week 2) | 26.54% | $139,130.85 |
| SQL/SQLite (Week 5) | 26.54% | $139,130.85 |
| AWS Athena (Week 6) | 26.54% | $139,130.85 |
| Google BigQuery (Week 3) | 26.54% | $456,116.60 total |
| Apache Spark (Week 9) | 26.54% | $139,130.85 |
| Final Capstone | 26.54% | $139,130.85 |

---

## 🔗 Links

- **GitHub:** https://github.com/lawrykoomson/DE-Bootcamp
- **LinkedIn:** https://linkedin.com/in/lawrence-koomson-689774266
- **Bootcamp:** GetSkills Network & Thrive Africa Data Engineering Bootcamp