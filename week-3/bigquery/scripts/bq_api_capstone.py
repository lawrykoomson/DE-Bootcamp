# ============================================
# WEEK 3 — MODULE 4: BQ + REST API Capstone
# Merge BigQuery data with REST API data
# GetSkills Network DE Bootcamp
# ============================================

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import (
    LoadJobConfig,
    WriteDisposition,
    SourceFormat)

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
            '../logs/capstone.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

PROJECT_ID = "de-bootcamp-sandbox"
DATASET_ID = "telco_de_bootcamp"
TELCO_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}"
    f".telco_customers")

client = bigquery.Client(
    project=PROJECT_ID)

start_time = datetime.now()

print("=" * 55)
print("   WEEK 3 CAPSTONE")
print("   BIGQUERY + REST API PIPELINE")
print("   GetSkills Network DE Bootcamp")
print("=" * 55)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS — ERROR HANDLING
# ─────────────────────────────────────────────
def safe_api_call(
        url, params=None,
        retries=3, timeout=10):
    """
    Safe REST API call with:
    - Retry logic
    - Timeout handling
    - HTTP error checking
    - Graceful fallback
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=timeout)
            response.raise_for_status()
            logger.info(
                f"API success: {url} "
                f"(attempt {attempt})")
            return response.json()

        except requests.Timeout:
            logger.warning(
                f"Timeout attempt "
                f"{attempt}/{retries}: {url}")
            if attempt == retries:
                print(f"   ⚠️ API timeout "
                      f"after {retries} tries")
                return None

        except requests.HTTPError as e:
            logger.error(
                f"HTTP error: {e}")
            print(f"   ⚠️ HTTP error: {e}")
            return None

        except requests.ConnectionError:
            logger.warning(
                f"Connection error "
                f"attempt {attempt}")
            if attempt == retries:
                print(f"   ⚠️ Connection "
                      f"failed after "
                      f"{retries} tries")
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error: {e}")
            print(f"   ⚠️ Error: {e}")
            return None

def safe_bq_query(sql, label):
    """Safe BQ query with error handling."""
    try:
        job = client.query(sql)
        df = job.to_dataframe()
        logger.info(
            f"BQ query '{label}': "
            f"{len(df)} rows")
        return df
    except Exception as e:
        logger.error(
            f"BQ error '{label}': {e}")
        print(f"   ❌ BQ error: {e}")
        return pd.DataFrame()

# ─────────────────────────────────────────────
# STAGE 1: EXTRACT FROM BIGQUERY
# ─────────────────────────────────────────────
print("\n🔧 STAGE 1: EXTRACT FROM BIGQUERY")
logger.info("Stage 1: Extract from BQ")

# Get churn summary by country-equivalent
# (using InternetService as the dimension)
bq_summary = safe_bq_query(f"""
    SELECT
        InternetService as service_type,
        COUNT(*) as customers,
        COUNTIF(Churn = TRUE) as churned,
        ROUND(
            COUNTIF(Churn = TRUE) *
            100.0 / COUNT(*), 2)
            as churn_rate_pct,
        ROUND(
            SUM(MonthlyCharges), 2)
            as total_revenue,
        ROUND(
            AVG(MonthlyCharges), 2)
            as avg_monthly,
        ROUND(
            AVG(tenure), 1)
            as avg_tenure_months
    FROM `{TELCO_TABLE}`
    GROUP BY InternetService
    ORDER BY churn_rate_pct DESC
""", "Churn by service type")

print(f"   ✅ BQ data extracted: "
      f"{len(bq_summary)} segments")
print(bq_summary.to_string(index=False))
logger.info(
    f"BQ extract: {len(bq_summary)} rows")

# ─────────────────────────────────────────────
# STAGE 2: EXTRACT FROM REST APIS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 2: EXTRACT FROM REST APIs")
logger.info("Stage 2: Extract from APIs")

# API 1 — REST Countries (Ghana context)
print("\n   🌍 API 1: Country data "
      "for context...")

countries_data = []
target_countries = [
    "ghana", "nigeria", "kenya",
    "ethiopia", "southafrica"]

for country in target_countries:
    data = safe_api_call(
        f"https://restcountries.com/"
        f"v3.1/name/{country}"
        f"?fullText=true",
        timeout=10)

    if data and isinstance(data, list) and len(data) > 0:
        c = data[0]
        pop = c.get('population', 0)
        name = c.get(
            'name', {}).get(
            'common', country)
        region = c.get('region', 'N/A')
        subregion = c.get(
            'subregion', 'N/A')
        countries_data.append({
            'country': name,
            'population': pop,
            'region': region,
            'subregion': subregion,
            'pop_millions': round(
                pop / 1_000_000, 1)
        })
        print(f"   ✅ {name}: "
              f"{pop/1e6:.1f}M people")
    else:
        print(f"   ⚠️ Skipped: {country}")

countries_df = pd.DataFrame(
    countries_data)
logger.info(
    f"Countries API: "
    f"{len(countries_df)} records")

# API 2 — Open-Meteo weather for Accra
print("\n   🌤️  API 2: Accra weather data...")

weather_data = safe_api_call(
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": 5.6037,
        "longitude": -0.1870,
        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "wind_speed_10m"),
        "timezone": "Africa/Accra"
    }
)

accra_weather = {}
if weather_data:
    current = weather_data.get(
        'current', {})
    accra_weather = {
        'city': 'Accra',
        'temperature_c': current.get(
            'temperature_2m', 'N/A'),
        'humidity_pct': current.get(
            'relative_humidity_2m', 'N/A'),
        'wind_kmh': current.get(
            'wind_speed_10m', 'N/A'),
    }
    print(f"   ✅ Accra: "
          f"{accra_weather['temperature_c']}°C, "
          f"{accra_weather['humidity_pct']}% humidity")
    logger.info(
        f"Weather API: Accra "
        f"{accra_weather['temperature_c']}°C")
else:
    print("   ⚠️ Weather API unavailable")

# ─────────────────────────────────────────────
# STAGE 3: TRANSFORM & MERGE
# ─────────────────────────────────────────────
print("\n🔧 STAGE 3: TRANSFORM & MERGE")
logger.info("Stage 3: Transform and merge")

# Enrich BQ data with context
enriched = bq_summary.copy()
enriched['pipeline_run'] = (
    datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"))
enriched['run_by'] = 'Lawrence Koomson'
enriched['data_source'] = (
    'BigQuery + REST APIs')
enriched['annual_revenue_projection'] = (
    enriched['total_revenue'] * 12
).round(2)
enriched['intervention_priority'] = (
    enriched['churn_rate_pct'].apply(
        lambda r:
        'Immediate' if r > 40
        else 'High' if r > 20
        else 'Standard'))

print(f"   ✅ BQ data enriched!")
print(f"\n   📊 Enriched analysis:")
print(enriched[[
    'service_type', 'customers',
    'churn_rate_pct',
    'annual_revenue_projection',
    'intervention_priority'
]].to_string(index=False))

# ─────────────────────────────────────────────
# STAGE 4: LOAD RESULTS BACK TO BIGQUERY
# ─────────────────────────────────────────────
print("\n🔧 STAGE 4: LOAD BACK TO BIGQUERY")
logger.info("Stage 4: Load to BigQuery")

RESULTS_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}"
    f".pipeline_results")

try:
    job_config = LoadJobConfig(
        autodetect=True,
        write_disposition=(
            WriteDisposition.WRITE_TRUNCATE)
    )

    load_job = client.load_table_from_dataframe(
        enriched,
        RESULTS_TABLE,
        job_config=job_config)

    load_job.result()
    table = client.get_table(RESULTS_TABLE)

    print(f"   ✅ Results loaded to BQ!")
    print(f"   Table: pipeline_results")
    print(f"   Rows:  {table.num_rows}")
    logger.info(
        f"Loaded to BQ: "
        f"{table.num_rows} rows")

except Exception as e:
    print(f"   ⚠️ BQ load: {e}")
    logger.warning(f"BQ load: {e}")

# ─────────────────────────────────────────────
# STAGE 5: SAVE ALL OUTPUTS
# ─────────────────────────────────────────────
print("\n🔧 STAGE 5: SAVING OUTPUTS")
logger.info("Stage 5: Save outputs")

# Save enriched analysis
enriched.to_csv(
    "../data/bq_enriched_analysis.csv",
    index=False)

# Save countries data
if not countries_df.empty:
    countries_df.to_csv(
        "../data/africa_countries.csv",
        index=False)

# Save full report
duration = (
    datetime.now() - start_time
).total_seconds()

report = {
    "pipeline_name":
        "BigQuery + REST API Pipeline",
    "author": "Lawrence Koomson",
    "run_timestamp": datetime.now(
        ).strftime("%Y-%m-%d %H:%M:%S"),
    "duration_seconds": round(
        duration, 2),
    "sources": {
        "bigquery_rows": len(bq_summary),
        "countries_fetched": len(
            countries_df),
        "weather_city": accra_weather.get(
            'city', 'N/A')
    },
    "bigquery": {
        "project": PROJECT_ID,
        "dataset": DATASET_ID,
        "tables_used": [
            "telco_customers",
            "contract_lookup",
            "pipeline_results"
        ]
    },
    "key_findings": {
        "highest_churn_service": (
            enriched.iloc[0][
                'service_type']),
        "highest_churn_rate": float(
            enriched.iloc[0][
                'churn_rate_pct']),
        "total_annual_revenue": float(
            enriched[
                'annual_revenue_projection'
            ].sum()),
        "immediate_intervention": int(
            enriched[
                enriched[
                    'intervention_priority'
                ] == 'Immediate'
            ]['customers'].sum())
    }
}

with open(
        "../data/week3_bq_report.json",
        'w') as f:
    json.dump(report, f, indent=4)

print("   ✅ bq_enriched_analysis.csv")
print("   ✅ africa_countries.csv")
print("   ✅ week3_bq_report.json")
logger.info("All outputs saved")

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("   WEEK 3 BIGQUERY CAPSTONE RESULTS")
print("=" * 55)
print(f"\n   Pipeline: BQ + REST API Pipeline")
print(f"   Author:   Lawrence Koomson")
print(f"   Duration: {duration:.2f}s")
print(f"\n   📊 Data Sources:")
print(f"   BQ rows extracted:   "
      f"{len(bq_summary)}")
print(f"   Countries fetched:   "
      f"{len(countries_df)}")
print(f"   Weather city:        "
      f"{accra_weather.get('city', 'N/A')}")
print(f"\n   ☁️  BigQuery:")
print(f"   Project: {PROJECT_ID}")
print(f"   Dataset: {DATASET_ID}")
print(f"   Tables:  3 used")
print(f"\n   💡 Key Findings:")
if len(enriched) > 0:
    print(f"   Highest churn service: "
          f"{enriched.iloc[0]['service_type']}"
          f" ({enriched.iloc[0]['churn_rate_pct']}%)")
    print(f"   Annual revenue at risk: "
          f"${enriched['annual_revenue_projection'].sum():,.2f}")
    print(f"\n✅ Week 3 BigQuery + REST API "
          f"Capstone Complete!")
logger.info("Week 3 BQ capstone complete!")