# ============================================
# WEEK 3 — MODULE 3 CAPSTONE:
# Ghana Economic Intelligence Pipeline
# Combines REST APIs + Pandas + JSON outputs
# GetSkills Network DE Bootcamp
# ============================================

import requests
import pandas as pd
import json
import logging
import os
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PIPELINE_NAME = "Ghana Economic Intelligence Pipeline"
PIPELINE_VERSION = "1.0.0"
AUTHOR = "Lawrence Koomson"

PROCESSED_DIR = Path("../data/processed")
RAW_DIR = Path("../data/raw")
LOGS_DIR = Path("../logs")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOGS_DIR / 'capstone_pipeline.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────
class APIError(Exception):
    pass

class PipelineError(Exception):
    pass

# ─────────────────────────────────────────────
# API HELPER FUNCTION
# ─────────────────────────────────────────────
def fetch_api(url, name, timeout=15):
    """Safely fetch data from any REST API."""
    logger.info(f"Fetching {name}: {url[:60]}...")
    try:
        response = requests.get(
            url, timeout=timeout)
        if response.status_code == 200:
            logger.info(
                f"{name} — ✅ {response.status_code}")
            return response.json()
        else:
            raise APIError(
                f"{name} returned "
                f"{response.status_code}")
    except requests.exceptions.Timeout:
        raise APIError(f"{name} timed out")
    except requests.exceptions.ConnectionError:
        raise APIError(
            f"{name} — no connection")

# ─────────────────────────────────────────────
# EXTRACT FUNCTIONS
# ─────────────────────────────────────────────
def extract_country_data(countries):
    """Fetch country profile data."""
    logger.info(
        f"Extracting data for "
        f"{len(countries)} countries")
    records = []
    for country in countries:
        try:
            url = (f"https://restcountries.com/"
                   f"v3.1/name/{country}")
            data = fetch_api(
                url, f"Country:{country}")
            c = data[0]
            records.append({
                "name": c['name']['common'],
                "official_name": c['name'][
                    'official'],
                "capital": c.get(
                    'capital', ['N/A'])[0],
                "population": c['population'],
                "area_km2": c.get('area', 0),
                "region": c['region'],
                "subregion": c.get(
                    'subregion', 'N/A'),
                "currency_code": list(
                    c['currencies'].keys()
                )[0] if c.get(
                    'currencies') else 'N/A',
                "currency_name": list(
                    c['currencies'].values()
                )[0]['name'] if c.get(
                    'currencies') else 'N/A',
                "languages": ', '.join(
                    c.get('languages',
                          {}).values()),
                "timezones": c.get(
                    'timezones', ['N/A'])[0],
                "fetched_at": datetime.now(
                    ).strftime(
                    "%Y-%m-%d %H:%M:%S")
            })
        except APIError as e:
            logger.warning(
                f"Skipping {country}: {e}")
    return records

def extract_gdp_data(country_codes):
    """Fetch GDP data from World Bank."""
    logger.info(
        f"Extracting GDP for "
        f"{len(country_codes)} countries")
    records = []
    for code, name in country_codes.items():
        try:
            url = (
                f"https://api.worldbank.org/v2/"
                f"country/{code}/indicator/"
                f"NY.GDP.MKTP.CD"
                f"?format=json&mrv=3")
            data = fetch_api(
                url, f"GDP:{name}")
            if data and len(data) > 1:
                for item in data[1]:
                    if item.get('value'):
                        records.append({
                            "country_code": code,
                            "country_name": name,
                            "year": int(
                                item['date']),
                            "gdp_usd": item[
                                'value'],
                            "gdp_billions": round(
                                item['value']
                                / 1e9, 2),
                            "fetched_at": datetime.now(
                                ).strftime(
                                "%Y-%m-%d %H:%M:%S")
                        })
        except APIError as e:
            logger.warning(
                f"Skipping GDP {name}: {e}")
    return records

def extract_weather_data(cities):
    """Fetch current weather for cities."""
    logger.info(
        f"Extracting weather for "
        f"{len(cities)} cities")
    records = []
    for city in cities:
        try:
            url = (
                f"https://api.open-meteo.com/"
                f"v1/forecast"
                f"?latitude={city['lat']}"
                f"&longitude={city['lon']}"
                f"&current=temperature_2m,"
                f"relative_humidity_2m,"
                f"precipitation,"
                f"wind_speed_10m,"
                f"weather_code"
                f"&timezone=Africa%2FAccra")
            data = fetch_api(
                url, f"Weather:{city['name']}")
            current = data['current']
            wcode = current['weather_code']
            records.append({
                "city": city['name'],
                "country": city['country'],
                "latitude": city['lat'],
                "longitude": city['lon'],
                "temperature_c": current[
                    'temperature_2m'],
                "humidity_pct": current[
                    'relative_humidity_2m'],
                "precipitation_mm": current[
                    'precipitation'],
                "wind_speed_kmh": current[
                    'wind_speed_10m'],
                "weather_code": wcode,
                "is_raining": wcode in
                    list(range(51, 68)) +
                    list(range(80, 83)),
                "fetched_at": datetime.now(
                    ).strftime(
                    "%Y-%m-%d %H:%M:%S")
            })
        except APIError as e:
            logger.warning(
                f"Skipping weather "
                f"{city['name']}: {e}")
    return records

# ─────────────────────────────────────────────
# TRANSFORM FUNCTIONS
# ─────────────────────────────────────────────
def transform_and_merge(
        countries, gdp_records, weather):
    """Merge all datasets together."""
    logger.info("Transforming and merging data")

    countries_df = pd.DataFrame(countries)
    gdp_df = pd.DataFrame(gdp_records)
    weather_df = pd.DataFrame(weather)

    # Get latest GDP per country
    latest_gdp = gdp_df.sort_values(
        'year', ascending=False
    ).groupby('country_name').first(
    ).reset_index()[[
        'country_name',
        'year', 'gdp_billions']]
    latest_gdp.columns = [
        'name', 'gdp_year', 'gdp_billions']

    # Merge countries with GDP
    merged = countries_df.merge(
        latest_gdp, on='name', how='left')

    # Add GDP per capita
    merged['gdp_per_capita_usd'] = (
        merged['gdp_billions'] * 1e9 /
        merged['population']
    ).round(2)

    # Add population density
    merged['pop_density_per_km2'] = (
        merged['population'] /
        merged['area_km2']
    ).round(2)

    # Weather summary per country
    weather_summary = weather_df.groupby(
        'country').agg(
        cities_monitored=('city', 'count'),
        avg_temperature_c=(
            'temperature_c', 'mean'),
        avg_humidity_pct=(
            'humidity_pct', 'mean'),
        cities_raining=(
            'is_raining', 'sum')
    ).round(2).reset_index()
    weather_summary.columns = [
        'name', 'cities_monitored',
        'avg_temp_c', 'avg_humidity_pct',
        'cities_raining']

    # Final merge
    final = merged.merge(
        weather_summary,
        on='name', how='left')

    logger.info(
        f"Merged dataset: "
        f"{final.shape[0]} countries, "
        f"{final.shape[1]} columns")
    return final, weather_df, gdp_df

# ─────────────────────────────────────────────
# LOAD FUNCTIONS
# ─────────────────────────────────────────────
def load_outputs(
        final_df, weather_df,
        gdp_df, insights):
    """Save all pipeline outputs."""
    logger.info("Saving pipeline outputs")

    # Save main report
    main_csv = (
        PROCESSED_DIR /
        'ghana_intelligence_report.csv')
    final_df.to_csv(main_csv, index=False)

    # Save weather data
    weather_csv = (
        PROCESSED_DIR / 'live_weather.csv')
    weather_df.to_csv(
        weather_csv, index=False)

    # Save GDP data
    gdp_csv = (
        PROCESSED_DIR / 'gdp_data.csv')
    gdp_df.to_csv(gdp_csv, index=False)

    # Save JSON report
    report_path = (
        PROCESSED_DIR /
        'pipeline_report.json')
    with open(report_path, 'w',
              encoding='utf-8') as f:
        json.dump(insights, f,
                  indent=4, default=str)

    logger.info("All outputs saved!")
    return [
        main_csv, weather_csv,
        gdp_csv, report_path]

# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def run_pipeline():
    start_time = datetime.now()

    logger.info("=" * 50)
    logger.info(f"  {PIPELINE_NAME}")
    logger.info(f"  Version: {PIPELINE_VERSION}")
    logger.info(f"  Author:  {AUTHOR}")
    logger.info("=" * 50)

    # CONFIG
    west_africa_countries = [
        "ghana", "nigeria", "senegal",
        "ivory coast", "cameroon"]

    country_codes = {
        "GH": "Ghana",
        "NG": "Nigeria",
        "SN": "Senegal",
        "CI": "Ivory Coast",
        "CM": "Cameroon"
    }

    ghana_cities = [
        {"name": "Accra",
         "country": "Ghana",
         "lat": 5.556, "lon": -0.1969},
        {"name": "Kumasi",
         "country": "Ghana",
         "lat": 6.6885, "lon": -1.6244},
        {"name": "Tamale",
         "country": "Ghana",
         "lat": 9.4075, "lon": -0.8533},
        {"name": "Cape Coast",
         "country": "Ghana",
         "lat": 5.1315, "lon": -1.2795},
        {"name": "Takoradi",
         "country": "Ghana",
         "lat": 4.8845, "lon": -1.7554},
    ]

    stats = {"status": "RUNNING", "errors": []}

    try:
        # EXTRACT
        logger.info("▶ Step 1: Extract")
        countries = extract_country_data(
            west_africa_countries)
        gdp_records = extract_gdp_data(
            country_codes)
        weather = extract_weather_data(
            ghana_cities)
        logger.info(
            f"✅ Extract: {len(countries)} "
            f"countries, {len(gdp_records)} "
            f"GDP records, {len(weather)} cities")

        # TRANSFORM
        logger.info("▶ Step 2: Transform")
        final_df, weather_df, gdp_df = \
            transform_and_merge(
                countries, gdp_records, weather)
        logger.info("✅ Transform complete")

        # BUILD INSIGHTS
        logger.info("▶ Step 3: Build insights")
        ghana_row = final_df[
            final_df['name'] == 'Ghana'].iloc[0]

        insights = {
            "pipeline_name": PIPELINE_NAME,
            "version": PIPELINE_VERSION,
            "author": AUTHOR,
            "run_timestamp": datetime.now(
                ).strftime("%Y-%m-%d %H:%M:%S"),
            "ghana_profile": {
                "population": int(
                    ghana_row['population']),
                "capital": ghana_row['capital'],
                "currency": ghana_row[
                    'currency_code'],
                "gdp_billions_usd": float(
                    ghana_row['gdp_billions']),
                "gdp_per_capita_usd": float(
                    ghana_row['gdp_per_capita_usd'
                    ]),
                "gdp_year": int(
                    ghana_row['gdp_year']),
            },
            "ghana_weather": {
                "cities_monitored": int(
                    len(weather)),
                "avg_temperature_c": round(float(
                    weather_df[
                        'temperature_c'].mean()),
                    1),
                "avg_humidity_pct": round(float(
                    weather_df[
                        'humidity_pct'].mean()),
                    1),
                "cities_raining": int(
                    weather_df[
                        'is_raining'].sum()),
            },
            "west_africa_comparison": {
                c['name']: {
                    "population": c['population'],
                    "gdp_billions": final_df[
                        final_df['name'] ==
                        c['name']
                    ]['gdp_billions'].values[0]
                    if len(final_df[
                        final_df['name'] ==
                        c['name']]) > 0
                    else None
                }
                for c in countries
            }
        }
        logger.info("✅ Insights built")

        # LOAD
        logger.info("▶ Step 4: Load")
        output_files = load_outputs(
            final_df, weather_df,
            gdp_df, insights)
        logger.info("✅ Load complete")

        stats["status"] = "SUCCESS"

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        stats["status"] = "FAILED"
        stats["errors"].append(str(e))
        insights = {}

    finally:
        duration = (
            datetime.now() - start_time
        ).total_seconds()
        logger.info(
            f"Pipeline {stats['status']} "
            f"in {duration:.2f}s")

    return stats, insights

# ─────────────────────────────────────────────
# RUN & DISPLAY
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n🚀 Starting {PIPELINE_NAME}...")
    print("=" * 55)

    results, insights = run_pipeline()

    print("\n" + "=" * 55)
    print("   PIPELINE RESULTS")
    print("=" * 55)
    print(f"\n   Status: {results['status']}")

    if insights:
        gh = insights['ghana_profile']
        wx = insights['ghana_weather']

        print(f"\n   🇬🇭 GHANA PROFILE:")
        print(f"   Capital:        {gh['capital']}")
        print(f"   Population:     "
              f"{gh['population']:,}")
        print(f"   Currency:       {gh['currency']}")
        print(f"   GDP ({gh['gdp_year']}):     "
              f"${gh['gdp_billions_usd']:.2f}B")
        print(f"   GDP per Capita: "
              f"${gh['gdp_per_capita_usd']:,.2f}")

        print(f"\n   🌤️  GHANA LIVE WEATHER:")
        print(f"   Cities monitored:  "
              f"{wx['cities_monitored']}")
        print(f"   Avg temperature:   "
              f"{wx['avg_temperature_c']}°C")
        print(f"   Avg humidity:      "
              f"{wx['avg_humidity_pct']}%")
        print(f"   Cities raining:    "
              f"{wx['cities_raining']}")

        print(f"\n   🌍 WEST AFRICA COMPARISON:")
        print(f"   {'Country':<15} "
              f"{'Population':>12} "
              f"{'GDP (B)':>10}")
        print("   " + "-" * 42)
        for name, data in insights[
                'west_africa_comparison'].items():
            gdp = data['gdp_billions']
            gdp_str = (f"${gdp:.1f}B"
                       if gdp else "N/A")
            print(f"   {name:<15} "
                  f"{data['population']:>12,} "
                  f"{gdp_str:>10}")

    print(f"\n   📁 OUTPUT FILES:")
    for f in [
        PROCESSED_DIR / 'ghana_intelligence_report.csv',
        PROCESSED_DIR / 'live_weather.csv',
        PROCESSED_DIR / 'gdp_data.csv',
        PROCESSED_DIR / 'pipeline_report.json',
        LOGS_DIR / 'capstone_pipeline.log'
    ]:
        if Path(f).exists():
            size = Path(f).stat().st_size
            print(f"   📄 {Path(f).name:<40} "
                  f"{size:,} bytes")

    print(f"\n✅ Week 3 REST API Capstone Complete!")