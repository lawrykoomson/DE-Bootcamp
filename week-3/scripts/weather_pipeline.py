# ============================================
# WEEK 3 — MODULE 3: REST APIs with Python
# Script 2: Weather API + Pandas Pipeline
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
# SETUP
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
os.makedirs("../data/raw", exist_ok=True)
os.makedirs("../data/processed", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/weather_pipeline.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

print("=" * 55)
print("   WEATHER API + PANDAS PIPELINE")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: DEFINE GHANA CITIES
# ─────────────────────────────────────────────
print("\n📌 1. DEFINING GHANA CITIES")

ghana_cities = [
    {"name": "Accra",
     "lat": 5.5560, "lon": -0.1969},
    {"name": "Kumasi",
     "lat": 6.6885, "lon": -1.6244},
    {"name": "Tamale",
     "lat": 9.4075, "lon": -0.8533},
    {"name": "Takoradi",
     "lat": 4.8845, "lon": -1.7554},
    {"name": "Cape Coast",
     "lat": 5.1315, "lon": -1.2795},
    {"name": "Sunyani",
     "lat": 7.3349, "lon": -2.3123},
    {"name": "Ho",
     "lat": 6.6011, "lon": 0.4717},
    {"name": "Koforidua",
     "lat": 6.0940, "lon": -0.2573},
]

print(f"   Cities to fetch: {len(ghana_cities)}")
for city in ghana_cities:
    print(f"   📍 {city['name']:<15} "
          f"lat={city['lat']}, "
          f"lon={city['lon']}")

# ─────────────────────────────────────────────
# STEP 2: FETCH WEATHER FOR EACH CITY
# ─────────────────────────────────────────────
print("\n📌 2. FETCHING WEATHER DATA")
logger.info(
    f"Fetching weather for "
    f"{len(ghana_cities)} cities")

weather_records = []

for city in ghana_cities:
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={city['lat']}"
        f"&longitude={city['lon']}"
        f"&current=temperature_2m,"
        f"relative_humidity_2m,"
        f"precipitation,"
        f"wind_speed_10m,"
        f"weather_code"
        f"&timezone=Africa%2FAccra"
    )

    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            current = data['current']

            # Decode weather code
            wcode = current['weather_code']
            if wcode == 0:
                condition = "Clear sky ☀️"
            elif wcode in [1, 2, 3]:
                condition = "Cloudy ⛅"
            elif wcode in range(51, 68):
                condition = "Rainy 🌧️"
            elif wcode in range(80, 83):
                condition = "Showers 🌦️"
            elif wcode in range(95, 100):
                condition = "Thunderstorm ⛈️"
            else:
                condition = f"Code {wcode}"

            record = {
                "city": city['name'],
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
                "weather_condition": condition,
                "weather_code": wcode,
                "fetched_at": datetime.now(
                    ).strftime("%Y-%m-%d %H:%M:%S")
            }
            weather_records.append(record)
            logger.info(
                f"✅ {city['name']}: "
                f"{record['temperature_c']}°C "
                f"{condition}")
            print(
                f"   ✅ {city['name']:<12} "
                f"{record['temperature_c']:>5.1f}°C  "
                f"{condition}")
        else:
            logger.warning(
                f"Failed {city['name']}: "
                f"{response.status_code}")
            print(
                f"   ❌ {city['name']} failed: "
                f"{response.status_code}")

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout fetching {city['name']}")
        print(f"   ⏰ {city['name']} timed out")
    except Exception as e:
        logger.error(
            f"Error fetching {city['name']}: {e}")
        print(f"   ❌ {city['name']} error: {e}")

# ─────────────────────────────────────────────
# STEP 3: CONVERT TO PANDAS DATAFRAME
# ─────────────────────────────────────────────
print("\n📌 3. CONVERTING TO PANDAS DATAFRAME")

df = pd.DataFrame(weather_records)
print(f"   Shape: {df.shape}")
print(f"\n   Weather Summary:")
print(df[[
    'city', 'temperature_c',
    'humidity_pct', 'wind_speed_kmh',
    'weather_condition']].to_string(index=False))

# ─────────────────────────────────────────────
# STEP 4: ANALYSE WEATHER DATA
# ─────────────────────────────────────────────
print("\n📌 4. WEATHER ANALYSIS")

print(f"\n   🌡️  Temperature Stats:")
print(f"   Hottest city:  "
      f"{df.loc[df['temperature_c'].idxmax(), 'city']} "
      f"({df['temperature_c'].max():.1f}°C)")
print(f"   Coolest city:  "
      f"{df.loc[df['temperature_c'].idxmin(), 'city']} "
      f"({df['temperature_c'].min():.1f}°C)")
print(f"   Average temp:  "
      f"{df['temperature_c'].mean():.1f}°C")

print(f"\n   💧 Humidity Stats:")
print(f"   Most humid:    "
      f"{df.loc[df['humidity_pct'].idxmax(), 'city']} "
      f"({df['humidity_pct'].max():.0f}%)")
print(f"   Least humid:   "
      f"{df.loc[df['humidity_pct'].idxmin(), 'city']} "
      f"({df['humidity_pct'].min():.0f}%)")
print(f"   Average:       "
      f"{df['humidity_pct'].mean():.0f}%")

print(f"\n   💨 Wind Stats:")
print(f"   Windiest city: "
      f"{df.loc[df['wind_speed_kmh'].idxmax(), 'city']} "
      f"({df['wind_speed_kmh'].max():.1f} km/h)")
print(f"   Average wind:  "
      f"{df['wind_speed_kmh'].mean():.1f} km/h")

# ─────────────────────────────────────────────
# STEP 5: LOAD COUNTRIES DATA AND MERGE
# ─────────────────────────────────────────────
print("\n📌 5. MERGING WITH COUNTRIES DATA")

countries_file = Path(
    "../data/raw/west_africa_countries.json")

if countries_file.exists():
    with open(countries_file, 'r') as f:
        countries_json = json.load(f)

    countries_df = pd.DataFrame(
        countries_json['countries'])

    # Add Ghana country data to weather
    ghana_info = countries_df[
        countries_df['name'] == 'Ghana'].iloc[0]

    df['country'] = 'Ghana'
    df['country_population'] = (
        ghana_info['population'])
    df['country_currency'] = (
        ghana_info['currency'])
    df['country_region'] = (
        ghana_info['region'])

    print(f"   Merged weather with country data!")
    print(f"   Country:    Ghana")
    print(f"   Population: "
          f"{ghana_info['population']:,}")
    print(f"   Currency:   "
          f"{ghana_info['currency']}")
else:
    print("   ⚠️  Countries file not found — "
          "run api_basics.py first!")

# ─────────────────────────────────────────────
# STEP 6: SAVE RESULTS
# ─────────────────────────────────────────────
print("\n📌 6. SAVING RESULTS")

# Save raw weather JSON
raw_json = "../data/raw/ghana_weather.json"
with open(raw_json, 'w',
          encoding='utf-8') as f:
    json.dump({
        "source": "Open-Meteo API",
        "country": "Ghana",
        "fetched_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"),
        "total_cities": len(weather_records),
        "cities": weather_records
    }, f, indent=4)

# Save processed CSV
processed_csv = (
    "../data/processed/ghana_weather.csv")
df.to_csv(processed_csv, index=False)

print(f"   ✅ JSON saved: {raw_json}")
print(f"   ✅ CSV saved:  {processed_csv}")
print(f"   Records:      {len(df)}")

logger.info(
    f"Pipeline complete: "
    f"{len(weather_records)} cities processed")

print(f"\n{'='*55}")
print("   WEATHER PIPELINE SUMMARY")
print(f"{'='*55}")
print("   Free API — no key needed!")
print("   8 Ghana cities fetched in real time")
print("   JSON → Pandas DataFrame → Analysis")
print("   Merged with country data")
print("   Saved JSON + CSV outputs")
print(f"\n✅ Script 2 Complete!")
logger.info("Weather pipeline complete")