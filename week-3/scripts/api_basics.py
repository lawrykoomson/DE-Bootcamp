# ============================================
# WEEK 3 — MODULE 3: REST APIs with Python
# Script 1: API Basics
# GetSkills Network DE Bootcamp
# ============================================

import requests
import json
import logging
import os
from datetime import datetime

# ─────────────────────────────────────────────
# SETUP LOGGING
# ─────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            '../logs/module3.log',
            encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

print("=" * 55)
print("   MODULE 3 — REST APIs WITH PYTHON")
print("=" * 55)

# ─────────────────────────────────────────────
# STEP 1: UNDERSTANDING HTTP STATUS CODES
# ─────────────────────────────────────────────
print("\n📌 1. HTTP STATUS CODES")
print("   200 → Success ✅")
print("   400 → Bad request ❌")
print("   401 → Unauthorised 🔒")
print("   404 → Not found 🔍")
print("   500 → Server error 💥")

# ─────────────────────────────────────────────
# STEP 2: FIRST API CALL — REST Countries
# ─────────────────────────────────────────────
print("\n📌 2. FIRST API CALL — REST Countries")

url = "https://restcountries.com/v3.1/name/ghana"

logger.info(f"Calling API: {url}")

try:
    response = requests.get(url, timeout=10)
    logger.info(
        f"Response status: {response.status_code}")

    print(f"\n   URL:         {url}")
    print(f"   Status Code: {response.status_code}")
    print(f"   Status:      "
          f"{'✅ Success' if response.status_code == 200 else '❌ Failed'}")

    if response.status_code == 200:
        data = response.json()
        ghana = data[0]

        print(f"\n   🇬🇭 GHANA DATA FROM API:")
        print(f"   Official Name: "
              f"{ghana['name']['official']}")
        print(f"   Capital:       "
              f"{ghana['capital'][0]}")
        print(f"   Population:    "
              f"{ghana['population']:,}")
        print(f"   Region:        "
              f"{ghana['region']}")
        print(f"   Sub-region:    "
              f"{ghana['subregion']}")
        print(f"   Area (km²):    "
              f"{ghana['area']:,}")
        print(f"   Currency:      "
              f"{list(ghana['currencies'].keys())[0]} — "
              f"{list(ghana['currencies'].values())[0]['name']}")
        print(f"   Languages:     "
              f"{', '.join(ghana['languages'].values())}")
        print(f"   Timezones:     "
              f"{ghana['timezones']}")

        logger.info(
            f"Ghana data retrieved: "
            f"population={ghana['population']:,}")

except requests.exceptions.Timeout:
    logger.error("Request timed out!")
    print("   ❌ Request timed out!")
except requests.exceptions.ConnectionError:
    logger.error("Connection failed!")
    print("   ❌ No internet connection!")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    print(f"   ❌ Error: {e}")

# ─────────────────────────────────────────────
# STEP 3: GET MULTIPLE WEST AFRICAN COUNTRIES
# ─────────────────────────────────────────────
print("\n📌 3. WEST AFRICAN COUNTRIES DATA")

west_africa = [
    "ghana", "nigeria", "senegal",
    "ivory coast", "cameroon"]

countries_data = []

for country_name in west_africa:
    url = f"https://restcountries.com/v3.1/name/{country_name}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            country = response.json()[0]
            record = {
                "name": country['name']['common'],
                "capital": country.get(
                    'capital', ['N/A'])[0],
                "population": country['population'],
                "area_km2": country.get('area', 0),
                "region": country['region'],
                "subregion": country.get(
                    'subregion', 'N/A'),
                "currency": list(
                    country['currencies'].keys()
                )[0] if country.get(
                    'currencies') else 'N/A',
                "fetched_at": datetime.now(
                    ).strftime("%Y-%m-%d %H:%M:%S")
            }
            countries_data.append(record)
            logger.info(
                f"Fetched: {record['name']} "
                f"pop={record['population']:,}")
        else:
            logger.warning(
                f"Failed to fetch {country_name}: "
                f"{response.status_code}")
    except Exception as e:
        logger.error(
            f"Error fetching {country_name}: {e}")

print(f"\n   {'Country':<15} {'Capital':<15} "
      f"{'Population':>12} {'Currency':<8}")
print("   " + "-" * 55)
for c in countries_data:
    print(f"   {c['name']:<15} "
          f"{c['capital']:<15} "
          f"{c['population']:>12,} "
          f"{c['currency']:<8}")

# ─────────────────────────────────────────────
# STEP 4: SAVE API DATA TO JSON
# ─────────────────────────────────────────────
print("\n📌 4. SAVING API DATA TO JSON")

os.makedirs("../data/raw", exist_ok=True)
output_file = "../data/raw/west_africa_countries.json"

with open(output_file, 'w',
          encoding='utf-8') as f:
    json.dump({
        "source": "REST Countries API",
        "fetched_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"),
        "total_countries": len(countries_data),
        "countries": countries_data
    }, f, indent=4)

print(f"   ✅ Saved to: {output_file}")
print(f"   Records:    {len(countries_data)}")
logger.info(
    f"Saved {len(countries_data)} country "
    f"records to {output_file}")

# ─────────────────────────────────────────────
# STEP 5: WORLD BANK API — Ghana GDP Data
# ─────────────────────────────────────────────
print("\n📌 5. WORLD BANK API — Ghana GDP")

wb_url = ("https://api.worldbank.org/v2/country/"
          "GH/indicator/NY.GDP.MKTP.CD"
          "?format=json&mrv=5")

logger.info(f"Calling World Bank API: {wb_url}")

try:
    response = requests.get(wb_url, timeout=10)
    if response.status_code == 200:
        wb_data = response.json()
        records = wb_data[1]

        print(f"\n   Ghana GDP (Last 5 years):")
        print(f"   {'Year':<8} {'GDP (USD)':>20}")
        print("   " + "-" * 30)

        gdp_records = []
        for record in records:
            if record.get('value'):
                year = record['date']
                gdp = record['value']
                gdp_billions = gdp / 1e9
                print(f"   {year:<8} "
                      f"${gdp_billions:>15.2f}B")
                gdp_records.append({
                    "year": year,
                    "gdp_usd": gdp,
                    "gdp_billions": round(
                        gdp_billions, 2)
                })

        # Save GDP data
        gdp_file = "../data/raw/ghana_gdp.json"
        with open(gdp_file, 'w',
                  encoding='utf-8') as f:
            json.dump({
                "source": "World Bank API",
                "country": "Ghana",
                "indicator": "GDP (current USD)",
                "fetched_at": datetime.now(
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                "data": gdp_records
            }, f, indent=4)
        print(f"\n   ✅ GDP data saved to: {gdp_file}")
        logger.info(
            f"Ghana GDP data saved: "
            f"{len(gdp_records)} years")
    else:
        print(f"   ❌ World Bank API failed: "
              f"{response.status_code}")
except Exception as e:
    logger.error(
        f"World Bank API error: {e}")
    print(f"   ❌ Error: {e}")

print(f"\n{'='*55}")
print("   MODULE 3 — SCRIPT 1 SUMMARY")
print(f"{'='*55}")
print("   requests.get(url)    → Make API call")
print("   response.status_code → Check success")
print("   response.json()      → Parse response")
print("   timeout=10           → Avoid hanging")
print("   try/except           → Handle errors")
print("   json.dump()          → Save to file")
print(f"\n✅ Script 1 Complete!")
logger.info("Module 3 Script 1 complete")