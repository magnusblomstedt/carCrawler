import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
import schedule
import os
import csv
import logging
from supabase import create_client
from supabase_conf import SUPABASE_CONFIG

#test

"""
Influx local
http://localhost:8086/

Grafana local
http://localhost:3000

Influx - pi
http://192.168.1.247:8086
http://176.10.128.179:8086

Grafana - pi
http://192.168.1.247:3000
http://176.10.128.179:3000


"""

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'car_crawler.log')),
        logging.StreamHandler()
    ]
)

# Supabase setup
supabase = create_client(SUPABASE_CONFIG["url"], SUPABASE_CONFIG["key"])

# ---------- JSON extractor using balanced brackets ----------
def extract_store_objects(script_content):
    start_idx = script_content.find('{')
    if start_idx == -1:
        return None

    depth = 0
    for i in range(start_idx, len(script_content)):
        if script_content[i] == '{':
            depth += 1
        elif script_content[i] == '}':
            depth -= 1
            if depth == 0:
                json_str = script_content[start_idx:i+1]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logging.warning(f"⚠️ JSON parse error: {e}")
                    return None
    logging.warning("⚠️ Bracket mismatch or invalid JSON structure.")
    return None

def clean_model_name(model_name):
    """Clean up model name by removing horsepower, kWh details, and trailing commas."""
    if not model_name:
        return ""
    
    # Remove horsepower details in parentheses (e.g., "(228hk)")
    cleaned = re.sub(r'\s*\(\d+hk\)', '', model_name)
    
    # Remove standalone horsepower details (e.g., "228hk")
    cleaned = re.sub(r'\s+\d+hk\b', '', cleaned)
    
    # Remove kWh details (e.g., "80,0 kWh", "95 kWh")
    cleaned = re.sub(r'\s+\d+(?:[,.]\d+)?\s*kWh\b', '', cleaned, flags=re.IGNORECASE)
    
    # Remove all commas and whitespace
    cleaned = re.sub(r',\s*', ' ', cleaned.strip())
    
    return cleaned

def clean_brand_name(brand_name):
    """Clean up brand name by removing commas."""
    if not brand_name:
        return ""
    
    # Remove all commas and whitespace
    cleaned = re.sub(r',\s*', ' ', brand_name.strip())
    
    return cleaned

def get_tesla_battery_capacity(model_name_short):
    """Get battery capacity for Tesla models based on model name."""
    tesla_capacities = {
        "Model 3 Long Range Dual Motor AWD": 82.0,
        "Model 3 Performance AWD": 82.0,
        "Model 3 Standard Range RWD": 55.0,
        "Model S 100D": 100.0,
        "Model S 60": 60.0,
        "Model S 75D": 75.0,
        "Model S 85D": 85.0,
        "Model S 90D": 90.0,
        "Model S P100D": 100.0,
        "Model S P85": 85.0,
        "Model X LR AWD": 100.0,
        "Model Y Long Range Dual Motor AWD": 75.0,
        "Model Y Performance Dual Motor AWD": 75.0
    }
    
    # Remove "Tesla" prefix if present and clean the string
    cleaned_name = model_name_short.replace("Tesla ", "").strip()
    return tesla_capacities.get(cleaned_name)

def extract_engine_power_from_model_name(model_name):
    """Extract engine power (in hk) from model name if present."""
    if not model_name:
        return None
    
    # Match patterns like "(254hk)" or "254hk"
    match = re.search(r'(?:\((\d+)hk\)|(\d+)hk)', model_name)
    if match:
        # Get the first non-None group (either from parentheses or standalone)
        hk_value = next((g for g in match.groups() if g is not None), None)
        if hk_value:
            try:
                return int(hk_value)
            except (ValueError, TypeError):
                return None
    return None

def extract_fields(store):
    data = {}
    for key, item in store.get('objectView', {}).get('storeObjects', {}).items():
        if not item:
            continue

        # Safely get nested objects with defaults
        process_object = item.get("processObject", {}) or {}
        base_obj = process_object.get("baseObject", {}) or {}
        location_info = process_object.get("locationInfo", {}) or {}
        facility = location_info.get("facility", {}) or {}
        properties = process_object.get("properties", {}) or {}
        fuels = properties.get("fuels", []) or []
        active_auction = item.get("activeAuction", {}) or {}
        winning_bid = item.get("winningBid", {}) or {}
        highest_bid = (active_auction.get("highestBid", {}) or {}).get("amount")

        # Extract auction ID safely
        auction_url = item.get("auctionUrl", "") or ""
        auction_id = None
        if auction_url:
            match = re.search(r'-(\d+)$', auction_url)
            auction_id = match.group(1) if match else None

        # Get model name first
        model_name = base_obj.get("modelName", "") or ""

        # Get the first fuel code if available
        fuel_code = None
        if fuels and len(fuels) > 0:
            fuel_code = fuels[0].get("fuelCode")
            # Get engine power HP from authority register information
            authority_info = process_object.get("baseObject", {}).get("authorityRegisterInformation", {}) or {}
            tech_spec = authority_info.get("generalTechSpecification", {}) or {}
            tech_fuels = tech_spec.get("fuels", []) or []
            if tech_fuels and len(tech_fuels) > 0:
                engine_power_hp = tech_fuels[0].get("enginePowerHp")
                engine_power = tech_fuels[0].get("enginePower")
            else:
                # Fallback to extracting from model name
                engine_power_hp = extract_engine_power_from_model_name(model_name)
                engine_power = None

        # Extract battery capacity from modelName
        battery_capacity = None
        if model_name:
            # Match patterns like:
            # "41 kWh", "77 kWh", "80,0 kWh", "40.0 kWh", "39kWh", "50 kWh"
            capacity_match = re.search(r'(\d+(?:[,.]\d+)?)\s*kWh', model_name, re.IGNORECASE)
            if capacity_match:
                try:
                    # Convert comma to dot for decimal point and convert to float
                    battery_capacity = float(capacity_match.group(1).replace(',', '.'))
                    logging.info(f"✅ Extracted battery capacity: {battery_capacity} kWh from {model_name}")
                except (ValueError, TypeError):
                    logging.warning(f"⚠️ Failed to convert battery capacity from: {model_name}")
                    battery_capacity = None
            else:
                logging.debug(f"No battery capacity found in model name: {model_name}")

        # Get brand
        brand = properties.get("brand") or None
        if brand:
            brand = clean_brand_name(brand)

        # Create cleaned model name with brand prefix
        model_name_short = clean_model_name(model_name)
        model_name_with_brand = f"{brand} {model_name_short}" if brand and model_name_short else model_name_short

        # If no battery capacity found and it's a Tesla, try to get it from the model name
        if battery_capacity is None and brand == "Tesla" and model_name_with_brand:
            battery_capacity = get_tesla_battery_capacity(model_name_with_brand)
            if battery_capacity:
                logging.info(f"✅ Set Tesla battery capacity: {battery_capacity} kWh for {model_name_with_brand}")

        # Get soldFor amount
        sold_for = item.get("soldFor") or None
        try:
            sold_for = float(sold_for) if sold_for else None
        except (ValueError, TypeError):
            sold_for = None

        # Create view fields based on conditions
        model_name_search = model_name_with_brand if sold_for and sold_for > 0 else None
        model_name_electric_search = model_name_with_brand if fuel_code == "Electric" and sold_for and sold_for > 0 else None
        model_name_fossil_search = model_name_with_brand if fuel_code != "Electric" and sold_for and sold_for > 0 else None

        # Create electric/fossil specific brands
        brand_electric_search = brand if fuel_code == "Electric" and sold_for and sold_for > 0 else None
        brand_fossil_search = brand if fuel_code != "Electric" and sold_for and sold_for > 0 else None

        # Safely get all fields with defaults
        data = {
            "auctionId": auction_id,
            "closedAt": item.get("closedAt") or None,
            "publishedAt": item.get("publishedAt") or None,
            "soldFor": sold_for,
            "sellMethod": item.get("sellMethod") or None,
            "slug": item.get("slug", "") or "",
            "auctionUrl": auction_url,
            "buyNowAmount": item.get("buyNowAmount") or None,
            "buyNowAvailable": bool(item.get("buyNowAvailable")),
            "preliminaryPrice": item.get("preliminaryPrice") or None,
            "isSoldByBuyNow": bool(item.get("isSoldByBuyNow")),
            "winningBid": winning_bid.get("amount") or None,
            "reservationPriceReached": bool(active_auction.get("reservationPriceReached")),
            "highestBid": highest_bid or None,
            "electricType": properties.get("electricType") or None,
            "odometerReading": properties.get("odometerReading") or None,
            "body": properties.get("body") or None,
            "brand": brand,
            "brandElectricSearch": brand_electric_search,
            "brandFossilSearch": brand_fossil_search,
            "familyName": properties.get("familyName") or None,
            "registrationPlate": base_obj.get("registrationPlate") or None,
            "modelName": model_name,
            "modelNamePresentation": model_name_with_brand,
            "modelNameSearch": model_name_search,
            "modelNameElectricSearch": model_name_electric_search,
            "modelNameFossilSearch": model_name_fossil_search,
            "year": base_obj.get("year") or None,
            "facilityPostCode": facility.get("postCode") or None,
            "facilityCity": facility.get("city") or None,
            "fuelCode": fuel_code,
            "batteryCapacity": battery_capacity,
            "rangeCityWltpDrive": fuels[0].get("rangeCityWltpDrive") if fuels else None,
            "rangeWltpDrive": fuels[0].get("rangeWltpDrive") if fuels else None,
            "enginePowerHp": engine_power_hp,
            "enginePower": engine_power,
            "gearbox": properties.get("gearbox") or None,
        }

        if auction_id:
            logging.info(f"✅ Extracted auction: {auction_id}")
        else:
            logging.warning("⚠️ Failed to extract auction ID")
        break
    return data

def write_to_supabase(data):
    if not data or not data.get("auctionId"):
        logging.warning("⚠️ No data or auction ID provided to write_to_supabase")
        return

    logging.info(f"📝 Preparing to write data for auction ID: {data['auctionId']}")

    try:
        # Convert datetime strings to proper format if they're not already
        for field in ['closedAt', 'publishedAt']:
            if isinstance(data.get(field), str):
                # The date is already a string, just replace Z with +00:00 for proper timezone
                data[field] = data[field].replace('Z', '+00:00') if data.get(field) else None
            elif data.get(field):
                # If it's a datetime object, convert to ISO format string
                data[field] = data[field].isoformat()

        # Prepare the data for Supabase
        supabase_data = {
            'auction_id': data['auctionId'],
            'closed_at': data.get('closedAt'),
            'published_at': data.get('publishedAt'),
            'sold_for': data.get('soldFor'),
            'sell_method': data.get('sellMethod'),
            'slug': data.get('slug'),
            'auction_url': data.get('auctionUrl'),
            'buy_now_amount': data.get('buyNowAmount'),
            'buy_now_available': data.get('buyNowAvailable'),
            'preliminary_price': data.get('preliminaryPrice'),
            'is_sold_by_buy_now': data.get('isSoldByBuyNow'),
            'winning_bid': data.get('winningBid'),
            'reservation_price_reached': data.get('reservationPriceReached'),
            'highest_bid': data.get('highestBid'),
            'electric_type': data.get('electricType'),
            'odometer_reading': data.get('odometerReading'),
            'body': data.get('body'),
            'brand': data.get('brand'),
            'brand_electric_search': data.get('brandElectricSearch'),
            'brand_fossil_search': data.get('brandFossilSearch'),
            'family_name': data.get('familyName'),
            'registration_plate': data.get('registrationPlate'),
            'model_name': data.get('modelName'),
            'model_name_presentation': data.get('modelNamePresentation'),
            'model_name_search': data.get('modelNameSearch'),
            'model_name_electric_search': data.get('modelNameElectricSearch'),
            'model_name_fossil_search': data.get('modelNameFossilSearch'),
            'year': data.get('year'),
            'facility_post_code': data.get('facilityPostCode'),
            'facility_city': data.get('facilityCity'),
            'fuel_code': data.get('fuelCode'),
            'battery_capacity': data.get('batteryCapacity'),
            'range_city_wltp_drive': data.get('rangeCityWltpDrive'),
            'range_wltp_drive': data.get('rangeWltpDrive'),
            'engine_power_hp': data.get('enginePowerHp'),
            'engine_power': data.get('enginePower'),
            'gearbox': data.get('gearbox'),
            'main_image_url': data.get('mainImageUrl'),
        }

        logging.info(f"🔍 Checking for existing record with auction_id: {data['auctionId']}")

        # Check if record exists
        existing_record = supabase.table('car_auctions') \
            .select("*") \
            .eq('auction_id', data['auctionId']) \
            .execute()

        if existing_record.data:
            # Update existing record
            supabase.table('car_auctions') \
                .update(supabase_data) \
                .eq('auction_id', data['auctionId']) \
                .execute()
            logging.info(f"🔄 Updated record for auction {data['auctionId']}")
        else:
            # Insert new record
            supabase.table('car_auctions') \
                .insert(supabase_data) \
                .execute()
            logging.info(f"📝 Created new record for auction {data['auctionId']}")

    except Exception as e:
        logging.error(f"❌ Error writing to Supabase: {str(e)}")
        logging.error(f"❌ Error details: {type(e).__name__}")
        import traceback
        logging.error(f"❌ Full traceback: {traceback.format_exc()}")

# Crawl KVD auctions and store data in Supabase
def crawl_kvd(limit=None):
    logging.info(f"🚗 Starting crawl at {datetime.now()}...")
    if limit:
        logging.info(f"⚠️ Limiting to first {limit} URLs")
        records = []

    url = "https://www.kvd.se/stangda-auktioner"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    links = soup.select('a[href^="/auktioner/"]')
    detail_urls = {"https://www.kvd.se" + a['href'] for a in links}
    
    if limit:
        detail_urls = list(detail_urls)[:limit]
        logging.info(f"🔍 Processing {len(detail_urls)} URLs")

    for detail_url in detail_urls:
        try:
            logging.info(f"🔍 Fetching {detail_url}")
            response = requests.get(detail_url, allow_redirects=False)
            
            if response.status_code in (301, 302, 303, 307, 308):
                logging.warning(f"⚠️ Skipping {detail_url} - URL redirects to {response.headers.get('Location', 'unknown')}")
                continue
                
            page = requests.get(detail_url)
            detail_soup = BeautifulSoup(page.text, 'html.parser')
            scripts = detail_soup.find_all('script')

            # Extract main image URL from meta tag
            main_image_url = None
            meta_image = detail_soup.find('meta', property='og:image')
            if meta_image and meta_image.get('content'):
                main_image_url = meta_image['content']

            store_data = None
            for script in scripts:
                if script.string and "storeObjects" in script.string:
                    store_data = extract_store_objects(script.string)
                    if store_data:
                        break

            if store_data:
                record = extract_fields(store_data)
                record['mainImageUrl'] = main_image_url
                write_to_supabase(record)
                if limit:
                    records.append(record)
            else:
                logging.warning(f"⚠️ Failed to extract JSON from {detail_url}")
                
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Network error for {detail_url}: {str(e)}")
            continue
        except Exception as e:
            logging.error(f"❌ Unexpected error for {detail_url}: {str(e)}")
            continue

    if limit and records:
        csv_filename = os.path.join(SCRIPT_DIR, f'cars_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        logging.info(f"📝 Writing {len(records)} records to {csv_filename}")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = records[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        logging.info(f"✅ CSV file created: {csv_filename}")

# Schedule the crawler daily at 5 AM
schedule.every().day.at("05:00").do(crawl_kvd)

if __name__ == '__main__':
    # Set to None for full crawl, or a number to limit URLs
    limit = 10000  # Set to None for full crawl
    
    # Only run immediately if we're not using the scheduler
    if limit is not None:
        crawl_kvd(limit=limit)  # Run once immediately with limit
    else:
        logging.info("🕒 Running in scheduled mode - waiting for 05:00")
        while True:
            schedule.run_pending()
            time.sleep(30)