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
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_conf import INFLUXDB_CONFIG

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

# InfluxDB setup
client = InfluxDBClient(
    url=INFLUXDB_CONFIG["url"],
    token=INFLUXDB_CONFIG["token"],
    org=INFLUXDB_CONFIG["org"]
)
write_api = client.write_api(write_options=SYNCHRONOUS)
bucket = INFLUXDB_CONFIG["bucket"]

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
    
    # Remove trailing commas and whitespace
    cleaned = re.sub(r',\s*$', '', cleaned.strip())
    
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
        fuels = base_obj.get("fuels", []) or []
        active_auction = item.get("activeAuction", {}) or {}
        winning_bid = item.get("winningBid", {}) or {}
        highest_bid = (active_auction.get("highestBid", {}) or {}).get("amount")

        # Extract auction ID safely
        auction_url = item.get("auctionUrl", "") or ""
        auction_id = None
        if auction_url:
            match = re.search(r'-(\d+)$', auction_url)
            auction_id = match.group(1) if match else None

        # Get the first fuel code if available
        fuel_code = None
        if fuels and len(fuels) > 0:
            fuel_code = fuels[0].get("fuelCode")

        # Extract battery capacity from modelName
        model_name = base_obj.get("modelName", "") or ""
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

        # Calculate cost per kWh if both soldFor and batteryCapacity are available
        cost_per_kwh = None
        if sold_for is not None and battery_capacity is not None and battery_capacity > 0:
            try:
                cost_per_kwh = sold_for / battery_capacity
                logging.info(f"✅ Calculated cost per kWh: {cost_per_kwh:.2f} SEK/kWh")
            except (ValueError, TypeError, ZeroDivisionError):
                logging.warning("⚠️ Failed to calculate cost per kWh")

        # Create view fields based on conditions
        model_name_view = model_name_with_brand if sold_for and sold_for > 0 and battery_capacity and battery_capacity > 0 else None
        model_name_electric_view = model_name_with_brand if fuel_code == "Electric" and sold_for and sold_for > 0 and battery_capacity and battery_capacity > 0 else None
        model_name_fossil_view = model_name_with_brand if fuel_code != "Electric" and sold_for and sold_for > 0 and battery_capacity and battery_capacity > 0 else None

        # Create electric/fossil specific brands
        brand_electric = brand if fuel_code == "Electric" else None
        brand_fossil = brand if fuel_code != "Electric" else None

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
            "brandElectric": brand_electric,
            "brandFossil": brand_fossil,
            "familyName": properties.get("familyName") or None,
            "registrationPlate": base_obj.get("registrationPlate") or None,
            "modelName": model_name,
            "modelNameShort": model_name_with_brand,
            "modelNameView": model_name_view,
            "modelNameElectricView": model_name_electric_view,
            "modelNameFossilView": model_name_fossil_view,
            "year": base_obj.get("year") or None,
            "facilityPostCode": facility.get("postCode") or None,
            "facilityCity": facility.get("city") or None,
            "fuelCode": fuel_code,
            "batteryCapacity": battery_capacity,
            "costPerKwh": cost_per_kwh,
        }

        if auction_id:
            logging.info(f"✅ Extracted auction: {auction_id}")
        else:
            logging.warning("⚠️ Failed to extract auction ID")
        break
    return data

def write_to_influx(data):
    if not data or not data.get("auctionId"):
        return

    # Convert numeric fields to float, handling None values
    def to_float(value):
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    point = Point("car_auction") \
        .tag("auctionId", data["auctionId"]) \
        .tag("brand", data.get("brand")) \
        .tag("brandElectric", data.get("brandElectric")) \
        .tag("brandFossil", data.get("brandFossil")) \
        .tag("modelName", data.get("modelName")) \
        .tag("modelNameShort", data.get("modelNameShort")) \
        .tag("modelNameView", data.get("modelNameView")) \
        .tag("modelNameElectricView", data.get("modelNameElectricView")) \
        .tag("modelNameFossilView", data.get("modelNameFossilView")) \
        .tag("registrationPlate", data.get("registrationPlate")) \
        .tag("year", str(data.get("year"))) \
        .tag("fuelCode", data.get("fuelCode")) \
        .tag("sellMethod", str(data.get("sellMethod"))) \
        .tag("slug", str(data.get("slug"))) \
        .tag("auctionUrl", str(data.get("auctionUrl"))) \
        .field("soldFor", to_float(data.get("soldFor"))) \
        .field("buyNowAmount", to_float(data.get("buyNowAmount"))) \
        .field("preliminaryPrice", to_float(data.get("preliminaryPrice"))) \
        .field("winningBid", to_float(data.get("winningBid"))) \
        .field("highestBid", to_float(data.get("highestBid"))) \
        .field("odometerReading", to_float(data.get("odometerReading"))) \
        .field("batteryCapacity", to_float(data.get("batteryCapacity"))) \
        .field("costPerKwh", to_float(data.get("costPerKwh"))) \
        .field("buyNowAvailable", bool(data.get("buyNowAvailable"))) \
        .field("isSoldByBuyNow", bool(data.get("isSoldByBuyNow"))) \
        .field("reservationPriceReached", bool(data.get("reservationPriceReached"))) \
        .field("closedAt", data.get("closedAt")) \
        .field("publishedAt", data.get("publishedAt")) \
        .time(datetime.fromisoformat(data.get("closedAt", "").replace("Z", "+00:00")))

    # Check if record exists
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "car_auction")
        |> filter(fn: (r) => r["auctionId"] == "{data['auctionId']}")
    '''
    result = client.query_api().query(query=query, org=INFLUXDB_CONFIG["org"])
    
    if result:
        logging.info(f"🔄 Updating existing record for auction {data['auctionId']}")
    else:
        logging.info(f"📝 Creating new record for auction {data['auctionId']}")

    # Write the point - InfluxDB will automatically handle the update
    write_api.write(bucket=bucket, org=INFLUXDB_CONFIG["org"], record=point)

# Crawl KVD auctions and store data in InfluxDB
def crawl_kvd(limit=None):
    logging.info(f"🚗 Starting crawl at {datetime.now()}...")
    if limit:
        logging.info(f"⚠️ Limiting to first {limit} URLs")
        # Create a list to store records for CSV output
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
            # Set allow_redirects to False to detect redirects
            response = requests.get(detail_url, allow_redirects=False)
            
            # Check if we got a redirect response
            if response.status_code in (301, 302, 303, 307, 308):
                logging.warning(f"⚠️ Skipping {detail_url} - URL redirects to {response.headers.get('Location', 'unknown')}")
                continue
                
            # If not a redirect, proceed with the request
            page = requests.get(detail_url)
            detail_soup = BeautifulSoup(page.text, 'html.parser')
            scripts = detail_soup.find_all('script')

            store_data = None
            for script in scripts:
                if script.string and "storeObjects" in script.string:
                    store_data = extract_store_objects(script.string)
                    if store_data:
                        break

            if store_data:
                record = extract_fields(store_data)
                write_to_influx(record)
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

    # If running with limit, write to CSV
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
    limit = None  # Set to None for full crawl
    
    # Only run immediately if we're not using the scheduler
    if limit is not None:
        crawl_kvd(limit=limit)  # Run once immediately with limit
    else:
        logging.info("🕒 Running in scheduled mode - waiting for 05:00")
        while True:
            schedule.run_pending()
            time.sleep(30)