import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
import os
import csv
import logging
import pg8000
import ssl
import gc
from supabase_conf import DB_CONFIG
from flask import Flask, request, jsonify
import psutil

"""
Starting manually on Google Could Run Function

curl -X POST https://car-crawler-884815102822.europe-west4.run.app/ \
  -H "Content-Type: application/json" \
  -d '{"limit": null}'

"""


app = Flask(__name__)

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Constants for batch processing
BATCH_SIZE = 1  # Still process one at a time for memory
REQUEST_TIMEOUT = 30  # 30 seconds timeout for requests
MAX_RETRIES = 6  # Increased number of retries for failed requests
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, 'crawler_checkpoint.json')

# Database setup
def get_db_connection():
    try:
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        return pg8000.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            ssl_context=ssl_context
        )
    except Exception as e:
        logging.error(f"❌ Database connection error: {str(e)}")
        raise

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
        engine_power_hp = None
        engine_power = None
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
        model_name_with_brand = model_name_short

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
            "familyName": properties.get("familyName") or None,
            "registrationPlate": base_obj.get("registrationPlate") or None,
            "modelName": model_name,
            "modelNamePresentation": model_name_with_brand,
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
            "objectViewJson": store.get('objectView', {}),
            "base_object_type": base_obj.get("baseObjectType") or None
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

        # Convert objectViewJson to JSON string if it exists
        object_view_json = data.get('objectViewJson')
        if object_view_json:
            object_view_json = json.dumps(object_view_json)

        # Convert numeric fields to appropriate types
        numeric_fields = {
            'soldFor': float,
            'buyNowAmount': float,
            'preliminaryPrice': float,
            'winningBid': float,
            'highestBid': float,
            'odometerReading': int,
            'year': int,
            'batteryCapacity': float,
            'rangeCityWltpDrive': int,
            'rangeWltpDrive': int,
            'enginePowerHp': int,
            'enginePower': int
        }

        # Prepare the data for database
        db_data = {
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
            'family_name': data.get('familyName'),
            'registration_plate': data.get('registrationPlate'),
            'model_name': data.get('modelName'),
            'model_name_presentation': data.get('modelNamePresentation'),
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
            'object_view_json': object_view_json,
            'base_object_type': data.get('base_object_type')
        }

        # Convert numeric fields to appropriate types
        for field, type_func in numeric_fields.items():
            db_field = field[0].lower() + field[1:]  # Convert to snake_case
            value = db_data.get(db_field)
            if value is not None:
                try:
                    db_data[db_field] = type_func(value)
                except (ValueError, TypeError):
                    logging.warning(f"⚠️ Could not convert {field} value '{value}' to {type_func.__name__}")
                    db_data[db_field] = None

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check if record exists
                cur.execute("SELECT * FROM car_auctions WHERE auction_id = %s", (data['auctionId'],))
                existing_record = cur.fetchone()

                if existing_record:
                    # Update existing record
                    set_clause = ", ".join([f"{k} = %s" for k in db_data.keys()])
                    values = list(db_data.values())
                    query = f"UPDATE car_auctions SET {set_clause} WHERE auction_id = %s"
                    cur.execute(query, values + [data['auctionId']])
                    logging.info(f"🔄 Updated record for auction {data['auctionId']}")
                else:
                    # Insert new record
                    columns = ", ".join(db_data.keys())
                    placeholders = ", ".join(["%s"] * len(db_data))
                    query = f"INSERT INTO car_auctions ({columns}) VALUES ({placeholders})"
                    cur.execute(query, list(db_data.values()))
                    logging.info(f"📝 Created new record for auction {data['auctionId']}")

                conn.commit()

    except Exception as e:
        logging.error(f"❌ Error writing to database: {str(e)}")
        logging.error(f"❌ Error details: {type(e).__name__}")
        import traceback
        logging.error(f"❌ Full traceback: {traceback.format_exc()}")

def save_checkpoint(processed_urls):
    """Save the list of processed URLs to a checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'processed_urls': processed_urls
            }, f)
    except Exception as e:
        logging.error(f"❌ Error saving checkpoint: {str(e)}")

def load_checkpoint():
    """Load the checkpoint file if it exists."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"❌ Error loading checkpoint: {str(e)}")
    return None

def clear_checkpoint():
    """Clear the checkpoint file."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    except Exception as e:
        logging.error(f"❌ Error clearing checkpoint: {str(e)}")

def log_memory_usage(context=""):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    logging.info(f"MEMORY USAGE {context}: RSS={mem_info.rss / (1024 * 1024):.2f} MB, VMS={mem_info.vms / (1024 * 1024):.2f} MB")

def process_url_single(detail_url):
    """Process a single URL and write the result to the database."""
    for attempt in range(MAX_RETRIES):
        try:
            log_memory_usage(f"before processing {detail_url}")
            logging.info(f"🔍 Fetching {detail_url} (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(5)  # 5 second delay between requests
            
            response = requests.get(detail_url, allow_redirects=False, timeout=REQUEST_TIMEOUT)
            if response.status_code in (301, 302, 303, 307, 308):
                logging.warning(f"⚠️ Skipping {detail_url} - URL redirects to {response.headers.get('Location', 'unknown')}")
                return
            page = requests.get(detail_url, timeout=REQUEST_TIMEOUT)
            detail_soup = BeautifulSoup(page.text, 'html.parser')
            scripts = detail_soup.find_all('script')

            # Try to get image URL from store data first
            main_image_url = None
            image_source = None
            store_data = None
            for script in scripts:
                if script.string and "storeObjects" in script.string:
                    store_data = extract_store_objects(script.string)
                    if store_data:
                        break

            if store_data:
                # Try to get image from objectView.storeObjects.{auctionId}.previewImage
                for key, item in store_data.get('objectView', {}).get('storeObjects', {}).items():
                    if item and item.get('previewImage'):
                        main_image_url = item['previewImage']
                        image_source = 'store_data_preview_image'
                        logging.info(f"✅ Found image URL in store data previewImage: {main_image_url}")
                        break

            # If no image found in store data, try meta tags
            if not main_image_url:
                meta_image = detail_soup.find('meta', property='og:image')
                if meta_image and meta_image.get('content'):
                    main_image_url = meta_image['content']
                    image_source = 'meta_og_image'
                else:
                    meta_image = detail_soup.find('meta', attrs={'property': 'og:image', 'data-react-helmet': 'true'})
                    if meta_image and meta_image.get('content'):
                        main_image_url = meta_image['content']
                        image_source = 'meta_react_helmet'
                    else:
                        meta_image = detail_soup.find('meta', attrs={'name': 'og:image'})
                        if meta_image and meta_image.get('content'):
                            main_image_url = meta_image['content']
                            image_source = 'meta_name_og_image'
                        else:
                            meta_images = detail_soup.find_all('meta')
                            for meta in meta_images:
                                content = meta.get('content', '')
                                if 'imgix.net' in content:
                                    main_image_url = content
                                    image_source = 'meta_imgix_net'
                                    break

            if main_image_url:
                logging.info(f"✅ Found image URL from {image_source}: {main_image_url}")
            else:
                logging.warning(f"⚠️ No image URL found for {detail_url}")

            if store_data:
                record = extract_fields(store_data)
                if record:
                    record['mainImageUrl'] = main_image_url
                    record['imageSource'] = image_source
                    write_to_supabase(record)
                    # Explicitly free memory
                    del detail_soup, scripts, store_data, record
                    gc.collect()
                    log_memory_usage(f"after processing {detail_url}")
                    return
                else:
                    logging.warning(f"⚠️ Failed to extract fields from store data for {detail_url}")
            else:
                logging.warning(f"⚠️ Failed to extract JSON from {detail_url}")
                if attempt == MAX_RETRIES - 1:
                    logging.error(f"❌ Failed to process {detail_url} after {MAX_RETRIES} attempts")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Network error for {detail_url}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                logging.error(f"❌ Failed to process {detail_url} after {MAX_RETRIES} attempts")
            time.sleep(5 * (attempt + 1))  # More forgiving backoff
        except Exception as e:
            logging.error(f"❌ Unexpected error for {detail_url}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                logging.error(f"❌ Failed to process {detail_url} after {MAX_RETRIES} attempts")
            time.sleep(5 * (attempt + 1))  # More forgiving backoff
        gc.collect()
        log_memory_usage(f"after gc.collect() for {detail_url}")

def crawl_kvd(limit=None):
    logging.info(f"🚗 Starting crawl at {datetime.now()}...")
    log_memory_usage("at crawl start")
    try:
        url = "https://www.kvd.se/stangda-auktioner"
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.select('a[href^="/auktioner/"]')
        detail_urls = ["https://www.kvd.se" + a['href'] for a in links]
        if limit:
            detail_urls = detail_urls[:limit]
        total_urls = len(detail_urls)
        logging.info(f"🔍 Processing {total_urls} URLs one at a time")
        for i, detail_url in enumerate(detail_urls, 1):
            logging.info(f"➡️ Processing {i}/{total_urls}: {detail_url}")
            process_url_single(detail_url)
            gc.collect()
            log_memory_usage(f"after gc.collect() in crawl_kvd loop {i}/{total_urls}")
            time.sleep(2)  # Small delay between records
        log_memory_usage("at crawl end")
        return {"status": "success", "processed_urls": total_urls}
    except Exception as e:
        logging.error(f"❌ Error in crawl_kvd: {str(e)}")
        log_memory_usage("on crawl error")
        return {"status": "error", "error": str(e)}

@app.route('/', methods=['GET', 'POST'])
def handle_request():
    """Cloud Run entry point."""
    if request.method == 'GET':
        return jsonify({
            "status": "healthy",
            "message": "Car crawler service is running. Use POST to trigger the crawler.",
            "usage": "Send a POST request with optional 'limit' parameter in JSON body"
        }), 200

    try:
        # Get the limit from the request if provided
        request_json = request.get_json(silent=True)
        limit = request_json.get('limit') if request_json else None
        
        # Run the crawler
        result = crawl_kvd(limit=limit)
        
        return jsonify(result), 200
    except Exception as e:
        logging.error(f"❌ Error in main function: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Cloud Run,
    # a production-grade WSGI server will be used instead.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080))) 