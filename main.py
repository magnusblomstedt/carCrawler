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
from supabase_conf import DB_CONFIG
from flask import Flask, request, jsonify

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

# Database setup
def get_db_connection():
    return pg8000.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        ssl_context=True
    )

def extract_store_objects(script_content):
    """Extract store objects from script content."""
    try:
        # First try the standard pattern
        match = re.search(r'window\.storeObjects\s*=\s*({.*?});', script_content, re.DOTALL)
        
        # If not found, try alternative patterns
        if not match:
            # Try without window prefix
            match = re.search(r'storeObjects\s*=\s*({.*?});', script_content, re.DOTALL)
        
        if not match:
            # Try with var declaration
            match = re.search(r'var\s+storeObjects\s*=\s*({.*?});', script_content, re.DOTALL)
        
        if not match:
            # Try with const declaration
            match = re.search(r'const\s+storeObjects\s*=\s*({.*?});', script_content, re.DOTALL)
        
        if not match:
            # Try with let declaration
            match = re.search(r'let\s+storeObjects\s*=\s*({.*?});', script_content, re.DOTALL)
        
        if not match:
            # Try finding any object that might contain auction data
            match = re.search(r'({.*?"id":\s*"\d+".*?})', script_content, re.DOTALL)
        
        if match:
            store_objects_str = match.group(1)
            
            # More aggressive JSON cleaning
            # Replace undefined with null
            store_objects_str = re.sub(r'undefined', 'null', store_objects_str)
            
            # Handle dates
            store_objects_str = re.sub(r'new Date\((.*?)\)', r'"\1"', store_objects_str)
            
            # Remove trailing commas
            store_objects_str = re.sub(r',\s*}', '}', store_objects_str)
            store_objects_str = re.sub(r',\s*]', ']', store_objects_str)
            
            # Fix unquoted property names
            store_objects_str = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', store_objects_str)
            
            # Fix single quotes to double quotes
            store_objects_str = re.sub(r"'", '"', store_objects_str)
            
            # Remove any non-printable characters
            store_objects_str = ''.join(char for char in store_objects_str if char.isprintable())
            
            # Remove any comments
            store_objects_str = re.sub(r'//.*?\n', '\n', store_objects_str)
            store_objects_str = re.sub(r'/\*.*?\*/', '', store_objects_str, flags=re.DOTALL)
            
            try:
                # Try to parse the JSON
                return json.loads(store_objects_str)
            except json.JSONDecodeError as e:
                logging.error(f"❌ JSON decode error: {str(e)}")
                # Log the problematic part of the JSON
                error_position = e.pos
                start = max(0, error_position - 50)
                end = min(len(store_objects_str), error_position + 50)
                logging.error(f"Problematic JSON section: ...{store_objects_str[start:end]}...")
                logging.error(f"Error position: {error_position}")
                return None
        else:
            logging.warning("⚠️ No store objects found in script content")
            # Log a sample of the script content for debugging
            logging.debug(f"Script content sample: {script_content[:500]}...")
            return None
    except Exception as e:
        logging.error(f"❌ Error extracting store objects: {str(e)}")
        return None

def clean_model_name(model_name):
    """Clean and standardize model name."""
    if not model_name:
        return None
    # Remove common suffixes and clean up
    model_name = re.sub(r'\s*\(.*?\)', '', model_name)  # Remove anything in parentheses
    model_name = re.sub(r'\s*[A-Za-z]+\s*$', '', model_name)  # Remove trailing words
    return model_name.strip()

def extract_fields(store_data):
    """Extract relevant fields from store data."""
    try:
        # Get the first auction object
        auction = next(iter(store_data.get('objectView', {}).get('storeObjects', {}).values()))
        if not auction:
            return None

        # Extract basic fields
        record = {
            'auctionId': auction.get('id'),
            'closedAt': auction.get('closedAt'),
            'publishedAt': auction.get('publishedAt'),
            'soldFor': auction.get('soldFor'),
            'sellMethod': auction.get('sellMethod'),
            'slug': auction.get('slug'),
            'auctionUrl': f"https://www.kvd.se/auktioner/{auction.get('slug')}",
            'buyNowAmount': auction.get('buyNowAmount'),
            'buyNowAvailable': auction.get('buyNowAvailable'),
            'preliminaryPrice': auction.get('preliminaryPrice'),
            'isSoldByBuyNow': auction.get('isSoldByBuyNow'),
            'winningBid': auction.get('winningBid'),
            'reservationPriceReached': auction.get('reservationPriceReached'),
            'highestBid': auction.get('highestBid'),
        }

        # Extract object data
        object_data = auction.get('object', {})
        if object_data:
            record.update({
                'electricType': object_data.get('electricType'),
                'odometerReading': object_data.get('odometerReading'),
                'body': object_data.get('body'),
                'brand': object_data.get('brand'),
                'familyName': object_data.get('familyName'),
                'registrationPlate': object_data.get('registrationPlate'),
                'modelName': object_data.get('modelName'),
                'modelNamePresentation': object_data.get('modelNamePresentation'),
                'year': object_data.get('year'),
                'facilityPostCode': object_data.get('facilityPostCode'),
                'facilityCity': object_data.get('facilityCity'),
                'fuelCode': object_data.get('fuelCode'),
                'batteryCapacity': object_data.get('batteryCapacity'),
                'rangeCityWltpDrive': object_data.get('rangeCityWltpDrive'),
                'rangeWltpDrive': object_data.get('rangeWltpDrive'),
                'enginePowerHp': object_data.get('enginePowerHp'),
                'enginePower': object_data.get('enginePower'),
                'gearbox': object_data.get('gearbox'),
            })

        # Clean up model name
        if record.get('modelName'):
            record['modelName'] = clean_model_name(record['modelName'])

        # Store the full object view JSON for reference
        record['objectViewJson'] = store_data

        return record
    except Exception as e:
        logging.error(f"❌ Error extracting fields: {str(e)}")
        return None

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
            # Add a small delay between requests
            time.sleep(1)  # 1 second delay
            
            response = requests.get(detail_url, allow_redirects=False)
            
            if response.status_code in (301, 302, 303, 307, 308):
                logging.warning(f"⚠️ Skipping {detail_url} - URL redirects to {response.headers.get('Location', 'unknown')}")
                continue
                
            page = requests.get(detail_url)
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
                # Try multiple ways to find the image URL in meta tags
                meta_image = detail_soup.find('meta', property='og:image')
                if meta_image and meta_image.get('content'):
                    main_image_url = meta_image['content']
                    image_source = 'meta_og_image'
                else:
                    # Try with React Helmet attribute
                    meta_image = detail_soup.find('meta', attrs={'property': 'og:image', 'data-react-helmet': 'true'})
                    if meta_image and meta_image.get('content'):
                        main_image_url = meta_image['content']
                        image_source = 'meta_react_helmet'
                    else:
                        # Try alternative meta tag formats
                        meta_image = detail_soup.find('meta', attrs={'name': 'og:image'})
                        if meta_image and meta_image.get('content'):
                            main_image_url = meta_image['content']
                            image_source = 'meta_name_og_image'
                        else:
                            # Try to find any meta tag with image in content
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
                record['mainImageUrl'] = main_image_url
                record['imageSource'] = image_source  # Add the source to the record
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

    return {"status": "success", "processed_urls": len(detail_urls)}

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