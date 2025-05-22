import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
import os
import csv
import logging
import psycopg2
from psycopg2.extras import DictCursor
from supabase_conf import DB_CONFIG

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
    return psycopg2.connect(**DB_CONFIG)

# ... [keep all the helper functions: extract_store_objects, clean_model_name, etc.] ...

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

def main(request):
    """Cloud Function entry point."""
    try:
        # Get the limit from the request if provided
        request_json = request.get_json(silent=True)
        limit = request_json.get('limit') if request_json else None
        
        # Run the crawler
        result = crawl_kvd(limit=limit)
        
        return result, 200
    except Exception as e:
        logging.error(f"❌ Error in main function: {str(e)}")
        return {"error": str(e)}, 500 