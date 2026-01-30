#!/usr/bin/env python3
"""
Script to read RERAs from CSV file, send to Telegram bot to get owner details, and output to JSON.

Usage:
  python csv_rera_to_owner_details.py --csv-file propertyfinder_listings_loc_3059.csv --tg-app-id 38533280 --tg-api-hash YOUR_API_HASH --tg-to @mrismail434

Or with environment variables (TELETHON_APP_ID, TELETHON_API_HASH):
  python csv_rera_to_owner_details.py --csv-file propertyfinder_listings_loc_3059.csv --tg-to @mrismail434
"""

import argparse
import asyncio
import csv
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telethon import TelegramClient

import dotenv
from dotenv import load_dotenv

from utils import (
    fetch_owner_for_rera,
    update_db_with_owner_details,
    has_owner_details,
    has_owner_details_response,
    extract_owner_details,
    extract_property_details,
    setup_logging,
    RateLimiter,
    validate_rera,
    get_db_connection,
    serialize_for_db,
    deserialize_from_db,
    LISTINGS_DIR
)

# Setup logging
logger = setup_logging("csv_rera_to_owner_details")


# =============================================================================
# Tracking Utilities
# =============================================================================

def load_used_reras() -> List[str]:
    """Load list of already-used reras from JSON file."""
    used_file = os.path.join(LISTINGS_DIR, 'data', 'used_reras.json')
    if os.path.exists(used_file):
        try:
            with open(used_file, 'r') as f:
                data = json.load(f)
                return data.get('reras', [])
        except Exception as e:
            logger.error(f"Error loading used_reras.json: {e}")
    return []


def save_used_reras(reras: List[str]) -> None:
    """Save used reras to JSON file."""
    data_dir = os.path.join(LISTINGS_DIR, 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    used_file = os.path.join(data_dir, 'used_reras.json')
    try:
        with open(used_file, 'w') as f:
            json.dump({
                'reras': reras, 
                'updated': datetime.now(timezone.utc).isoformat()
            }, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving used_reras.json: {e}")


def add_used_rera(rera: str) -> None:
    """Add a rera to the used list."""
    used_reras = load_used_reras()
    if rera not in used_reras:
        used_reras.append(rera)
        save_used_reras(used_reras)


# =============================================================================
# CSV Utilities
# =============================================================================

def parse_csv_filename(csv_file: str) -> tuple:
    """Parse CSV filename to extract location and category."""
    match = re.search(r'propertyfinder_listings_loc_(\d+)_cat_(\d+)\.csv', csv_file)
    if match:
        loc = match.group(1)
        cat = int(match.group(2))
        return loc, cat
    return None, None


def get_category_name(cat: int) -> str:
    """Get category name from category number."""
    categories = {
        1: 'residential_buy',
        2: 'residential_rent',
        3: 'commercial_buy',
        4: 'commercial_rent'
    }
    return categories.get(cat, 'unknown')


def extract_location_name(listing: Dict[str, Any]) -> str:
    """Extract location name from listing data."""
    area = listing.get('Area', '')
    project = listing.get('Project', '')
    if area:
        return area.replace(' ', '_')
    elif project:
        return project.replace(' ', '_')
    return 'unknown'


def read_listings_from_csv(csv_file: str) -> List[Dict[str, Any]]:
    """Read listings from CSV file."""
    listings = []
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rera = row.get('RERA', '').strip()
                if rera and rera != '':
                    listing = dict(row)
                    listing['rera'] = rera  # Ensure 'rera' key for compatibility
                    listings.append(listing)
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        sys.exit(1)

    logger.info(f"Read {len(listings)} listings from {csv_file}")
    return listings


# =============================================================================
# Database Utilities
# =============================================================================

def get_share_url_from_db(rera: str, db_file: str = None) -> str:
    """Get share URL from listings.db for the given RERA."""
    try:
        conn = get_db_connection(db_file)
        c = conn.cursor()
        c.execute("SELECT raw_json FROM listings WHERE raw_json LIKE ?", (f'%{rera}%',))
        row = c.fetchone()
        if row:
            raw_json = row[0]
            data = deserialize_from_db(raw_json, {})
            share_url = data.get('share_url') or data.get('url') or data.get('listing_url')
            return share_url or ''
        return ''
    except Exception as e:
        logger.error(f"Error getting share URL for RERA {rera}: {e}")
        return ''


# =============================================================================
# Telegram Processing
# =============================================================================

async def send_to_telegram(
    listings: List[Dict], 
    app_id: int, 
    api_hash: str, 
    session_file: str, 
    tg_to: str, 
    stop_event: Optional[asyncio.Event] = None
) -> List[Dict]:
    """Send listings to Telegram bot and collect responses."""
    responses = []
    
    # Create rate limiter for this process
    rate_limiter = RateLimiter(min_delay=3.0, max_delay=60.0, backoff_factor=2.0, jitter=1.0)

    for listing in listings:
        if stop_event and stop_event.is_set():
            logger.info("Stop event detected, stopping processing")
            break
        
        rera = listing.get('rera')
        if not rera:
            continue
        
        if not validate_rera(rera):
            logger.warning(f"Skipping invalid RERA format: {rera}")
            continue
        
        logger.info(f"Fetching owner for RERA {rera}")
        
        try:
            # Use the owner_fetcher module with rate limiting
            rate_limiter.wait()
            result = await fetch_owner_for_rera(rera, tg_to, app_id, api_hash, session_file)
            
            if result['status'] == 'success':
                owner_names = result.get('owner_names', [])
                owner_phones = result.get('owner_phones', [])
                owner_emails = result.get('owner_emails', [])
                property_number = result.get('property_number', '')

                # Update database with owner details
                update_db_with_owner_details(
                    listing.get('id', ''),
                    rera,
                    owner_names,
                    owner_phones,
                    owner_emails,
                    property_number
                )

                # Add to tracking
                add_used_rera(rera)

                resp_data = {
                    'rera': rera,
                    'telegram_message_id': result.get('telegram_message_id'),
                    'telegram_date': result.get('telegram_date'),
                    'sent_status': 'success',
                    'responses': result.get('responses', []),
                    'secondary_responses': result.get('secondary_responses', []),
                    'owner_details_found': result.get('owner_details_found', False),
                    'owner_names': owner_names,
                    'owner_phones': owner_phones,
                    'owner_emails': owner_emails,
                    'property_number': property_number,
                    'status': 'completed'
                }
                responses.append(resp_data)
                logger.info(f"Successfully fetched owner for RERA {rera}")
                rate_limiter.record_success()
            else:
                error = result.get('error', 'Unknown error')
                logger.error(f"Error for RERA {rera}: {error}")
                responses.append({
                    'rera': rera,
                    'status': 'error',
                    'error': error
                })
                rate_limiter.record_failure()

        except Exception as e:
            logger.error(f"Exception processing RERA {rera}: {e}")
            responses.append({
                'rera': rera,
                'status': 'error',
                'error': str(e)
            })
            rate_limiter.record_failure()
    
    return responses


# =============================================================================
# Main Processing
# =============================================================================

async def get_owner_details(
    csv_file: str, 
    output_json_file: str, 
    tg_to: str, 
    max_reras: Optional[int] = None, 
    stop_event: Optional[asyncio.Event] = None
) -> None:
    """Main function to process RERAs and get owner details."""
    # Load environment variables from .env file
    load_dotenv()

    # Telegram mode: require credentials from .env
    app_id = int(os.environ.get('TELETHON_APP_ID', '0'))
    api_hash = os.environ.get('TELETHON_API_HASH', '')

    if not app_id or not api_hash:
        logger.error("TELETHON_APP_ID and TELETHON_API_HASH must be set in .env file")
        return

    # Read listings from CSV
    all_listings = read_listings_from_csv(csv_file)
    if not all_listings:
        logger.error("No listings found in CSV")
        return

    # Parse CSV filename to get location and category for output naming
    loc, cat = parse_csv_filename(csv_file)
    if loc and cat:
        category_name = get_category_name(cat)
        location_name = extract_location_name(all_listings[0])
        output_json_file = os.path.join(
            LISTINGS_DIR, 
            'output', 
            f'owners_{location_name}_{category_name}.json'
        )
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_json_file), exist_ok=True)

    # Load already-used reras to avoid duplicates
    used_reras = load_used_reras()
    logger.info(f"Loaded {len(used_reras)} already-used reras")

    # Filter out already-used reras
    new_listings = [listing for listing in all_listings if str(listing.get('rera')) not in used_reras]
    logger.info(f"Found {len(new_listings)} new listings to process")

    if not new_listings:
        logger.info("No new listings to process")
        return

    # Limit the number of RERAs to process if specified
    if max_reras and max_reras > 0:
        new_listings = new_listings[:max_reras]
        logger.info(f"Limited to processing {len(new_listings)} RERAs as requested")

    # Send to Telegram
    responses = await send_to_telegram(
        new_listings, 
        app_id, 
        api_hash, 
        os.path.join(LISTINGS_DIR, 'extras', 'session.session'), 
        tg_to, 
        stop_event
    )

    # Check if process was stopped early
    was_stopped = stop_event and stop_event.is_set()
    if was_stopped:
        logger.info("Process was stopped early, saving partial results")

    # Extract owner details from responses and merge with listing data
    owners_list = []
    csv_data = []

    for resp in responses:
        rera = resp.get('rera')
        # Find the original listing
        listing = next((l for l in new_listings if l.get('rera') == rera), {})

        owner_names = resp.get('owner_names', [])
        owner_phones = resp.get('owner_phones', [])
        owner_emails = resp.get('owner_emails', [])
        property_number = resp.get('property_number', '')

        # For CSV, concatenate with semicolons
        owner_name_str = '; '.join(owner_names) if owner_names else ''
        owner_phone_str = '; '.join(owner_phones) if owner_phones else ''
        owner_email_str = '; '.join(owner_emails) if owner_emails else ''

        # Merge listing data with owner details
        owner_data = {
            **listing,
            'owner_names': owner_names,
            'owner_phones': owner_phones,
            'owner_emails': owner_emails,
            'property_number': property_number,
            'full_response': resp
        }
        owners_list.append(owner_data)

        # Get share URL from DB
        share_url = get_share_url_from_db(rera)

        # Prepare CSV row
        csv_row = {
            'RERA': listing.get('rera', ''),
            'Property Number': property_number,
            'owner name': owner_name_str,
            'owner pNo': owner_phone_str,
            'owner email': owner_email_str,
            'beds': listing.get('bedrooms', ''),
            'baths': listing.get('bathrooms', ''),
            'size': listing.get('size.value', ''),
            'share_url': share_url
        }
        csv_data.append(csv_row)

    # Filter to only include rows with owner details
    filtered_csv_data = [
        row for row in csv_data 
        if row['owner name'] or row['owner pNo'] or row['owner email']
    ]
    filtered_owners_list = [
        owner for owner in owners_list 
        if owner['owner_names'] or owner['owner_phones'] or owner['owner_emails']
    ]

    owners_output = {
        'csv_file': csv_file,
        'count': len(filtered_owners_list),
        'owners': filtered_owners_list
    }

    # Save to JSON
    with open(output_json_file, 'w', encoding='utf-8') as f:
        json.dump(owners_output, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(filtered_owners_list)} owner details to {output_json_file}")

    # Save to CSV
    output_csv_file = output_json_file.replace('.json', '.csv')
    with open(output_csv_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['RERA No.', 'Property Number', 'Unit No.', 'Rooms', 'Size (m²)', 'Owner Name', 'Phone', 'Email', 'Share URL']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_csv_data)

    logger.info(f"Saved {len(filtered_csv_data)} rows to {output_csv_file}")

    # Print extracted data for testing
    if filtered_owners_list:
        logger.info(f"Extracted {len(filtered_owners_list)} owner details:")
        for i, owner in enumerate(filtered_owners_list, 1):
            logger.info(f"Owner {i}:")
            logger.info(f"  RERA: {owner.get('rera')}")
            logger.info(f"  Name: {'; '.join(owner.get('owner_names', []))}")
            logger.info(f"  Phone: {'; '.join(owner.get('owner_phones', []))}")
            logger.info(f"  Email: {'; '.join(owner.get('owner_emails', []))}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process RERAs from CSV and get owner details via Telegram.'
    )
    parser.add_argument(
        '--csv-file', 
        required=True, 
        help='Path to the CSV file containing RERAs.'
    )
    parser.add_argument(
        '--tg-to', 
        required=True, 
        help='Telegram username or chat ID to send messages to.'
    )
    parser.add_argument(
        '--max-reras',
        type=int,
        default=None,
        help='Maximum number of RERAs to process.'
    )
    args = parser.parse_args()

    asyncio.run(get_owner_details(
        args.csv_file, 
        os.path.join(LISTINGS_DIR, 'output', 'owner_details.json'),
        args.tg_to,
        args.max_reras
    ))

