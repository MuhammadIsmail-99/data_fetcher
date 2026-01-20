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
import io
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import concurrent.futures

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import dotenv


# Removed parse_args function as it's no longer needed


def load_used_reras() -> List[str]:
    """Load list of already-used reras from JSON file."""
    used_file = 'data/used_reras.json'
    if os.path.exists(used_file):
        try:
            with open(used_file, 'r') as f:
                data = json.load(f)
                return data.get('reras', [])
        except Exception as e:
            print(f'[TRACKING] Error loading used_reras.json: {e}', file=sys.stderr)
    return []


def save_used_reras(reras: List[str]):
    """Save used reras to JSON file."""
    used_file = 'data/used_reras.json'
    try:
        with open(used_file, 'w') as f:
            json.dump({'reras': reras, 'updated': datetime.now(timezone.utc).isoformat()}, f, indent=2)
    except Exception as e:
        print(f'[TRACKING] Error saving used_reras.json: {e}', file=sys.stderr)


def add_used_rera(rera: str):
    """Add a rera to the used list."""
    used_reras = load_used_reras()
    if rera not in used_reras:
        used_reras.append(rera)
        save_used_reras(used_reras)


def has_owner_details(responses_text: List[str]) -> bool:
    """Check if any response contains actual owner details."""
    import re

    for text in responses_text:
        if not text:
            continue
        t = text.strip()

        # Check for an explicit owner block marker (with emoji or plain)
        if '👤 owner details:' in t.lower() or 'owner details:' in t.lower():
            return True

        # Check for presence of both Name and Phone fields (with or without emojis)
        name_match = re.search(r'(📝\s*)?name\s*:\s*\S+', t, flags=re.IGNORECASE)
        phone_match = re.search(r'(📞\s*)?phone\s*:\s*\S+', t, flags=re.IGNORECASE)
        if name_match and phone_match:
            return True

    return False


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
    # Assuming location is in 'Area' or 'Project' field
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
        print(f'[CSV] Error reading CSV file {csv_file}: {e}', file=sys.stderr)
        sys.exit(1)

    print(f'[CSV] Read {len(listings)} listings from {csv_file}', file=sys.stderr)
    return listings


def get_share_url_from_db(rera: str, db_file: str = 'listings.db') -> str:
    """Get share URL from listings.db for the given RERA."""
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute("SELECT raw_json FROM listings WHERE raw_json LIKE ?", (f'%{rera}%',))
        row = c.fetchone()
        if row:
            raw_json = row[0]
            data = json.loads(raw_json)
            share_url = data.get('share_url') or data.get('url') or data.get('listing_url')
            return share_url or ''
        return ''
    except Exception as e:
        print(f'[DB] Error getting share URL for RERA {rera}: {e}', file=sys.stderr)
        return ''
    finally:
        if 'conn' in locals():
            conn.close()


async def send_to_telegram(listings, app_id, api_hash, session_file, tg_to, stop_event=None):
    """Send listings to Telegram bot and collect responses."""
    responses = []

    async with TelegramClient(session_file, app_id, api_hash) as client:
        await client.start()

        try:
            for listing in listings:
                if stop_event and stop_event.is_set():
                    print(f'[TELEGRAM] Stop event detected, stopping processing', file=sys.stderr)
                    break
                rera = listing.get('rera')
                if not rera:
                    continue
                message = rera
                try:
                    # Send message
                    sent = await client.send_message(tg_to, message)
                    print(f'[TELEGRAM] Sent message for RERA {rera}', file=sys.stderr)
                    # Wait for bot response
                    await asyncio.sleep(2)
                    # Wait for responses
                    responses_for_rera = []
                    secondary_responses = []
                    # Initial response: get messages after the sent message
                    response = await client.get_messages(tg_to, min_id=sent.id, limit=10)
                    if response and not isinstance(response, list):
                        response = [response] if response else []
                    if response:
                        for msg in response:
                            responses_for_rera.append({'text': msg.text, 'id': msg.id})
                    # Check for button and click if present
                    if response and len(response) > 0 and hasattr(response[0], 'buttons') and response[0].buttons:
                        await asyncio.sleep(2)  # Wait 5 seconds before clicking extra info button
                        await response[0].click(0)  # Click first button
                        print(f'[TELEGRAM] Clicked button for RERA {rera}', file=sys.stderr)
                        # Get secondary response: messages after the initial response (in case there are additional messages)
                        secondary = await client.get_messages(tg_to, min_id=response[0].id, limit=10)
                        if secondary and not isinstance(secondary, list):
                            secondary = [secondary] if secondary else []
                        if secondary:
                            for msg in secondary:
                                secondary_responses.append({'text': msg.text, 'id': msg.id})
                        # Wait for the message to update with owner details
                        await asyncio.sleep(4)
                        # Get the updated message (bot updates the same message after clicking)
                        updated_messages = await client.get_messages(tg_to, ids=response[0].id)
                        if updated_messages and not isinstance(updated_messages, list):
                            updated_messages = [updated_messages] if updated_messages else []
                        if updated_messages:
                            responses_for_rera[0]['text'] = updated_messages[0].text  # Update the text with owner details
                        # Also get the latest message in the chat in case the update is in a new message
                        latest_messages = await client.get_messages(tg_to, limit=1)
                        if latest_messages and not isinstance(latest_messages, list):
                            latest_messages = [latest_messages] if latest_messages else []
                        if latest_messages and latest_messages[0].id != response[0].id:
                            secondary_responses.append({'text': latest_messages[0].text, 'id': latest_messages[0].id})
                    # Check for owner details in all responses (updated initial + secondary)
                    owner_details_found = has_owner_details([r['text'] for r in responses_for_rera + secondary_responses])
                    # Debug: print response texts
                    print(f'[DEBUG] Responses for RERA {rera}:', file=sys.stderr)
                    for r in responses_for_rera + secondary_responses:
                        print(f'  {r["text"]}', file=sys.stderr)
                    resp_data = {
                        'rera': rera,
                        'telegram_message_id': sent.id,
                        'telegram_date': sent.date.isoformat(),
                        'sent_status': 'success',
                        'responses': responses_for_rera,
                        'button_clicked': bool(secondary_responses),
                        'secondary_responses': secondary_responses,
                        'owner_details_found': owner_details_found,
                        'status': 'completed'
                    }
                    responses.append(resp_data)
                    # Save partial responses after each RERA
                    with open('output/partial_responses.json', 'w', encoding='utf-8') as f:
                        json.dump(responses, f, ensure_ascii=False, indent=2)
                    # Add to used reras
                    add_used_rera(str(rera))
                    # Delay to avoid rate limits
                    await asyncio.sleep(4)
                except Exception as e:
                    print(f'[TELEGRAM] Error processing RERA {rera}: {e}', file=sys.stderr)
                    responses.append({
                        'rera': rera,
                        'status': 'error',
                        'error': str(e)
                    })
        except KeyboardInterrupt:
            print('[TELEGRAM] KeyboardInterrupt detected, saving partial responses to output/partial_responses.json', file=sys.stderr)
            with open('output/partial_responses.json', 'w', encoding='utf-8') as f:
                json.dump(responses, f, ensure_ascii=False, indent=2)
        return responses


async def get_owner_details(csv_file, output_json_file, tg_to, max_reras=None, stop_event=None):
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Telegram mode: require credentials from .env
    app_id = int(os.environ.get('TELETHON_APP_ID', 0))
    api_hash = os.environ.get('TELETHON_API_HASH', '')

    if not app_id or not api_hash:
        print('ERROR: TELETHON_APP_ID and TELETHON_API_HASH must be set in .env file', file=sys.stderr)
        return

    # Read listings from CSV
    all_listings = read_listings_from_csv(csv_file)
    if not all_listings:
        print('[MAIN] No listings found in CSV', file=sys.stderr)
        return

    # Parse CSV filename to get location and category for output naming
    loc, cat = parse_csv_filename(csv_file)
    if loc and cat:
        category_name = get_category_name(cat)
        location_name = extract_location_name(all_listings[0])
        output_json_file = f'output/owners_{location_name}_{category_name}.json'
    # If parsing fails, use the provided output_json_file

    # Load already-used reras to avoid duplicates
    used_reras = load_used_reras()
    print(f'[MAIN] Loaded {len(used_reras)} already-used reras', file=sys.stderr)

    # Filter out already-used reras
    new_listings = [listing for listing in all_listings if str(listing.get('rera')) not in used_reras]
    print(f'[MAIN] Found {len(new_listings)} new listings to process', file=sys.stderr)

    if not new_listings:
        print('[MAIN] No new listings to process', file=sys.stderr)
        return

    # Limit the number of RERAs to process if specified
    if max_reras and max_reras > 0:
        new_listings = new_listings[:max_reras]
        print(f'[MAIN] Limited to processing {len(new_listings)} RERAs as requested', file=sys.stderr)

    # Send to Telegram
    responses = await send_to_telegram(new_listings, app_id, api_hash, 'extras/session.session', tg_to, stop_event)

    # Check if process was stopped early
    was_stopped = stop_event and stop_event.is_set()
    if was_stopped:
        print(f'[MAIN] Process was stopped early, saving partial results', file=sys.stderr)

    # Extract owner details from responses and merge with listing data
    import re
    owners_list = []
    csv_data = []
    for resp in responses:
        rera = resp.get('rera')
        # Find the original listing
        listing = next((l for l in new_listings if l.get('rera') == rera), {})

        owner_names = []
        owner_phones = []
        owner_emails = []

        if resp.get('owner_details_found'):
            # Extract from all responses (updated initial + secondary)
            all_resp = resp.get('responses', []) + resp.get('secondary_responses', [])
            combined_text = '\n'.join([s.get('text', '') for s in all_resp])
            # Split by owner blocks if possible, but for now, find all matches
            # name
            name_matches = re.findall(r'(?:📝\s*)?name\s*:\s*(.+?)(?=\n|$)', combined_text, flags=re.IGNORECASE | re.MULTILINE)
            for match in name_matches:
                name = match.strip().split('\n')[0].strip()
                if name and name not in owner_names:
                    owner_names.append(name)
            # phone
            phone_matches = re.findall(r'(?:📞\s*)?phone\s*:\s*(\+?[0-9\s\-()]{6,})', combined_text, flags=re.IGNORECASE)
            for match in phone_matches:
                phone = match.strip()
                if phone and phone not in owner_phones:
                    owner_phones.append(phone)
            # email
            email_matches = re.findall(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', combined_text)
            for match in email_matches:
                email = match.strip()
                if email and email not in owner_emails:
                    owner_emails.append(email)

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
            'full_response': resp
        }
        owners_list.append(owner_data)

        # Get share URL from DB
        share_url = get_share_url_from_db(rera)

        # Prepare CSV row
        csv_row = {
            'RERA': listing.get('rera', ''),
            'owner name': owner_name_str,
            'owner pNo': owner_phone_str,
            'owner email': owner_email_str,
            'beds': listing.get('bedrooms', ''),
            'baths': listing.get('bathrooms', ''),
            'size': listing.get('size.value', ''),
            'share_url': share_url
        }
        csv_data.append(csv_row)

    # Filter CSV data and owners list to only include rows with owner details
    filtered_csv_data = [row for row in csv_data if row['owner name'] or row['owner pNo'] or row['owner email']]
    filtered_owners_list = [owner for owner in owners_list if owner['owner_names'] or owner['owner_phones'] or owner['owner_emails']]

    owners_output = {
        'csv_file': csv_file,
        'count': len(filtered_owners_list),
        'owners': filtered_owners_list
    }

    # Save to JSON
    with open(output_json_file, 'w', encoding='utf-8') as f:
        json.dump(owners_output, f, ensure_ascii=False, indent=2)

    print(f'[MAIN] Saved {len(filtered_owners_list)} owner details to {output_json_file}', file=sys.stderr)

    # Save to CSV
    output_csv_file = output_json_file.replace('.json', '.csv')
    with open(output_csv_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['RERA No.', 'Unit No.', 'Rooms', 'Size (m²)', 'Owner Name', 'Phone', 'Email', 'Share URL']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_csv_data)

    print(f'[MAIN] Saved {len(filtered_csv_data)} rows to {output_csv_file}', file=sys.stderr)

    # Print extracted data for testing
    if filtered_owners_list:
        print(f'[MAIN] Extracted {len(filtered_owners_list)} owner details:', file=sys.stderr)
        for i, owner in enumerate(filtered_owners_list, 1):
            print(f'[MAIN] Owner {i}:', file=sys.stderr)
            print(f'  RERA: {owner.get("rera")}', file=sys.stderr)
            print(f'  Name: {"; ".join(owner.get("owner_names", []))}', file=sys.stderr)
            print(f'  Phone: {"; ".join(owner.get("owner_phones", []))}', file=sys.stderr)
            print(f'  Email: {"; ".join(owner.get("owner_emails", []))}', file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process RERAs from CSV and get owner details via Telegram.')
    parser.add_argument('--csv-file', required=True, help='Path to the CSV file containing RERAs.')
    parser.add_argument('--tg-to', required=True, help='Telegram username or chat ID to send messages to.')
    args = parser.parse_args()

    asyncio.run(get_owner_details(args.csv_file, 'output/owner_details.json', args.tg_to))
