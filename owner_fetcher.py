#!/usr/bin/env python3
"""
Backend module for fetching owner details from Telegram bot and updating the database.
This extracts logic from csv_rera_to_owner_details.py for reuse in the Streamlit UI.
"""

import asyncio
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from utils import (
    get_db_connection,
    get_owners_db_connection,
    has_owner_details as utils_has_owner_details,
    extract_owner_details as utils_extract_owner_details,
    serialize_for_db,
    deserialize_from_db,
    RateLimiter,
    validate_rera,
    setup_logging,
    LISTINGS_DIR
)

try:
    from telethon import TelegramClient
except ImportError:
    print("Warning: Telethon not installed. Some features will not work.")
    TelegramClient = None

try:
    import dotenv
except ImportError:
    dotenv = None

# Setup logging
logger = setup_logging("owner_fetcher")

# Session file path - configurable via environment variable
_session_file_env = os.environ.get('TELETHON_SESSION_FILE', 'extras/session.session')
if not os.path.isabs(_session_file_env):
    SESSION_FILE = os.path.join(LISTINGS_DIR, _session_file_env)
else:
    SESSION_FILE = _session_file_env

logger.debug(f"SESSION_FILE set to: {SESSION_FILE}")

# Adaptive rate limiter for Telegram requests
rate_limiter = RateLimiter(min_delay=2.0, max_delay=30.0, backoff_factor=1.5, jitter=0.5)


async def fetch_owner_for_rera(
    rera: str, 
    tg_to: str, 
    app_id: int, 
    api_hash: str, 
    session_file: Optional[str] = None
) -> Optional[Dict]:
    """Fetch owner details for a single RERA from Telegram bot.
    
    Args:
        rera: RERA number to fetch owner details for
        tg_to: Telegram bot username or chat ID
        app_id: Telegram API app ID
        api_hash: Telegram API hash
        session_file: Optional session file path (defaults to SESSION_FILE env or 'extras/session.session')
        
    Returns:
        Dict with owner details or error information
    """
    if TelegramClient is None:
        return {
            'rera': rera,
            'status': 'error',
            'error': 'Telethon not installed'
        }
    
    # Validate RERA format
    if not validate_rera(rera):
        logger.warning(f"Invalid RERA format: {rera}")
        return {
            'rera': rera,
            'status': 'error',
            'error': 'Invalid RERA format'
        }
    
    # Use default session file if not provided
    if session_file is None:
        session_file = SESSION_FILE
    
    # Make session file path absolute if relative
    if not os.path.isabs(session_file):
        session_file = os.path.join(LISTINGS_DIR, session_file)
    
    # Wait for rate limiting
    rate_limiter.wait()
    
    try:
        async with TelegramClient(session_file, app_id, api_hash) as client:
            await client.start()

            message = str(rera)
            
            # Send message
            sent = await client.send_message(tg_to, message)

            responses_for_rera = []
            secondary_responses = []

            # Try to get initial response immediately, retry with delay if needed
            response = None
            max_retries = 3
            retry_delay = 1.0

            for attempt in range(max_retries):
                # Get initial response
                response = await client.get_messages(tg_to, min_id=sent.id, limit=10)
                if response and not isinstance(response, list):
                    response = [response] if response else []

                # Check if we got a meaningful response
                if response and len(response) > 0:
                    # Check if response has text content or buttons
                    has_content = any(msg.text.strip() for msg in response)
                    has_buttons = any(hasattr(msg, 'buttons') and msg.buttons for msg in response)

                    if has_content or has_buttons:
                        break  # We have a valid response

                # If no response or empty response, wait and retry
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)

            if response:
                for msg in response:
                    responses_for_rera.append({'text': msg.text, 'id': msg.id})

            # Check for button and click if present
            if response and len(response) > 0 and hasattr(response[0], 'buttons') and response[0].buttons:
                await asyncio.sleep(1)  # Reduced from 2s
                await response[0].click(0)  # Click first button

                # Get secondary response
                secondary = await client.get_messages(tg_to, min_id=response[0].id, limit=10)
                if secondary and not isinstance(secondary, list):
                    secondary = [secondary] if secondary else []
                if secondary:
                    for msg in secondary:
                        secondary_responses.append({'text': msg.text, 'id': msg.id})

                # Wait for update (reduced from 4s)
                await asyncio.sleep(2)

                # Get updated message
                updated_messages = await client.get_messages(tg_to, ids=response[0].id)
                if updated_messages and not isinstance(updated_messages, list):
                    updated_messages = [updated_messages] if updated_messages else []
                if updated_messages:
                    responses_for_rera[0]['text'] = updated_messages[0].text

                # Get latest message
                latest_messages = await client.get_messages(tg_to, limit=1)
                if latest_messages and not isinstance(latest_messages, list):
                    latest_messages = [latest_messages] if latest_messages else []
                if latest_messages and latest_messages[0].id != response[0].id:
                    secondary_responses.append({'text': latest_messages[0].text, 'id': latest_messages[0].id})
            
            # Check for owner details
            all_responses = responses_for_rera + secondary_responses
            owner_details_found = utils_has_owner_details([r['text'] for r in all_responses])

            owner_names = []
            owner_phones = []
            owner_emails = []

            if owner_details_found:
                response_texts = [r['text'] for r in all_responses]
                owner_names, owner_phones, owner_emails = utils_extract_owner_details(response_texts)

            # Log Telegram responses for debugging
            logger.info(f"Telegram responses for RERA {rera}:")
            for resp in responses_for_rera:
                logger.info(f"  Primary: {resp['text'][:200]}..." if len(resp['text']) > 200 else f"  Primary: {resp['text']}")
            for resp in secondary_responses:
                logger.info(f"  Secondary: {resp['text'][:200]}..." if len(resp['text']) > 200 else f"  Secondary: {resp['text']}")

            # Record success for rate limiter
            rate_limiter.record_success()

            return {
                'rera': rera,
                'telegram_message_id': sent.id,
                'telegram_date': sent.date.isoformat(),
                'responses': responses_for_rera,
                'secondary_responses': secondary_responses,
                'owner_details_found': owner_details_found,
                'owner_names': owner_names,
                'owner_phones': owner_phones,
                'owner_emails': owner_emails,
                'status': 'success'
            }
    
    except Exception as e:
        # Record failure for rate limiter
        rate_limiter.record_failure()
        logger.error(f"Error fetching owner for RERA {rera}: {e}", exc_info=True)
        return {
            'rera': rera,
            'status': 'error',
            'error': str(e)
        }


def update_db_with_owner_details(
    listing_id: str,
    rera: str,
    owner_names: List[str],
    owner_phones: List[str],
    owner_emails: List[str],
    db_file: str = None
) -> bool:
    """Update the owners database with owner details.

    Args:
        listing_id: The listing ID to update
        rera: RERA number
        owner_names: List of owner names
        owner_phones: List of owner phone numbers
        owner_emails: List of owner email addresses
        db_file: Optional database file path (ignored, uses owners.db)

    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_owners_db_connection()
        c = conn.cursor()

        # Use utility functions for serialization
        names_json = serialize_for_db(owner_names)
        phones_json = serialize_for_db(owner_phones)
        emails_json = serialize_for_db(owner_emails)
        fetched_at = get_current_isoformat()

        # Check if owner details exist for this listing
        c.execute("SELECT id FROM owners WHERE listing_id = ?", (listing_id,))
        existing = c.fetchone()

        if existing:
            # Update existing owner details
            c.execute("""
                UPDATE owners
                SET rera = ?, owner_names = ?, owner_phones = ?, owner_emails = ?, fetched_at = ?
                WHERE listing_id = ?
            """, (rera, names_json, phones_json, emails_json, fetched_at, listing_id))
        else:
            # Insert new owner details
            c.execute("""
                INSERT INTO owners (listing_id, rera, owner_names, owner_phones, owner_emails, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (listing_id, rera, names_json, phones_json, emails_json, fetched_at))

        conn.commit()
        conn.close()
        logger.info(f"Updated owners DB for listing {listing_id} (RERA {rera}) with owner details")
        return True

    except Exception as e:
        logger.error(f"Error updating owners DB for listing {listing_id} (RERA {rera}): {e}", exc_info=True)
        raise


def get_listing_by_id(listing_id: str, db_file: str = None) -> Optional[Dict]:
    """Get a specific listing by ID.
    
    Args:
        listing_id: The listing ID to look up
        db_file: Optional database file path
        
    Returns:
        Dict with listing data or None if not found
    """
    try:
        conn = get_db_connection(db_file)
        c = conn.cursor()
        c.execute("SELECT * FROM listings WHERE id = ?", (listing_id,))
        columns = [description[0] for description in c.description]
        row = c.fetchone()
        conn.close()
        
        if row:
            return dict(zip(columns, row))
        return None
    
    except Exception as e:
        logger.error(f"Error getting listing {listing_id}: {e}")
        return None


def get_listings_by_rera(reras: List[str], db_file: str = None) -> List[Dict]:
    """Get multiple listings by RERA.
    
    Args:
        reras: List of RERA numbers to look up
        db_file: Optional database file path
        
    Returns:
        List of dicts with listing data
    """
    try:
        conn = get_db_connection(db_file)
        c = conn.cursor()
        placeholders = ','.join(['?' for _ in reras])
        c.execute(
            f"SELECT id, title, rera, owner_names, owner_phones, owner_emails, owner_fetched_at "
            f"FROM listings WHERE rera IN ({placeholders})", 
            reras
        )
        columns = [description[0] for description in c.description]
        rows = c.fetchall()
        conn.close()
        
        return [dict(zip(columns, row)) for row in rows]
    
    except Exception as e:
        logger.error(f"Error getting listings: {e}")
        return []


def has_owner_details_fetched(listing_id: str) -> bool:
    """Check if owner details have been fetched for a listing.
    
    Args:
        listing_id: The listing ID to check
        
    Returns:
        True if owner details exist, False otherwise
    """
    try:
        conn = get_owners_db_connection()
        c = conn.cursor()
        c.execute("SELECT id FROM owners WHERE listing_id = ?", (listing_id,))
        result = c.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking owner fetch status: {e}")
        return False


def get_owner_details(listing_id: str) -> Optional[Dict]:
    """Get owner details for a listing.
    
    Args:
        listing_id: The listing ID to look up
        
    Returns:
        Dict with owner details or None if not found
    """
    try:
        conn = get_owners_db_connection()
        c = conn.cursor()
        c.execute(
            "SELECT owner_names, owner_phones, owner_emails, fetched_at FROM owners WHERE listing_id = ?", 
            (listing_id,)
        )
        row = c.fetchone()
        conn.close()

        if row:
            owner_names = deserialize_from_db(row[0], [])
            owner_phones = deserialize_from_db(row[1], [])
            owner_emails = deserialize_from_db(row[2], [])

            return {
                'names': owner_names,
                'phones': owner_phones,
                'emails': owner_emails,
                'fetched_at': row[3]
            }
        return None
    except Exception as e:
        logger.error(f"Error getting owner details: {e}")
        return None


def get_current_isoformat() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def init_owner_database() -> None:
    """Initialize the owners database with proper schema and indexes."""
    conn = get_owners_db_connection()
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS owners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT NOT NULL UNIQUE,
            rera TEXT,
            owner_names TEXT,
            owner_phones TEXT,
            owner_emails TEXT,
            fetched_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_owners_listing_id ON owners(listing_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_owners_rera ON owners(rera)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_owners_fetched_at ON owners(fetched_at)')
    
    conn.commit()
    conn.close()
    logger.info("Owners database initialized")

