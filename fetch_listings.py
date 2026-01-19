#!/usr/bin/env python3
"""
Fetch property listings for all agents in the database with pagination support.
"""

import requests
import sqlite3
import json
import time
import sys
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Progress tracking
total_listings_fetched = 0
total_agents_processed = 0
total_agents = 0


def update_progress(agents_done: int, listings: int):
    """Update progress display in place."""
    global total_listings_fetched, total_agents_processed
    total_listings_fetched += listings
    total_agents_processed = agents_done
    percent = (agents_done / total_agents * 100) if total_agents > 0 else 0
    sys.stdout.write(f"\rAgents: {agents_done}/{total_agents} ({percent:.1f}%) | Listings: {total_listings_fetched:,}   ")
    sys.stdout.flush()


def print_final_summary():
    """Print final summary."""
    sys.stdout.write("\n")
    sys.stdout.flush()
    print(f"Done! Processed {total_agents_processed}/{total_agents} agents, fetched {total_listings_fetched:,} listings")

# API Configuration - trying different endpoints
API_ENDPOINTS = [
    "https://www.propertyfinder.ae/api/pwa/property/search",
    "https://www.propertyfinder.ae/pf-b2c-customer/en/search",
    "https://www.propertyfinder.ae/en/search",
]

LISTINGS_DB = "listings.db"

# Global cache for table schema to avoid repeated PRAGMA calls
table_columns_cache = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Origin": "https://www.propertyfinder.ae",
    "Referer": "https://www.propertyfinder.ae/en/search",
    "Content-Type": "application/json",
}


def create_listings_table(conn: sqlite3.Connection) -> None:
    """Create the listings table if it doesn't exist."""
    # Create a minimal listings table. Additional columns will be added dynamically
    # to store each flattened field as a separate column.
    conn.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id TEXT PRIMARY KEY,
            agent_id INTEGER,
            page INTEGER,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER,
            page INTEGER,
            status TEXT,
            listings_count INTEGER,
            error_message TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()


def _flatten(value, parent_key='', sep='.'):
    """Recursively flatten dicts. Lists are JSON-encoded. Returns dict of flattened keys."""
    items = {}
    if isinstance(value, dict):
        for k, v in value.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(_flatten(v, new_key, sep=sep))
    elif isinstance(value, list):
        # store lists as JSON strings
        try:
            items[parent_key] = json.dumps(value, ensure_ascii=False)
        except Exception:
            items[parent_key] = str(value)
    else:
        # scalar
        items[parent_key] = value
    return items


def flatten_listing(listing: Dict, agent_id: int, page: int) -> Dict:
    """Flatten the full listing/property dict into key->value mapping.

    Nested keys are joined with dot (`.`). Lists are JSON-encoded strings.
    """
    prop = listing.get('property', listing)
    flat = _flatten(prop)
    # Add metadata columns
    flat['id'] = prop.get('id')
    flat['agent_id'] = agent_id
    flat['page'] = page
    return flat


def insert_listing(conn: sqlite3.Connection, listing: Dict) -> None:
    """Insert a listing into the database."""
    global table_columns_cache

    # Use cached schema if available
    if table_columns_cache is None:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(listings)")
        table_columns_cache = {row[1] for row in cur.fetchall()}

    existing = table_columns_cache

    # sanitize keys to valid column/param names: replace non-alphanumeric with '_'
    def _sanitize(k: str) -> str:
        return ''.join([c if (c.isalnum() or c == '_') else '_' for c in k])

    mapping = {k: _sanitize(k) for k in listing.keys()}

    # add missing columns
    for orig, safe in mapping.items():
        if safe not in existing:
            try:
                conn.execute(f"ALTER TABLE listings ADD COLUMN '{safe}' TEXT")
                existing.add(safe)
                table_columns_cache.add(safe)  # Update cache
            except Exception:
                pass  # Column already exists or other error

    # Build insert statement dynamically using sanitized names
    cols = [mapping[k] for k in listing.keys()]
    placeholders = ", ".join([f":{mapping[k]}" for k in listing.keys()])
    cols_sql = ", ".join([f"'{c}'" for c in cols])
    sql = f"INSERT OR REPLACE INTO listings ({cols_sql}) VALUES ({placeholders})"

    # Prepare params: convert dict/list to JSON strings
    params = {}
    for k, v in listing.items():
        sk = mapping[k]
        if isinstance(v, (dict, list)):
            try:
                params[sk] = json.dumps(v, ensure_ascii=False)
            except Exception:
                params[sk] = str(v)
        else:
            params[sk] = v

    conn.execute(sql, params)


def log_fetch(conn: sqlite3.Connection, agent_id: int, page: int, 
              status: str, listings_count: int = 0, error: str = None) -> None:
    """Log fetch attempt."""
    conn.execute('''
        INSERT INTO fetch_log (agent_id, page, status, listings_count, error_message)
        VALUES (?, ?, ?, ?, ?)
    ''', (agent_id, page, status, listings_count, error))


def fetch_listings_for_agent(
    session: requests.Session,
    agent_id: int,
    max_pages: int = 10,
    delay: float = 1.0
) -> List[Dict]:
    """Fetch all listings for a specific agent with pagination."""
    all_listings = []

    for page in range(1, max_pages + 1):
        params = {
            "page": page,
            "filters.agent_id": agent_id,
            "limit": 24,
        }

        success = False
        for endpoint in API_ENDPOINTS:
            try:
                resp = session.get(endpoint, params=params, headers=HEADERS, timeout=10)  # Reduced timeout

                if resp.status_code == 200:
                    data = resp.json()

                    # Try different response structures
                    listings = None
                    if isinstance(data, dict):
                        if 'listings' in data:
                            listings = data['listings']
                        elif 'data' in data:
                            listings = data['data']
                        elif 'properties' in data:
                            listings = data['properties']

                    if listings is None:
                        # Try to find any array in the response
                        for key, value in data.items():
                            if isinstance(value, list):
                                listings = value
                                break

                    if listings:
                        for listing in listings:
                            flat_listing = flatten_listing(listing, agent_id, page)
                            all_listings.append(flat_listing)

                        success = True
                        break

                    # No listings found, might be end of pagination
                    return all_listings

                elif resp.status_code == 500:
                    continue

                elif resp.status_code == 429:
                    # Rate limited - wait longer
                    time.sleep(5)  # Reduced wait time
                    continue

                else:
                    continue

            except Exception as e:
                continue

        if not success:
            break

        time.sleep(delay)

    return all_listings


def get_agent_ids_from_db(db_path: str = "agents.db") -> List[int]:
    """Get all agent IDs from the agents database."""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM agents")
        agent_ids = [row[0] for row in cur.fetchall()]
        conn.close()
        return agent_ids
    except Exception as e:
        return []


def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    s = requests.Session()
    
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])
    )
    
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    
    return s


def process_agent(agent_id: int, max_pages: int, delay: float) -> int:
    """Fetch listings for a single agent and save them to the DB. Returns number saved."""
    session = create_session()
    try:
        listings = fetch_listings_for_agent(session, agent_id, max_pages=max_pages, delay=delay)
    except Exception as e:
        return 0

    if not listings:
        return 0

    conn = sqlite3.connect(LISTINGS_DB)
    saved = 0
    try:
        for listing in listings:
            try:
                insert_listing(conn, listing)
                saved += 1
            except Exception:
                pass  # Skip failed listings
        conn.commit()
    finally:
        conn.close()

    return saved


def main():
    global total_agents
    
    import argparse
    parser = argparse.ArgumentParser(description='Fetch property listings')
    parser.add_argument('--agents-db', default='agents.db')
    parser.add_argument('--workers', type=int, default=20)
    parser.add_argument('--delay', type=float, default=0.5)  # Reduced default delay
    parser.add_argument('--max-pages', type=int, default=5)
    parser.add_argument('--agent-ids', type=str, help='Comma-separated agent IDs')
    args = parser.parse_args()
    
    # Get agent IDs
    if args.agent_ids:
        agent_ids = [int(x.strip()) for x in args.agent_ids.split(',')]
    else:
        agent_ids = get_agent_ids_from_db(args.agents_db)

    if not agent_ids:
        return
    
    total_agents = len(agent_ids)
    processed = 0
    update_progress(0, 0)
    
    # Create listings database
    conn = sqlite3.connect(LISTINGS_DB)
    conn.execute("PRAGMA synchronous = OFF")  # Speed up SQLite
    conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
    conn.execute("PRAGMA cache_size = 1000000")  # 1GB cache
    create_listings_table(conn)
    conn.close()
    
    # Use ThreadPoolExecutor to process agents in parallel
    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = {}
        for agent_id in agent_ids:
            futures[exe.submit(process_agent, agent_id, args.max_pages, args.delay)] = agent_id
        for fut in as_completed(futures):
            agent_id = futures[fut]
            try:
                saved = fut.result()
                processed += 1
                update_progress(processed, saved)
            except Exception as e:
                processed += 1
                update_progress(processed, 0)

    print_final_summary()


if __name__ == '__main__':
    main()

