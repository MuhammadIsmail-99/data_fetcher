#!/usr/bin/env python3
import requests
import threading
import sqlite3
import json
import time
import queue
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

API_URL = "https://www.propertyfinder.ae/pf-b2c-customer/en/agents"

# Default API parameters
DEFAULT_PARAMS = {
    "per_page": 24,
    "is_superagent": "true",
    "location_ids": "",
    "category_id": "",
    "sort": "-trusted_score",
    "force_min_count": "true",
    "force_ranking_control": "true",
}




def requests_session(retries=5, backoff_factor=1.0, status_forcelist=(500,502,503,504)):
    s = requests.Session()

    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist, allowed_methods=frozenset(["GET"]), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Origin": "https://www.propertyfinder.ae",
        "Referer": "https://www.propertyfinder.ae/en/agents"
    })
    return s


def create_table(conn):
    conn.execute('''
    CREATE TABLE IF NOT EXISTS agents (
        id INTEGER PRIMARY KEY,
        slug TEXT,
        name TEXT,
        email TEXT,
        phone TEXT,
        whatsappPhone TEXT,
        userId INTEGER,
        superagent INTEGER,
        verified INTEGER,
        totalProperties INTEGER,
        propertiesResidentialForRentCount INTEGER,
        propertiesResidentialForSaleCount INTEGER,
        propertiesCommercialForRentCount INTEGER,
        propertiesCommercialForSaleCount INTEGER,
        avgWhatsappResponseTime INTEGER,
        experienceSince INTEGER,
        position TEXT,
        bio TEXT,
        ranking INTEGER,
        isTransactionsVisible INTEGER,
        transactionsCount INTEGER,
        listingLevel INTEGER,
        averageRating REAL,
        reviewCount INTEGER,
        medianListingQuality INTEGER,
        licenseNumber TEXT,
        languages_json TEXT,
        topLocations_json TEXT,
        image_json TEXT,
        broker_json TEXT,
        compliances_json TEXT,
        claimedTransactions_json TEXT
    );
    ''')
    conn.commit()


def flatten_agent(agent):
    # Extract top-level scalar fields and JSON-encode nested ones
    row = {
        'id': agent.get('id'),
        'slug': agent.get('slug'),
        'name': agent.get('name'),
        'email': agent.get('email'),
        'phone': agent.get('phone'),
        'whatsappPhone': agent.get('whatsappPhone'),
        'userId': agent.get('userId'),
        'superagent': 1 if agent.get('superagent') else 0,
        'verified': 1 if agent.get('verified') else 0,
        'totalProperties': agent.get('totalProperties'),
        'propertiesResidentialForRentCount': agent.get('propertiesResidentialForRentCount'),
        'propertiesResidentialForSaleCount': agent.get('propertiesResidentialForSaleCount'),
        'propertiesCommercialForRentCount': agent.get('propertiesCommercialForRentCount'),
        'propertiesCommercialForSaleCount': agent.get('propertiesCommercialForSaleCount'),
        'avgWhatsappResponseTime': agent.get('avgWhatsappResponseTime'),
        'experienceSince': agent.get('experienceSince'),
        'position': agent.get('position'),
        'bio': agent.get('bio'),
        'ranking': agent.get('ranking'),
        'isTransactionsVisible': 1 if agent.get('isTransactionsVisible') else 0,
        'transactionsCount': agent.get('transactionsCount'),
        'listingLevel': agent.get('listingLevel'),
        'averageRating': agent.get('averageRating'),
        'reviewCount': agent.get('reviewCount'),
        'medianListingQuality': agent.get('medianListingQuality') if agent.get('medianListingQuality') is not None else agent.get('medianListingQuality'),
        'licenseNumber': agent.get('licenseNumber'),
        'languages_json': json.dumps(agent.get('languages', []), ensure_ascii=False),
        'topLocations_json': json.dumps(agent.get('topLocations', []), ensure_ascii=False),
        'image_json': json.dumps(agent.get('image', {}), ensure_ascii=False),
        'broker_json': json.dumps(agent.get('broker', {}), ensure_ascii=False),
        'compliances_json': json.dumps(agent.get('compliances', []), ensure_ascii=False),
        'claimedTransactions_json': json.dumps(agent.get('claimedTransactionsList', []), ensure_ascii=False)
    }
    return row


def db_writer(db_path, q, stop_event):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    create_table(conn)
    insert_sql = '''INSERT OR REPLACE INTO agents(
        id, slug, name, email, phone, whatsappPhone, userId, superagent, verified,
        totalProperties, propertiesResidentialForRentCount, propertiesResidentialForSaleCount,
        propertiesCommercialForRentCount, propertiesCommercialForSaleCount, avgWhatsappResponseTime,
        experienceSince, position, bio, ranking, isTransactionsVisible, transactionsCount, listingLevel,
        averageRating, reviewCount, medianListingQuality, licenseNumber, languages_json, topLocations_json,
        image_json, broker_json, compliances_json, claimedTransactions_json
    ) VALUES (
        :id, :slug, :name, :email, :phone, :whatsappPhone, :userId, :superagent, :verified,
        :totalProperties, :propertiesResidentialForRentCount, :propertiesResidentialForSaleCount,
        :propertiesCommercialForRentCount, :propertiesCommercialForSaleCount, :avgWhatsappResponseTime,
        :experienceSince, :position, :bio, :ranking, :isTransactionsVisible, :transactionsCount, :listingLevel,
        :averageRating, :reviewCount, :medianListingQuality, :licenseNumber, :languages_json, :topLocations_json,
        :image_json, :broker_json, :compliances_json, :claimedTransactions_json
    )'''

    cur = conn.cursor()
    inserted = 0
    try:
        while True:
            item = q.get()
            if item is None:
                break
            try:
                cur.execute(insert_sql, item)
                inserted += 1
                if inserted % 100 == 0:
                    conn.commit()
            except Exception:
                logging.exception('DB insert failed for id=%s', item.get('id'))
        conn.commit()
    finally:
        conn.close()
        stop_event.set()
        logging.info('DB writer finished, inserted %d rows', inserted)


def fetch_page(session, page, max_retries=5):
    params = {**DEFAULT_PARAMS, 'page': page}
    base_delay = 2.0  # Start with 2 seconds
    
    for attempt in range(max_retries):
        try:
            resp = session.get(API_URL, params=params, timeout=60)
            
            # Handle 500 errors with exponential backoff
            if resp.status_code >= 500:
                delay = base_delay * (2 ** attempt) + (hash(page) % 1000) / 1000.0
                logging.warning('Page %d got HTTP %d, attempt %d/%d, retrying in %.1fs', 
                              page, resp.status_code, attempt + 1, max_retries, delay)
                time.sleep(delay)
                continue
                
            # Handle 429 (rate limit) with longer backoff
            if resp.status_code == 429:
                delay = base_delay * (2 ** attempt) * 2 + (hash(page) % 2000) / 1000.0
                logging.warning('Page %d got HTTP 429 (rate limited), attempt %d/%d, retrying in %.1fs',
                              page, attempt + 1, max_retries, delay)
                time.sleep(delay)
                continue
                
            # Handle 4xx errors (except 429) - don't retry
            if 400 <= resp.status_code < 500:
                logging.error('Page %d got HTTP %d client error, skipping', page, resp.status_code)
                return None
                
            resp.raise_for_status()
            return resp
            
        except requests.exceptions.Timeout:
            logging.warning('Page %d timeout on attempt %d/%d', page, attempt + 1, max_retries)
            time.sleep(base_delay * (2 ** attempt))
        except requests.exceptions.ConnectionError as e:
            logging.warning('Page %d connection error on attempt %d/%d: %s', page, attempt + 1, max_retries, e)
            time.sleep(base_delay * (2 ** attempt))
        except Exception:
            logging.exception('Request failed for page %s on attempt %d/%d', page, attempt + 1, max_retries)
            time.sleep(base_delay * (2 ** attempt))
    
    logging.error('Page %d failed after %d retries', page, max_retries)
    return None


def worker_loop(session, q, get_next_page, stop_event, min_delay=0.5):
    last_request_time = 0
    
    while not stop_event.is_set():
        page = get_next_page()
        if page is None:
            break
            
        # Rate limiting: ensure minimum delay between requests
        current_time = time.time()
        time_since_last = current_time - last_request_time
        if time_since_last < min_delay:
            time.sleep(min_delay - time_since_last)
        
        last_request_time = time.time()
        resp = fetch_page(session, page)
        if resp is None:
            # retry logic handled by fetch_page with backoff
            continue
        try:
            data = resp.json()
        except Exception:
            logging.exception('Invalid JSON for page %s', page)
            continue
        # Expecting either a list or an object containing list
        if isinstance(data, dict):
            # common wrappers
            for key in ('data', 'items', 'results'):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                # unknown format
                logging.warning('Unexpected JSON format on page %s', page)
                data = []
        if not isinstance(data, list):
            logging.warning('Page %s did not return a list', page)
            data = []
        if len(data) == 0:
            logging.info('Page %s returned empty list, stopping.', page)
            stop_event.set()
            break
        for agent in data:
            q.put(flatten_agent(agent))
        logging.info('Fetched page %s with %d agents', page, len(data))


def main():
    parser = argparse.ArgumentParser(description='Fetch all agents and save to SQLite DB')
    parser.add_argument('--db', default='agents.db', help='SQLite DB path')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers (5-10 recommended to avoid 500 errors)')
    parser.add_argument('--delay', type=float, default=0.5, help='Minimum delay between requests per worker (seconds)')
    parser.add_argument('--dry-run', action='store_true', help='Only fetch page 1 and exit (verify)')
    args = parser.parse_args()

    if args.workers < 1:
        args.workers = 1

    session = requests_session()

    # Do initial fetch page 1
    resp = fetch_page(session, 1)
    if resp is None:
        logging.error('Failed to fetch page 1; exiting')
        return
    try:
        data = resp.json()
    except Exception:
        logging.exception('Failed to parse JSON from page 1')
        return
    if isinstance(data, dict):
        for key in ('data', 'items', 'results'):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
    if not isinstance(data, list):
        logging.error('Page 1 did not return a list; unexpected format')
        return

    q = queue.Queue(maxsize=10000)
    stop_event = threading.Event()

    # start DB writer
    writer_thread = threading.Thread(target=db_writer, args=(args.db, q, stop_event), daemon=True)
    writer_thread.start()

    # push page 1 results
    for agent in data:
        q.put(flatten_agent(agent))
    logging.info('Page 1 contains %d agents', len(data))

    if args.dry_run:
        # signal writer to finish
        q.put(None)
        # wait for writer
        stop_event.wait()
        logging.info('Dry run complete. DB written to %s', args.db)
        return

    # coordinated page counter
    counter = {'next_page': 2}
    counter_lock = threading.Lock()

    def get_next_page():
        with counter_lock:
            if stop_event.is_set():
                return None
            p = counter['next_page']
            counter['next_page'] += 1
            return p

    # start worker threads
    workers = min(args.workers, 20)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = []
        for _ in range(workers):
            futures.append(ex.submit(worker_loop, session, q, get_next_page, stop_event, args.delay))
        # wait for all workers to finish
        for fut in futures:
            try:
                fut.result()
            except Exception:
                logging.exception('Worker error')

    # signal writer to finish
    q.put(None)
    # wait for writer thread to finish
    stop_event.wait()
    logging.info('All done. DB saved at %s', args.db)


if __name__ == '__main__':
    main()
