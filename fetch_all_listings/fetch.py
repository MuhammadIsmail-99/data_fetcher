import streamlit as st
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from datetime import datetime
import sqlite3
from pathlib import Path
import random

# Page config
st.set_page_config(page_title="Property Finder Scraper", page_icon="🏠", layout="wide")

st.title("🏠 Property Finder Scraper")
st.markdown("Fetch all property listings with parallel processing and save to SQLite")

# Sidebar configuration
st.sidebar.header("⚙️ Configuration")
num_workers = st.sidebar.slider("Number of Workers", min_value=1, max_value=40, value=5, 
                                help="Number of parallel threads for fetching")
request_delay = st.sidebar.number_input("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.1, step=0.1,
                                        help="Small delay to avoid rate limiting")
max_retries = st.sidebar.number_input("Max retries per page", min_value=0, max_value=10, value=3,
                                      help="Number of retry attempts for failed requests")
location_id = st.sidebar.text_input("Location ID", value="1", help="Filter by location ID")
limit_per_page = st.sidebar.number_input("Listings per page", min_value=1, max_value=100, value=100)

# Database file
DB_FILE = "raw_listings.db"

# API endpoint
base_url = "https://www.propertyfinder.ae/api/pwa/property/search"


def init_database():
    """Initialize SQLite database with listings table"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_number INTEGER NOT NULL,
            location_id TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(page_number, location_id)
        )
    ''')
    
    conn.commit()
    conn.close()


def save_page_to_db(page_num, location_id, json_data):
    """Save raw JSON data for a page to database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO listings (page_number, location_id, raw_json, fetched_at)
            VALUES (?, ?, ?, ?)
        ''', (page_num, location_id, json.dumps(json_data), datetime.now().isoformat()))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Database error on page {page_num}: {str(e)}")
        return False
    finally:
        conn.close()


def fetch_page(page_num, location_id, limit, retry_count=0):
    """Fetch a single page of listings with retry logic"""
    params = {
        'pagination.page': page_num,
        'limit': limit,
        'filters.location_id': location_id
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract listings
        listings = data.get('listings', [])
        total_count = data.get('meta', {}).get('total_count', 0)
        
        # Save raw JSON to database
        save_success = save_page_to_db(page_num, location_id, data)
        
        return {
            'page': page_num,
            'listings': listings,
            'total_count': total_count,
            'success': True,
            'db_saved': save_success
        }
    except Exception as e:
        # Retry logic for network errors
        error_str = str(e)
        if retry_count < max_retries and any(x in error_str for x in ['NameResolutionError', 'ConnectionError', 'Max retries exceeded', 'timed out', 'Connection reset']):
            wait_time = (2 ** retry_count) + random.uniform(0, 1)  # Exponential backoff with jitter
            time.sleep(wait_time)
            return fetch_page(page_num, location_id, limit, retry_count + 1)
        
        return {
            'page': page_num,
            'listings': [],
            'total_count': 0,
            'success': False,
            'error': str(e),
            'db_saved': False,
            'retries': retry_count
        }


def fetch_first_page(location_id, limit):
    """Fetch first page to determine total pages"""
    result = fetch_page(1, location_id, limit)
    if result['success'] and result['total_count'] > 0:
        total_pages = (result['total_count'] + limit - 1) // limit
        return result, total_pages
    return result, 0


def get_database_stats():
    """Get statistics from database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM listings')
    total_pages = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT location_id) FROM listings')
    total_locations = cursor.fetchone()[0]
    
    conn.close()
    
    return total_pages, total_locations


# Initialize database
init_database()

# Display database stats
if Path(DB_FILE).exists():
    try:
        total_pages_db, total_locations_db = get_database_stats()
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Database Stats")
        st.sidebar.metric("Pages Stored", total_pages_db)
        st.sidebar.metric("Locations", total_locations_db)
    except:
        pass

# Main scraping logic
if st.button("🚀 Start Scraping", type="primary", use_container_width=True):
    
    # Create placeholder for progress
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    
    # Initialize
    all_listings = []
    start_time = time.time()
    
    # Fetch first page to get total count
    with st.spinner("Fetching first page to determine total pages..."):
        first_result, total_pages = fetch_first_page(location_id, limit_per_page)
    
    if not first_result['success']:
        st.error(f"❌ Failed to fetch first page: {first_result.get('error', 'Unknown error')}")
        st.stop()
    
    if total_pages == 0:
        st.warning("⚠️ No listings found")
        st.stop()
    
    # Add first page listings
    all_listings.extend(first_result['listings'])
    
    st.info(f"📊 Total listings: {first_result['total_count']} | Total pages: {total_pages}")
    
    # If only one page, we're done
    if total_pages == 1:
        progress_placeholder.success(f"**Page 1/{total_pages}** | Fetched **{len(all_listings)}** listings")
    else:
        # Fetch remaining pages in parallel
        pages_to_fetch = list(range(2, total_pages + 1))
        completed_pages = 1
        failed_pages = []
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_page = {
                executor.submit(fetch_page, page, location_id, limit_per_page): page 
                for page in pages_to_fetch
            }
            
            # Process completed tasks
            for future in as_completed(future_to_page):
                result = future.result()
                
                if result['success']:
                    all_listings.extend(result['listings'])
                    completed_pages += 1
                    
                    if not result.get('db_saved', False):
                        status_placeholder.warning(f"⚠️ Page {result['page']} fetched but not saved to DB")
                else:
                    failed_pages.append(result['page'])
                    st.warning(f"⚠️ Failed to fetch page {result['page']}: {result.get('error', 'Unknown error')}")
                    completed_pages += 1
                
                # Update progress in place
                progress_placeholder.info(
                    f"**Page {completed_pages}/{total_pages}** | Fetched **{len(all_listings)}** listings"
                )
    
    # Final results
    elapsed_time = time.time() - start_time
    
    st.success(f"✅ Scraping completed in {elapsed_time:.2f} seconds!")
    
    # Display results
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Listings", len(all_listings))
    with col2:
        st.metric("Total Pages", total_pages)
    with col3:
        st.metric("Completed Pages", completed_pages)
    with col4:
        st.metric("Failed Pages", len(failed_pages) if 'failed_pages' in locals() else 0)
    
    # Show failed pages if any
    if 'failed_pages' in locals() and failed_pages:
        st.error(f"Failed pages: {', '.join(map(str, failed_pages))}")
    
    # Database info
    st.markdown("---")
    st.subheader("💾 Database Information")
    st.info(f"Data saved to: `{DB_FILE}`")
    
    total_pages_db, total_locations_db = get_database_stats()
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Pages in DB", total_pages_db)
    with col2:
        st.metric("Total Locations in DB", total_locations_db)
    
    # Show sample listing
    if all_listings:
        st.markdown("---")
        st.subheader("📄 Sample Listing")
        with st.expander("View first listing"):
            st.json(all_listings[0])

# Display query interface
st.markdown("---")
st.subheader("🔍 Query Database")

if st.button("Show All Pages in Database"):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT page_number, location_id, fetched_at 
        FROM listings 
        ORDER BY page_number
    ''')
    
    results = cursor.fetchall()
    conn.close()
    
    if results:
        st.write(f"Found {len(results)} pages in database:")
        for page_num, loc_id, fetched_at in results[:20]:  # Show first 20
            st.text(f"Page {page_num} | Location ID: {loc_id} | Fetched: {fetched_at}")
        
        if len(results) > 20:
            st.info(f"... and {len(results) - 20} more pages")
    else:
        st.warning("No data in database yet")