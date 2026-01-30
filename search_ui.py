#!/usr/bin/env python3
"""
Streamlit UI for Property Listings Search with Owner Details.
"""

import asyncio
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from utils import (
    get_db_connection,
    get_owners_db_connection,
    RateLimiter,
    setup_logging,
    validate_rera,
    LISTINGS_DIR
)

# Import owner_fetcher functions
try:
    from owner_fetcher import (
        fetch_owner_for_rera,
        update_db_with_owner_details,
        get_owner_details,
        has_owner_details_fetched,
        get_current_isoformat,
        SESSION_FILE,
        init_owner_database
    )
except ImportError as e:
    st.error(f"Error importing owner_fetcher: {e}")
    st.stop()

# Setup logging
logger = setup_logging("search_ui")

# Initialize session state for persistent notifications
if 'notifications' not in st.session_state:
    st.session_state.notifications = []

# Page config
st.set_page_config(
    page_title="Property Listings Search",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define residential and commercial property types
RESIDENTIAL_TYPES = [
    'Apartment', 'Villa', 'Townhouse', 'Penthouse', 'Bungalow', 
    'Duplex', 'Compound', 'Farm', 'Full Floor', 'Half Floor',
    'Bulk Rent Unit', 'Bulk Sale Unit'
]

COMMERCIAL_TYPES = [
    'Office Space', 'Retail', 'Shop', 'Show Room', 'Business Centre',
    'Warehouse', 'Factory', 'Hotel & Hotel Apartment', 'Labor Camp',
    'Staff Accommodation', 'Whole Building', 'Co-working space'
]

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Telegram credentials from environment
TELETHON_APP_ID = int(os.environ.get('TELETHON_APP_ID', '0'))
TELETHON_API_HASH = os.environ.get('TELETHON_API_HASH', '')
TELEGRAM_BOT_USER = os.environ.get('TELEGRAM_BOT_USER', '@AtlasDubaiBot')

# Database path
DB_PATH = os.path.join(LISTINGS_DIR, 'listings.db')


# =============================================================================
# Database Utilities (using utils)
# =============================================================================

def get_db_connection_ui() -> sqlite3.Connection:
    """Get database connection for UI."""
    return get_db_connection(DB_PATH)


def get_owners_db_connection_ui() -> sqlite3.Connection:
    """Get owners database connection for UI."""
    return get_owners_db_connection()


# =============================================================================
# Cached Data Functions
# =============================================================================

@st.cache_data(ttl=3600)
def get_total_count() -> int:
    """Get total count of listings without loading all data."""
    conn = get_db_connection_ui()
    result = conn.execute("SELECT COUNT(*) FROM listings").fetchone()
    conn.close()
    return result[0] if result else 0


@st.cache_data(ttl=300)
def search_listings(search_query: str, search_field: str, limit: int = 100) -> pd.DataFrame:
    """Fast filtered search - only loads what's needed."""
    conn = get_db_connection_ui()
    query = f"""
    SELECT id, title, property_type, price_value, price_currency,
           bedrooms, bathrooms, location_name, broker_name, listed_date, rera, share_url,
           offering_type
    FROM listings
    WHERE {search_field} LIKE ?
    LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(f"%{search_query}%", limit))
    conn.close()

    # Add owner information from owners database
    owners_conn = get_owners_db_connection_ui()
    owners_df = pd.read_sql_query("""
        SELECT listing_id, owner_names, owner_phones, owner_emails, property_number, fetched_at as owner_fetched_at
        FROM owners
    """, owners_conn)
    owners_conn.close()

    # Merge owner data with listings
    df = df.merge(owners_df, left_on='id', right_on='listing_id', how='left')
    return df


@st.cache_data(ttl=3600)
def get_filter_options() -> dict:
    """Get filter options on demand."""
    conn = get_db_connection_ui()
    property_types = conn.execute(
        "SELECT DISTINCT property_type FROM listings WHERE property_type IS NOT NULL ORDER BY property_type"
    ).fetchall()
    locations = conn.execute(
        "SELECT DISTINCT location_name FROM listings WHERE location_name IS NOT NULL ORDER BY location_name"
    ).fetchall()
    price_max = conn.execute(
        "SELECT MAX(price_value) FROM listings WHERE price_value IS NOT NULL"
    ).fetchone()
    
    conn.close()
    
    return {
        'property_types': [p[0] for p in property_types],
        'locations': [l[0] for l in locations],
        'price_max': price_max[0] if price_max[0] else 0
    }


# =============================================================================
# Owner Fetching Functions
# =============================================================================

async def fetch_owner_for_single(listing_id: str, rera: str) -> bool:
    """Fetch owner details for a single listing."""
    if not TELETHON_APP_ID or not TELETHON_API_HASH:
        st.error("Telegram credentials not configured. Set TELETHON_APP_ID and TELETHON_API_HASH in .env")
        return False

    try:
        st.info(f"Fetching owner details for RERA {rera}...")
        result = await fetch_owner_for_rera(rera, TELEGRAM_BOT_USER, TELETHON_APP_ID, TELETHON_API_HASH)
        logger.debug(f"Fetch result for RERA {rera}: {result}")

        if result['status'] == 'success':
            # Update database
            try:
                logger.info(f"Updating DB for RERA {rera} with owner data")
                success = update_db_with_owner_details(
                    listing_id,
                    rera,
                    result.get('owner_names', []),
                    result.get('owner_phones', []),
                    result.get('owner_emails', []),
                    result.get('property_number', '')
                )
                logger.info(f"DB update result: {success}")
            except Exception as db_error:
                logger.error(f"Database error for RERA {rera}: {db_error}", exc_info=True)
                add_notification(f"Database error: {str(db_error)}", "error")
                return False

            if success:
                add_notification(f"Owner details fetched for RERA {rera}", "success")
                return True
            else:
                add_notification(f"Database update failed for RERA {rera}", "error")
                return False
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"Error fetching owner for RERA {rera}: {error_msg}")
            add_notification(f"Error: {error_msg}", "error")
            return False
    except Exception as e:
        logger.error(f"Exception fetching owner for RERA {rera}: {e}", exc_info=True)
        st.error(f"Error fetching owner: {str(e)}")
        return False


def fetch_owner_sync(listing_id: str, rera: str) -> bool:
    """Synchronous wrapper for fetch_owner_for_single."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(fetch_owner_for_single(listing_id, rera))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"Error in sync wrapper: {e}", exc_info=True)
        st.error(f"❌ Error: {str(e)}")
        return False


# =============================================================================
# UI Components
# =============================================================================

def display_owner_details(row: pd.Series) -> None:
    """Display owner details for a listing."""
    owner_fetched = pd.notna(row.get('owner_fetched_at'))

    if owner_fetched:
        try:
            owner_names = json.loads(row['owner_names']) if row.get('owner_names') else []
            owner_phones = json.loads(row['owner_phones']) if row.get('owner_phones') else []
            owner_emails = json.loads(row['owner_emails']) if row.get('owner_emails') else []
        except (json.JSONDecodeError, TypeError):
            owner_names = []
            owner_phones = []
            owner_emails = []

        if owner_names or owner_phones or owner_emails:
            st.write("**Owner Details:**")
            if owner_names:
                st.write(f"**Name:** {', '.join(owner_names)}")
            if owner_phones:
                st.write(f"**Phone:** {', '.join(owner_phones)}")
            if owner_emails:
                st.write(f"**Email:** {', '.join(owner_emails)}")
        else:
            st.info("No owner details found")

        # Display property number if available
        property_number = row.get('property_number')
        if pd.notna(property_number) and property_number:
            st.write(f"**Property No:** {property_number}")
    else:
        st.info("Owner details not fetched yet")


def apply_filters(
    df: pd.DataFrame,
    price_range: tuple,
    property_types: list,
    bedrooms: tuple,
    locations: list,
    selected_categories: list = None,
    selected_types: list = None
) -> pd.DataFrame:
    """Apply filters to the dataframe."""
    if df is None or len(df) == 0:
        return df
    
    # Category filter (Buy/Rent/Commercial) - based on offering_type
    if selected_categories and "All" not in selected_categories:
        def matches_category(row):
            offering = row.get('offering_type', '') or ''
            if 'Rent' in selected_categories and 'for Rent' in offering:
                return True
            if 'Buy' in selected_categories and 'for Sale' in offering:
                return True
            if 'Commercial Rent' in selected_categories and offering == 'Commercial for Rent':
                return True
            if 'Commercial Buy' in selected_categories and offering == 'Commercial for Sale':
                return True
            return False
        df = df[df.apply(matches_category, axis=1)]
    
    # Property Type filter (Residential/Commercial) - based on property_type
    if selected_types and "All" not in selected_types:
        def matches_type_category(row):
            prop_type = row.get('property_type', '') or ''
            if 'Residential' in selected_types and prop_type in RESIDENTIAL_TYPES:
                return True
            if 'Commercial' in selected_types and prop_type in COMMERCIAL_TYPES:
                return True
            return False
        df = df[df.apply(matches_type_category, axis=1)]
    
    # Price filter
    if price_range:
        df = df[
            (df['price_value'] >= price_range[0]) & 
            (df['price_value'] <= price_range[1])
        ]
    
    # Property type filter
    if property_types:
        df = df[df['property_type'].isin(property_types)]
    
    # Bedrooms filter
    if bedrooms:
        df = df[
            (pd.to_numeric(df['bedrooms'], errors='coerce').fillna(0) >= bedrooms[0]) &
            (pd.to_numeric(df['bedrooms'], errors='coerce').fillna(0) <= bedrooms[1])
        ]
    
    # Location filter
    if locations:
        df = df[df['location_name'].isin(locations)]
    
    return df


# =============================================================================
# Notification Functions
# =============================================================================

def add_notification(message: str, type: str = "info"):
    """Add a notification that auto-dismisses after 5 seconds."""
    if type == "success":
        st.toast(message, icon="✅", duration=5)
    elif type == "error":
        st.toast(message, icon="❌", duration=5)
    elif type == "warning":
        st.toast(message, icon="⚠️", duration=5)
    else:
        st.toast(message, duration=5)

def display_notifications():
    """Display notifications (now handled by toast, kept for compatibility)."""
    pass

# =============================================================================
# Main Application
# =============================================================================

def main():
    """Main application function."""
    # Initialize owners database
    init_owner_database()

    # Display persistent notifications
    display_notifications()

    # Title
    st.title("Property Listings Search")

    # Get total count
    total_listings = get_total_count()

    # Search controls - search bar on right (70% width), dropdown in sidebar
    col1, col2 = st.columns([3, 7])  # 30% for left space, 70% for search bar

    with col2:
        search_query = st.text_input(
            "Search",
            placeholder="Type to search listings...",
            help="Type to search in real-time",
            label_visibility="visible"
        )

    # Sidebar for "Search in" dropdown and filters
    st.sidebar.metric("Total Listings", total_listings)
    st.sidebar.subheader("Search")
    search_field = st.sidebar.selectbox(
        "Search in",
        ["location_name", "title", "property_type", "broker_name"],
        index=0
    )

    # Only load filter options if user interacts
    st.sidebar.subheader("Filters")

    filter_options = get_filter_options()

    # Category filter (Buy/Rent) - first filter
    st.sidebar.write("**Category**")
    category_options = ["All", "Buy", "Rent", "Commercial Buy", "Commercial Rent"]
    selected_categories = st.sidebar.multiselect(
        "Category",
        category_options,
        default=["All"],
        help="Select Buy, Rent, or Commercial categories"
    )

    # Property Type filter (Residential/Commercial) - second filter
    st.sidebar.write("**Property Type**")
    type_options = ["All", "Residential", "Commercial"]
    selected_types = st.sidebar.multiselect(
        "Property Type",
        type_options,
        default=["All"],
        help="Select Residential or Commercial property types"
    )

    price_range = st.sidebar.slider(
        "Price Range",
        min_value=0,
        max_value=int(filter_options['price_max']),
        value=(0, int(filter_options['price_max'])),
    )

    property_types = st.sidebar.multiselect(
        "Specific Property Type",
        filter_options['property_types'],
        help="Select one or more property types"
    )

    bedrooms = st.sidebar.slider(
        "Bedrooms",
        min_value=0,
        max_value=10,
        value=(0, 10)
    )

    locations = st.sidebar.multiselect(
        "Location",
        filter_options['locations'],
        help="Select one or more locations"
    )

    # Apply search
    st.subheader("Search Results")

    if search_query:
        # Only search when user types something
        try:
            results_df = search_listings(search_query, search_field)
            
            # Apply additional filters to results
            results_df = apply_filters(results_df, price_range, property_types, bedrooms, locations, selected_categories, selected_types)
            
            if results_df is not None and len(results_df) > 0:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.metric("Results Found", len(results_df))
                
                # Batch fetch button
                with col2:
                    # Use placeholders for button and progress bar
                    button_placeholder = st.empty()
                    progress_placeholder = st.empty()

                    if button_placeholder.button("Fetch All Owner Details", key="fetch_all_btn"):
                        # Only fetch for listings that don't have owner details fetched AND have a RERA number
                        unfetched = results_df[
                            (results_df['owner_fetched_at'].isna()) &
                            (results_df['rera'].notna()) &
                            (results_df['rera'] != '')
                        ]
                        if len(unfetched) > 0:
                            # Hide button and show progress bar
                            button_placeholder.empty()
                            progress_bar = progress_placeholder.progress(0)
                            status_text = progress_placeholder.empty()

                            success_count = 0
                            total_count = len(unfetched)

                            for idx, (_, row) in enumerate(unfetched.iterrows()):
                                if fetch_owner_sync(row['id'], row['rera']):
                                    success_count += 1

                                # Update progress bar
                                progress = (idx + 1) / total_count
                                progress_bar.progress(progress)
                                status_text.text(f"Fetching owner details... ({idx+1}/{total_count})")

                            st.cache_data.clear()  # Clear cache after batch fetch

                            # Clear progress elements and show success
                            progress_placeholder.empty()
                            button_placeholder.success(f"✅ Completed: {success_count}/{total_count} owner details fetched")
                        else:
                            button_placeholder.info("All listings already have owner details fetched!")
                
                if len(results_df) > 0:
                    # Pagination setup
                    items_per_page = 10
                    total_pages = (len(results_df) + items_per_page - 1) // items_per_page

                    # Initialize page in session state
                    if 'page' not in st.session_state:
                        st.session_state.page = 1

                    # Page controls
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col1:
                        if st.button("⬅️ Previous", disabled=st.session_state.page <= 1):
                            st.session_state.page -= 1
                            st.rerun()
                    with col2:
                        page_options = list(range(1, total_pages + 1))
                        selected_page = st.selectbox(
                            f"Page {st.session_state.page} of {total_pages}",
                            options=page_options,
                            index=st.session_state.page - 1,
                            key="page_selector",
                            label_visibility="collapsed"
                        )
                        if selected_page != st.session_state.page:
                            st.session_state.page = selected_page
                            st.rerun()
                    with col3:
                        if st.button("Next ➡️", disabled=st.session_state.page >= total_pages):
                            st.session_state.page += 1
                            st.rerun()

                    # Get current page data
                    start_idx = (st.session_state.page - 1) * items_per_page
                    end_idx = min(start_idx + items_per_page, len(results_df))
                    page_df = results_df.iloc[start_idx:end_idx]

                    # Display results with owner info
                    st.subheader(f"🏠 Property Listings (Showing {start_idx + 1}-{end_idx} of {len(results_df)})")

                    for idx, row in page_df.iterrows():
                        with st.container(border=True):
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.write(f"**{row['title']}** ({row['property_type']})")
                                st.write(f"{row['location_name']}")
                                st.write(f"{row['price_value']:,.0f} {row['price_currency']}")
                                st.write(f"{row['bedrooms']} beds | {row['bathrooms']} baths")
                                if row.get('rera'):
                                    st.write(f"RERA: {row['rera']}")
                            
                            with col2:
                                # Owner details section
                                display_owner_details(row)
                            
                            with col3:
                                # Fetch button for individual listing
                                owner_fetched = pd.notna(row.get('owner_fetched_at'))
                                if not owner_fetched and row.get('rera'):
                                    if st.button("Fetch Owner", key=f"fetch_{row['id']}", use_container_width=True):
                                        with st.spinner(f"Fetching for RERA {row['rera']}..."):
                                            fetch_owner_sync(row['id'], row['rera'])
                                        st.cache_data.clear()
                                        st.rerun()
                                elif owner_fetched:
                                    st.write("Done")
                    
                    st.divider()
                    
                    # Download option
                    display_df = results_df[[
                        'id', 'title', 'rera', 'property_type', 'price_value',
                        'bedrooms', 'bathrooms', 'location_name', 'owner_names', 'owner_phones', 'owner_emails', 'property_number', 'share_url'
                    ]].copy()
                    display_df.columns = [
                        'ID', 'Title', 'RERA', 'Type', 'Price',
                        'Beds', 'Baths', 'Location', 'Owner Name', 'Owner Phone', 'Owner Emails', 'Property Number', 'Share URL'
                    ]
                    
                    csv = display_df.to_csv(index=False)

                    # Get the most common location name for the filename
                    if len(results_df) > 0 and 'location_name' in results_df.columns:
                        most_common_location = results_df['location_name'].mode()
                        if len(most_common_location) > 0:
                            location_name = str(most_common_location.iloc[0])
                            # Clean the location name for filename (remove special chars, replace spaces)
                            import re
                            clean_location = re.sub(r'[^\w\s-]', '', location_name).strip()
                            clean_location = re.sub(r'[-\s]+', '_', clean_location)
                            filename_location = clean_location[:30]  # Limit length
                        else:
                            filename_location = "Unknown"
                    else:
                        filename_location = "Unknown"

                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"{filename_location}_listings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                    # Stats
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Found", len(results_df))
                    with col2:
                        avg_price = results_df['price_value'].mean()
                        st.metric("Avg Price", f"${avg_price:,.0f}" if not pd.isna(avg_price) else "N/A")
                    with col3:
                        avg_beds = pd.to_numeric(results_df['bedrooms'], errors='coerce').mean()
                        st.metric("Avg Bedrooms", f"{avg_beds:.1f}" if not pd.isna(avg_beds) else "N/A")
                    with col4:
                        fetched_count = len(results_df[results_df['owner_fetched_at'].notna()])
                        st.metric("Owner Details Fetched", f"{fetched_count}/{len(results_df)}")
                else:
                    st.warning("No listings match your filter criteria.")
            else:
                st.info("No results found. Try a different search term.")
        except Exception as e:
            logger.error(f"Search error: {e}")
            st.error(f"Search error: {e}")
    else:
        st.info("Start typing in the search box to find listings. Results load instantly!")

    # Footer
    st.divider()
    st.caption("Instant search - loads only when you search. Filter options cached for fast performance.")


if __name__ == "__main__":
    main()

