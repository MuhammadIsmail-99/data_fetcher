import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import time

# Page config
st.set_page_config( 
    page_title="Property Listings Search",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache database connection
@st.cache_resource
def get_db_connection():
    return sqlite3.connect('/home/muhammad/Documents/listings/listings.db')

# Get total count without loading all data
@st.cache_data(ttl=3600)
def get_total_count():
    conn = get_db_connection()
    result = conn.execute("SELECT COUNT(*) FROM listings").fetchone()
    return result[0] if result else 0

# Fast filtered search - only loads what's needed
@st.cache_data(ttl=300)
def search_listings(search_query: str, search_field: str, limit: int = 100):
    conn = get_db_connection()
    query = f"""
    SELECT id, title, property_type, price_value, price_currency, 
           bedrooms, bathrooms, location_name, broker_name, listed_date
    FROM listings
    WHERE {search_field} LIKE ?
    LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(f"%{search_query}%", limit))
    return df

# Get filter options on demand
@st.cache_data(ttl=3600)
def get_filter_options():
    conn = get_db_connection()
    property_types = conn.execute(
        "SELECT DISTINCT property_type FROM listings WHERE property_type IS NOT NULL ORDER BY property_type"
    ).fetchall()
    locations = conn.execute(
        "SELECT DISTINCT location_name FROM listings WHERE location_name IS NOT NULL ORDER BY location_name"
    ).fetchall()
    price_max = conn.execute(
        "SELECT MAX(price_value) FROM listings WHERE price_value IS NOT NULL"
    ).fetchone()
    
    return {
        'property_types': [p[0] for p in property_types],
        'locations': [l[0] for l in locations],
        'price_max': price_max[0] if price_max[0] else 0
    }

# Title
st.title("🏠 Property Listings Search")

# Get total count
total_listings = get_total_count()
st.sidebar.metric("Total Listings", total_listings)

# Search controls
col1, col2 = st.columns([3, 1])

with col1:
    search_query = st.text_input(
        "🔍 Quick Search",
        placeholder="Search by title, location, property type...",
        help="Type to search in real-time"
    )

with col2:
    search_field = st.selectbox(
        "Search in",
        ["title", "location_name", "property_type", "broker_name"],
        label_visibility="collapsed"
    )

# Only load filter options if user interacts
st.sidebar.subheader("⚙️ Filters")

filter_options = get_filter_options()

price_range = st.sidebar.slider(
    "Price Range",
    min_value=0,
    max_value=int(filter_options['price_max']),
    value=(0, int(filter_options['price_max'])),
)

property_types = st.sidebar.multiselect(
    "Property Type",
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
st.subheader("📊 Search Results")

if search_query:
    # Only search when user types something
    try:
        results_df = search_listings(search_query, search_field)
        
        # Apply additional filters to results
        if results_df is not None and len(results_df) > 0:
            # Price filter
            results_df = results_df[
                (results_df['price_value'] >= price_range[0]) & 
                (results_df['price_value'] <= price_range[1])
            ]
            
            # Property type filter
            if property_types:
                results_df = results_df[results_df['property_type'].isin(property_types)]
            
            # Bedrooms filter
            results_df = results_df[
                (pd.to_numeric(results_df['bedrooms'], errors='coerce').fillna(0) >= bedrooms[0]) &
                (pd.to_numeric(results_df['bedrooms'], errors='coerce').fillna(0) <= bedrooms[1])
            ]
            
            # Location filter
            if locations:
                results_df = results_df[results_df['location_name'].isin(locations)]
            
            st.metric("Results Found", len(results_df))
            
            if len(results_df) > 0:
                # Format display columns
                display_df = results_df[['id', 'title', 'property_type', 'price_value', 'price_currency', 
                                        'bedrooms', 'bathrooms', 'location_name', 'broker_name', 'listed_date']].copy()
                display_df.columns = ['ID', 'Title', 'Type', 'Price', 'Currency', 
                                      'Beds', 'Baths', 'Location', 'Broker', 'Listed Date']
                
                # Display as table
                st.dataframe(display_df, width='stretch', height=400)
                
                # Download option
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"listings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
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
                    st.metric("Unique Brokers", results_df['broker_name'].nunique())
            else:
                st.warning("No listings match your filter criteria.")
        else:
            st.info("No results found. Try a different search term.")
    except Exception as e:
        st.error(f"Search error: {e}")
else:
    st.info("💡 Start typing in the search box to find listings. Results load instantly!")

# Footer
st.divider()
st.caption("💡 Instant search - loads only when you search. Filter options cached for fast performance.")
