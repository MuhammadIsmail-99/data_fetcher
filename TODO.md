# TODO: Search UI Modifications

## Task: Remove emojis, reposition search, and add category filters

### Steps:
1. [x] Remove all emojis from search_ui.py UI text
2. [x] Move search bar and dropdown to right sidebar (right-aligned)
3. [x] Set default search field to "location_name"
4. [x] Add Category filter (Buy/Rent) as first filter in sidebar
5. [x] Add Property Type filter (Residential/Commercial) to sidebar
6. [x] Update get_filter_options() to fetch new filter options
7. [x] Update apply_filters() function to handle new filters
8. [x] Update display text to remove emojis (owner details, buttons, etc.)
9. [x] Update search_listings() to include offering_type column for filtering

### Filter Categories:
- **Category (Buy/Rent)**: Based on `offering_type`
  - Residential for Rent -> "Rent"
  - Residential for Sale -> "Buy"
  - Commercial for Rent -> "Commercial Rent"
  - Commercial for Sale -> "Commercial Buy"
  - Default: "All"

- **Property Type (Residential/Commercial)**: Based on `property_type`
  - Residential: Apartment, Villa, Townhouse, Penthouse, Bungalow, Duplex, etc.
  - Commercial: Office Space, Retail, Shop, Show Room, Business Centre, etc.
  - Default: "All"

### Files modified:
- /home/muhammad/Documents/listings/search_ui.py

**Status: COMPLETED**

