# Data Fetcher - Property Listings Management

A comprehensive tool for fetching property agents, their listings, and owner details with a search interface powered by Streamlit.

## Overview

This project fetches real estate agents from PropertyFinder, retrieves their property listings, and provides a web-based search interface to browse and manage property information.

## Prerequisites

- Python 3.8+
- pip or conda
- SQLite3 (comes with Python)

## Setup Instructions

### 1. Environment Configuration

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` and add any required API keys or configuration values:

```env
DATABASE_URL=sqlite:///listings.db
```

### 2. Install Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

or with conda:

```bash
conda create -n listings python=3.10
conda activate listings
pip install -r requirements.txt
```

### 3. Fetch Agents

Fetch all real estate agents from the PropertyFinder API:

```bash
python fetch_agents.py
```

This will:
- Connect to the PropertyFinder API
- Fetch all superagents with pagination
- Store agent data in `agents.db`
- Display progress in real-time

**Options:**
- Run `python fetch_agents.py --help` to see available arguments for pagination, filtering, etc.

### 4. Fetch Listings

Fetch property listings for all agents in the database:

```bash
python fetch_listings.py
```

This will:
- Query all agents from the agents database
- Fetch listings for each agent with multi-threading support
- Store listings in `listings.db`
- Display progress with agent and listing counts
- Handle rate limiting automatically

**Features:**
- Multi-threaded fetching for faster performance
- Automatic retry logic
- Progress tracking
- Rate limiting compliance

### 5. Launch Search Interface

Start the Streamlit search UI to browse and interact with listings:

```bash
streamlit run search_ui.py
```

The interface will open in your default browser at `http://localhost:8501`

**Features:**
- Search property listings by various filters
- View owner details (RERA registration)
- Advanced filtering and sorting
- Export search results

## Project Structure

```
.
├── fetch_agents.py           # Fetch agents from API
├── fetch_listings.py         # Fetch listings for all agents
├── search_ui.py              # Streamlit search interface
├── owner_fetcher.py          # Fetch owner/RERA details
├── utils.py                  # Utility functions
├── migrate_db.py             # Database migration utilities
├── csv_rera_to_owner_details.py  # CSV import helper
├── .env.example              # Environment template
├── .gitignore                # Git ignore rules
└── tests/                    # Test suite
    ├── __init__.py
    └── test_utils.py
```

## Database Files

The project uses SQLite databases:
- `agents.db` - Stores agent information
- `listings.db` - Stores property listings
- `owners.db` - Stores owner/RERA details (generated during fetch)

**Note:** Database files are excluded from version control (see `.gitignore`)

## Quick Start (Complete Workflow)

```bash
# 1. Setup environment
cp .env.example .env
pip install -r requirements.txt

# 2. Fetch agents
python fetch_agents.py

# 3. Fetch listings
python fetch_listings.py

# 4. Launch search UI
streamlit run search_ui.py
```

## Troubleshooting

- **Database locked error:** Ensure only one process is accessing the database at a time
- **API rate limiting:** The scripts include automatic rate limiting; wait if you see rate limit messages
- **Missing dependencies:** Run `pip install -r requirements.txt` again
- **Port already in use:** Use `streamlit run search_ui.py --server.port 8502` to use a different port

## Configuration Files

- `.env` - Environment variables (create from `.env.example`)
- `.gitignore` - Files excluded from version control (databases and .env are excluded)

## License

This project is private and intended for internal use.
