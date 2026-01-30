#!/usr/bin/env python3
"""
Database migration script to add indexes and improve schema.
Run this script to optimize existing databases.
"""

import os
import sqlite3
from datetime import datetime, timezone

# Get the absolute path to the listings directory
LISTINGS_DIR = os.path.dirname(os.path.abspath(__file__))
LISTINGS_DB = os.path.join(LISTINGS_DIR, 'listings.db')
OWNERS_DB = os.path.join(LISTINGS_DIR, 'owners.db')
AGENTS_DB = os.path.join(LISTINGS_DIR, 'agents.db')


def migrate_listings_db(db_path: str) -> None:
    """Migrate listings database - add indexes and improve schema."""
    print(f"\n📊 Migrating {db_path}...")
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Add missing columns if they don't exist
    columns_to_add = [
        ('owner_fetched_at', 'TEXT'),
        ('owner_names', 'TEXT'),
        ('owner_phones', 'TEXT'),
        ('owner_emails', 'TEXT'),
    ]
    
    # Get existing columns
    c.execute("PRAGMA table_info(listings)")
    existing_columns = {row[1] for row in c.fetchall()}
    
    for col_name, col_type in columns_to_add:
        if col_name not in existing_columns:
            try:
                c.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
                print(f"  ✅ Added column: {col_name}")
            except sqlite3.OperationalError:
                print(f"  ℹ️ Column {col_name} already exists")
    
    # Create indexes
    indexes = [
        ("idx_listings_rera", "CREATE INDEX IF NOT EXISTS idx_listings_rera ON listings(rera)"),
        ("idx_listings_agent_id", "CREATE INDEX IF NOT EXISTS idx_listings_agent_id ON listings(agent_id)"),
        ("idx_listings_property_type", "CREATE INDEX IF NOT EXISTS idx_listings_property_type ON listings(property_type)"),
        ("idx_listings_location", "CREATE INDEX IF NOT EXISTS idx_listings_location ON listings(location_name)"),
        ("idx_listings_price", "CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_value)"),
        ("idx_listings_owner_fetched", "CREATE INDEX IF NOT EXISTS idx_listings_owner_fetched ON listings(owner_fetched_at)"),
    ]
    
    for index_name, index_sql in indexes:
        try:
            c.execute(index_sql)
            print(f"  ✅ Created index: {index_name}")
        except sqlite3.OperationalError as e:
            if "already exists" in str(e):
                print(f"  ℹ️ Index {index_name} already exists")
            else:
                print(f"  ⚠️ Error creating index {index_name}: {e}")
    
    # Add foreign key for agent_id if agents table exists
    try:
        c.execute("SELECT id FROM agents LIMIT 1")
        # Agents table exists, add foreign key relationship
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_listings_agent_id_ref 
            ON listings(agent_id) 
            WHERE agent_id IS NOT NULL
        """)
        print("  ✅ Created agent reference index")
    except sqlite3.OperationalError:
        pass  # Agents table doesn't exist yet
    
    conn.commit()
    conn.close()
    print(f"  ✅ Migration complete for {db_path}")


def migrate_owners_db(db_path: str) -> None:
    """Migrate owners database - create proper schema and indexes."""
    print(f"\n📊 Migrating {db_path}...")
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create owners table with proper schema
    c.execute('''
        CREATE TABLE IF NOT EXISTS owners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT NOT NULL UNIQUE,
            rera TEXT,
            owner_names TEXT,
            owner_phones TEXT,
            owner_emails TEXT,
            property_number TEXT,
            fetched_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add property_number column if it doesn't exist (for existing databases)
    try:
        c.execute("ALTER TABLE owners ADD COLUMN property_number TEXT")
        print("  ✅ Added column: property_number")
    except sqlite3.OperationalError:
        print("  ℹ️ Column property_number already exists")

    # Create indexes
    indexes = [
        ("idx_owners_listing_id", "CREATE UNIQUE INDEX IF NOT EXISTS idx_owners_listing_id ON owners(listing_id)"),
        ("idx_owners_rera", "CREATE INDEX IF NOT EXISTS idx_owners_rera ON owners(rera)"),
        ("idx_owners_fetched_at", "CREATE INDEX IF NOT EXISTS idx_owners_fetched_at ON owners(fetched_at)"),
        ("idx_owners_property_number", "CREATE INDEX IF NOT EXISTS idx_owners_property_number ON owners(property_number)"),
    ]
    
    for index_name, index_sql in indexes:
        try:
            c.execute(index_sql)
            print(f"  ✅ Created index: {index_name}")
        except sqlite3.OperationalError as e:
            if "already exists" in str(e):
                print(f"  ℹ️ Index {index_name} already exists")
            else:
                print(f"  ⚠️ Error creating index {index_name}: {e}")
    
    conn.commit()
    conn.close()
    print(f"  ✅ Migration complete for {db_path}")


def migrate_agents_db(db_path: str) -> None:
    """Migrate agents database - add indexes."""
    print(f"\n📊 Migrating {db_path}...")
    
    if not os.path.exists(db_path):
        print(f"  ℹ️ Agents database does not exist yet, skipping")
        return
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create indexes
    indexes = [
        ("idx_agents_slug", "CREATE INDEX IF NOT EXISTS idx_agents_slug ON agents(slug)"),
        ("idx_agents_user_id", "CREATE INDEX IF NOT EXISTS idx_agents_user_id ON agents(userId)"),
        ("idx_agents_license", "CREATE INDEX IF NOT EXISTS idx_agents_license ON agents(licenseNumber)"),
        ("idx_agents_superagent", "CREATE INDEX IF NOT EXISTS idx_agents_superagent ON agents(superagent)"),
        ("idx_agents_verified", "CREATE INDEX IF NOT EXISTS idx_agents_verified ON agents(verified)"),
    ]
    
    for index_name, index_sql in indexes:
        try:
            c.execute(index_sql)
            print(f"  ✅ Created index: {index_name}")
        except sqlite3.OperationalError as e:
            if "already exists" in str(e):
                print(f"  ℹ️ Index {index_name} already exists")
            else:
                print(f"  ⚠️ Error creating index {index_name}: {e}")
    
    conn.commit()
    conn.close()
    print(f"  ✅ Migration complete for {db_path}")


def sync_owner_data(listings_db: str, owners_db: str) -> None:
    """Sync owner data from listings.db to owners.db."""
    print(f"\n🔄 Syncing owner data from {listings_db} to {owners_db}...")
    
    conn_listings = sqlite3.connect(listings_db)
    c_listings = conn_listings.cursor()
    
    conn_owners = sqlite3.connect(owners_db)
    c_owners = conn_owners.cursor()
    
    # Create owners table if not exists
    c_owners.execute('''
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
    
    # Find listings with owner data
    c_listings.execute("""
        SELECT id, rera, owner_names, owner_phones, owner_emails, owner_fetched_at
        FROM listings
        WHERE owner_fetched_at IS NOT NULL
    """)
    
    listings_with_owners = c_listings.fetchall()
    synced = 0
    
    for row in listings_with_owners:
        listing_id, rera, owner_names, owner_phones, owner_emails, fetched_at = row
        
        # Check if already exists in owners.db
        c_owners.execute("SELECT id FROM owners WHERE listing_id = ?", (listing_id,))
        existing = c_owners.fetchone()
        
        if not existing:
            c_owners.execute("""
                INSERT INTO owners (listing_id, rera, owner_names, owner_phones, owner_emails, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (listing_id, rera, owner_names, owner_phones, owner_emails, fetched_at))
            synced += 1
    
    conn_owners.commit()
    conn_listings.close()
    conn_owners.close()
    
    print(f"  ✅ Synced {synced} listings with owner data")


def main():
    """Main migration function."""
    print("=" * 60)
    print("🚀 Starting Database Migration")
    print("=" * 60)
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Working directory: {LISTINGS_DIR}")
    
    # Migrate all databases
    migrate_listings_db(LISTINGS_DB)
    migrate_owners_db(OWNERS_DB)
    migrate_agents_db(AGENTS_DB)
    
    # Sync data between databases
    sync_owner_data(LISTINGS_DB, OWNERS_DB)
    
    print("\n" + "=" * 60)
    print("✅ Migration Complete!")
    print("=" * 60)
    print("\n📝 Recommendations:")
    print("  1. Run this script periodically to maintain indexes")
    print("  2. Consider vacuuming the databases after bulk operations:")
    print("     sqlite3 listings.db 'VACUUM;'")
    print("  3. Monitor database size and consider archiving old data")


if __name__ == '__main__':
    main()

