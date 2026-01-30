#!/usr/bin/env python3
"""
Shared utilities module for the listings system.
Consolidates duplicate code and provides common functionality.
"""

import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Get the absolute path to the listings directory
LISTINGS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(LISTINGS_DIR, 'listings.db')
OWNERS_DB_PATH = os.path.join(LISTINGS_DIR, 'owners.db')
AGENTS_DB_PATH = os.path.join(LISTINGS_DIR, 'agents.db')


# =============================================================================
# Database Connection Utilities
# =============================================================================

def get_db_connection(db_file: str = None) -> sqlite3.Connection:
    """Get database connection for listings database.
    
    Args:
        db_file: Optional path to database file. If relative, resolved from listings dir.
        
    Returns:
        SQLite connection object
    """
    if db_file is None:
        db_file = DEFAULT_DB_PATH
    elif not os.path.isabs(db_file):
        db_file = os.path.join(LISTINGS_DIR, db_file)
    
    # Ensure absolute path
    db_file = os.path.abspath(db_file)
    
    # Check if database file exists
    if not os.path.exists(db_file):
        raise FileNotFoundError(f"Database file not found: {db_file}")
    
    # Check if readable
    if not os.access(db_file, os.R_OK):
        raise PermissionError(f"No read permission for database: {db_file}")
    
    try:
        conn = sqlite3.connect(db_file, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Test connection
        conn.execute("SELECT 1")
        return conn
    except sqlite3.OperationalError as e:
        raise sqlite3.OperationalError(f"Cannot open database {db_file}: {e}")


def get_owners_db_connection() -> sqlite3.Connection:
    """Get owners database connection."""
    db_file = os.path.abspath(OWNERS_DB_PATH)
    if not os.path.exists(db_file):
        raise FileNotFoundError(f"Database file not found: {db_file}")
    if not os.access(db_file, os.R_OK):
        raise PermissionError(f"No read permission for database: {db_file}")
    try:
        conn = sqlite3.connect(db_file, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("SELECT 1")
        return conn
    except sqlite3.OperationalError as e:
        raise sqlite3.OperationalError(f"Cannot open database {db_file}: {e}")


def get_agents_db_connection() -> sqlite3.Connection:
    """Get agents database connection."""
    db_file = os.path.abspath(AGENTS_DB_PATH)
    if not os.path.exists(db_file):
        raise FileNotFoundError(f"Database file not found: {db_file}")
    if not os.access(db_file, os.R_OK):
        raise PermissionError(f"No read permission for database: {db_file}")
    try:
        conn = sqlite3.connect(db_file, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("SELECT 1")
        return conn
    except sqlite3.OperationalError as e:
        raise sqlite3.OperationalError(f"Cannot open database {db_file}: {e}")


def init_database_indexes(conn: sqlite3.Connection) -> None:
    """Initialize database indexes for better query performance.
    
    Args:
        conn: Database connection
    """
    # Listings indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_listings_rera ON listings(rera)",
        "CREATE INDEX IF NOT EXISTS idx_listings_agent_id ON listings(agent_id)",
        "CREATE INDEX IF NOT EXISTS idx_listings_property_type ON listings(property_type)",
        "CREATE INDEX IF NOT EXISTS idx_listings_location ON listings(location_name)",
        "CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_value)",
        "CREATE INDEX IF NOT EXISTS idx_listings_owner_fetched ON listings(owner_fetched_at)",
        # Owners indexes
        "CREATE INDEX IF NOT EXISTS idx_owners_listing_id ON owners(listing_id)",
        "CREATE INDEX IF NOT EXISTS idx_owners_fetched_at ON owners(fetched_at)",
        # Agents indexes
        "CREATE INDEX IF NOT EXISTS idx_agents_slug ON agents(slug)",
        "CREATE INDEX IF NOT EXISTS idx_agents_user_id ON agents(userId)",
        "CREATE INDEX IF NOT EXISTS idx_agents_license ON agents(licenseNumber)",
    ]
    
    for index_sql in indexes:
        try:
            conn.execute(index_sql)
        except Exception:
            # Index may already exist
            pass
    
    conn.commit()


# =============================================================================
# Owner Details Extraction Utilities
# =============================================================================

def has_owner_details(responses_text: List[str]) -> bool:
    """Check if any response contains actual owner details.

    Args:
        responses_text: List of response text strings

    Returns:
        True if owner details are found, False otherwise
    """
    for text in responses_text:
        if not text:
            continue
        t = text.strip()

        # Check for an explicit owner block marker
        if '👤 owner details:' in t.lower() or 'owner details:' in t.lower():
            return True

        # Check for presence of both Name and Phone fields
        name_match = re.search(r'(📝\s*)?name\s*:\s*\S+', t, flags=re.IGNORECASE)
        phone_match = re.search(r'(📞\s*)?phone\s*:\s*\S+', t, flags=re.IGNORECASE)
        if name_match and phone_match:
            return True

        # Check if response explicitly says "Owner details unavailable"
        if 'owner details unavailable' in t.lower() or '❌ owner details unavailable' in t.lower():
            return False

    return False


def has_owner_details_response(responses_text: List[str]) -> Tuple[bool, bool]:
    """Check if response contains owner details or explicitly says unavailable.

    Args:
        responses_text: List of response text strings

    Returns:
        Tuple of (has_owner_details, has_unavailable_message)
    """
    for text in responses_text:
        if not text:
            continue
        t = text.strip()

        # Check if response explicitly says "Owner details unavailable"
        if 'owner details unavailable' in t.lower() or '❌ owner details unavailable' in t.lower():
            return (False, True)

        # Check for an explicit owner block marker
        if '👤 owner details:' in t.lower() or 'owner details:' in t.lower():
            return (True, False)

        # Check for presence of both Name and Phone fields
        name_match = re.search(r'(📝\s*)?name\s*:\s*\S+', t, flags=re.IGNORECASE)
        phone_match = re.search(r'(📞\s*)?phone\s*:\s*\S+', t, flags=re.IGNORECASE)
        if name_match and phone_match:
            return (True, False)

    return (False, False)


def extract_owner_details(responses_text: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Extract owner names, phones, and emails from response text.
    
    Args:
        responses_text: List of response text strings
        
    Returns:
        Tuple of (owner_names, owner_phones, owner_emails) lists
    """
    owner_names = []
    owner_phones = []
    owner_emails = []

    combined_text = '\n'.join(responses_text)

    # Extract names
    name_matches = re.findall(
        r'(?:📝\s*)?name\s*:\s*(.+?)(?=\n|$)', 
        combined_text, 
        flags=re.IGNORECASE | re.MULTILINE
    )
    for match in name_matches:
        name = match.strip().split('\n')[0].strip()
        if name and name not in owner_names:
            owner_names.append(name)

    # Extract phones
    phone_matches = re.findall(
        r'(?:📞\s*)?phone\s*:\s*(\+?[0-9\s\-()]{6,})', 
        combined_text, 
        flags=re.IGNORECASE
    )
    for match in phone_matches:
        phone = match.strip()
        if phone and phone not in owner_phones:
            owner_phones.append(phone)

    # Extract emails
    email_matches = re.findall(
        r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', 
        combined_text
    )
    for match in email_matches:
        email = match.strip()
        if email and email not in owner_emails:
            owner_emails.append(email)

    return owner_names, owner_phones, owner_emails


def extract_property_details(responses_text: List[str]) -> Dict[str, str]:
    """Extract property details from Telegram response text.

    Extracts: property_number, property_size, rooms, is_freehold, area, project, building

    Args:
        responses_text: List of response text strings

    Returns:
        Dict with property details
    """
    combined_text = '\n'.join(responses_text)
    details = {}

    # Extract Property Number
    property_number_match = re.search(
        r'(?:🔢\s*)?Property\s*Number\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if property_number_match:
        details['property_number'] = property_number_match.group(1).strip().split('\n')[0].strip()

    # Extract Property Size
    property_size_match = re.search(
        r'(?:📏\s*)?Property\s*Size\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if property_size_match:
        details['property_size'] = property_size_match.group(1).strip().split('\n')[0].strip()

    # Extract Rooms
    rooms_match = re.search(
        r'(?:🛏️\s*)?Rooms\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if rooms_match:
        details['rooms'] = rooms_match.group(1).strip().split('\n')[0].strip()

    # Extract Is Free Hold
    freehold_match = re.search(
        r'(?:🏢\s*)?Is\s*Free\s*Hold\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if freehold_match:
        details['is_freehold'] = freehold_match.group(1).strip().split('\n')[0].strip()

    # Extract Area
    area_match = re.search(
        r'(?:📏\s*)?Area\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if area_match:
        details['area'] = area_match.group(1).strip().split('\n')[0].strip()

    # Extract Project
    project_match = re.search(
        r'(?:📏\s*)?Project\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if project_match:
        details['project'] = project_match.group(1).strip().split('\n')[0].strip()

    # Extract Building
    building_match = re.search(
        r'(?:🏢\s*)?Building\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if building_match:
        details['building'] = building_match.group(1).strip().split('\n')[0].strip()

    # Extract Property Type
    property_type_match = re.search(
        r'(?:📦\s*)?Property\s*Type\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if property_type_match:
        details['property_type'] = property_type_match.group(1).strip().split('\n')[0].strip()

    # Extract Property Sub Type
    property_sub_type_match = re.search(
        r'(?:📦\s*)?Property\s*Sub\s*Type\s*:\s*(.+)',
        combined_text,
        flags=re.IGNORECASE
    )
    if property_sub_type_match:
        details['property_sub_type'] = property_sub_type_match.group(1).strip().split('\n')[0].strip()

    return details


# =============================================================================
# Validation Utilities
# =============================================================================

def validate_rera(rera: str) -> bool:
    """Validate RERA format.
    
    Args:
        rera: RERA string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not rera or not isinstance(rera, str):
        return False
    # RERA format typically: numbers and dashes, e.g., "254-XXXX"
    rera = rera.strip()
    return bool(re.match(r'^[\d\-]+$', rera))


def validate_phone(phone: str) -> bool:
    """Validate phone number format.
    
    Args:
        phone: Phone number string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not phone or not isinstance(phone, str):
        return False
    phone = phone.strip()
    # Basic validation for international format
    return bool(re.match(r'^\+?[\d\s\-()]{6,}$', phone))


def validate_email(email: str) -> bool:
    """Validate email format.
    
    Args:
        email: Email string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not email or not isinstance(email, str):
        return False
    email = email.strip()
    pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    return bool(re.match(pattern, email))


# =============================================================================
# JSON Serialization Utilities
# =============================================================================

def serialize_for_db(value: Any) -> Optional[str]:
    """Serialize a value for database storage.
    
    Args:
        value: Value to serialize (list, dict, or scalar)
        
    Returns:
        JSON string or None if value is empty/None
    """
    if value is None or (isinstance(value, (list, dict)) and len(value) == 0):
        return None
    
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    
    return str(value)


def deserialize_from_db(json_str: str, default: Any = None) -> Any:
    """Deserialize a value from database storage.
    
    Args:
        json_str: JSON string from database
        default: Default value if deserialization fails
        
    Returns:
        Deserialized value or default
    """
    if not json_str:
        return default
    
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default


# =============================================================================
# Time Utilities
# =============================================================================

def get_current_isoformat() -> str:
    """Get current timestamp in ISO format.
    
    Returns:
        ISO format timestamp string
    """
    return datetime.now(timezone.utc).isoformat()


def parse_isoformat(date_str: str) -> Optional[datetime]:
    """Parse ISO format date string.
    
    Args:
        date_str: ISO format date string
        
    Returns:
        datetime object or None if parsing fails
    """
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Logging Utilities
# =============================================================================

def setup_logging(
    name: str = "listings",
    level: int = 20,  # INFO level
    log_file: Optional[str] = None
) -> logging.Logger:
    """Setup structured logging for the application.
    
    Args:
        name: Logger name
        level: Logging level (10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR)
        log_file: Optional path to log file
        
    Returns:
        Configured logger instance
    """
    import logging
    from logging.handlers import RotatingFileHandler
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# =============================================================================
# Rate Limiting Utilities
# =============================================================================

class RateLimiter:
    """Adaptive rate limiter with exponential backoff.
    
    Attributes:
        min_delay: Minimum delay between requests (seconds)
        max_delay: Maximum delay between requests (seconds)
        backoff_factor: Multiplier for exponential backoff
        jitter: Random jitter range (seconds)
    """
    
    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: float = 0.5
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.current_delay = min_delay
        self.last_request_time = 0
        self.failure_count = 0
    
    def wait(self) -> None:
        """Wait before making the next request."""
        import time
        import random
        
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.current_delay:
            sleep_time = self.current_delay - time_since_last
            # Add jitter
            sleep_time += random.uniform(-self.jitter, self.jitter)
            sleep_time = max(0, sleep_time)
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def record_success(self) -> None:
        """Record a successful request - decrease delay."""
        self.failure_count = 0
        self.current_delay = max(self.min_delay, self.current_delay / self.backoff_factor)
    
    def record_failure(self) -> None:
        """Record a failed request - increase delay."""
        self.failure_count += 1
        self.current_delay = min(
            self.max_delay,
            self.current_delay * self.backoff_factor
        )
    
    def reset(self) -> None:
        """Reset rate limiter to initial state."""
        self.current_delay = self.min_delay
        self.failure_count = 0
        self.last_request_time = 0


# =============================================================================
# Circuit Breaker Utilities
# =============================================================================

class CircuitBreaker:
    """Circuit breaker pattern implementation for API calls.
    
    Attributes:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds to wait before attempting recovery
        expected_exception: Exception type to catch
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    def __enter__(self):
        if self.state == "open":
            if self._can_attempt_recovery():
                self.state = "half-open"
            else:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is open. Retry after {self.recovery_timeout} seconds."
                )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type and issubclass(exc_type, self.expected_exception):
            self.record_failure()
            return True
        else:
            self.record_success()
            return False
    
    def _can_attempt_recovery(self) -> bool:
        """Check if circuit breaker can attempt recovery."""
        if self.last_failure_time is None:
            return True
        return (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() >= self.recovery_timeout
    
    def record_failure(self) -> None:
        """Record a failure."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
    
    def record_success(self) -> None:
        """Record a success."""
        self.failure_count = 0
        self.state = "closed"
    
    def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


# =============================================================================
# Export Check
# =============================================================================


