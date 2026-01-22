#!/usr/bin/env python3
"""
Unit tests for utils module.
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    has_owner_details,
    extract_owner_details,
    validate_rera,
    validate_phone,
    validate_email,
    serialize_for_db,
    deserialize_from_db,
    get_current_isoformat,
    RateLimiter,
    CircuitBreaker,
    CircuitBreakerOpenError,
    LISTINGS_DIR,
    DEFAULT_DB_PATH
)


class TestHasOwnerDetails(unittest.TestCase):
    """Tests for has_owner_details function."""
    
    def test_explicit_owner_block(self):
        """Test detection of explicit owner block marker."""
        responses = ["👤 owner details:\n📝 Name: John Doe\n📞 Phone: +1234567890"]
        self.assertTrue(has_owner_details(responses))
    
    def test_plain_owner_block(self):
        """Test detection of plain owner block marker."""
        responses = ["Owner details:\nName: John Doe\nPhone: +1234567890"]
        self.assertTrue(has_owner_details(responses))
    
    def test_name_and_phone_fields(self):
        """Test detection of name and phone fields."""
        responses = ["📝 Name: John Doe\n📞 Phone: +1234567890"]
        self.assertTrue(has_owner_details(responses))
    
    def test_missing_phone(self):
        """Test rejection when phone is missing."""
        responses = ["📝 Name: John Doe\nNo phone here"]
        self.assertFalse(has_owner_details(responses))
    
    def test_missing_name(self):
        """Test rejection when name is missing."""
        responses = ["📞 Phone: +1234567890\nNo name here"]
        self.assertFalse(has_owner_details(responses))
    
    def test_empty_responses(self):
        """Test handling of empty responses."""
        self.assertFalse(has_owner_details([]))
        self.assertFalse(has_owner_details([""]))
        self.assertFalse(has_owner_details(["   "]))


class TestExtractOwnerDetails(unittest.TestCase):
    """Tests for extract_owner_details function."""
    
    def test_extract_names(self):
        """Test extraction of owner names."""
        responses = ["📝 Name: John Doe\n📞 Phone: +1234567890"]
        names, phones, emails = extract_owner_details(responses)
        self.assertIn("John Doe", names)
    
    def test_extract_phones(self):
        """Test extraction of phone numbers."""
        responses = ["📝 Name: John Doe\n📞 Phone: +123-456-7890"]
        names, phones, emails = extract_owner_details(responses)
        self.assertTrue(len(phones) > 0)
    
    def test_extract_emails(self):
        """Test extraction of email addresses."""
        responses = ["📝 Name: John Doe\n📧 Email: john@example.com"]
        names, phones, emails = extract_owner_details(responses)
        self.assertIn("john@example.com", emails)
    
    def test_multiple_entries(self):
        """Test extraction of multiple entries."""
        responses = [
            "📝 Name: John Doe\n📞 Phone: +1234567890\n📧 Email: john@example.com",
            "📝 Name: Jane Smith\n📞 Phone: +0987654321\n📧 Email: jane@example.com"
        ]
        names, phones, emails = extract_owner_details(responses)
        self.assertIn("John Doe", names)
        self.assertIn("Jane Smith", names)
        self.assertTrue(len(emails) >= 2)
    
    def test_duplicate_prevention(self):
        """Test prevention of duplicate entries."""
        responses = [
            "📝 Name: John Doe\n📞 Phone: +1234567890",
            "📝 Name: John Doe\n📞 Phone: +1234567890"
        ]
        names, phones, emails = extract_owner_details(responses)
        self.assertEqual(names.count("John Doe"), 1)


class TestValidation(unittest.TestCase):
    """Tests for validation functions."""
    
    def test_valid_rera(self):
        """Test valid RERA formats."""
        self.assertTrue(validate_rera("254-1234"))
        self.assertTrue(validate_rera("12345"))
        self.assertTrue(validate_rera("1234-5678-90"))
    
    def test_invalid_rera(self):
        """Test invalid RERA formats."""
        self.assertFalse(validate_rera(""))
        self.assertFalse(validate_rera(None))
        self.assertFalse(validate_rera("ABC-123"))  # Letters not allowed
        self.assertFalse(validate_rera("1234!5678"))  # Special chars
    
    def test_valid_phone(self):
        """Test valid phone formats."""
        self.assertTrue(validate_phone("+1234567890"))
        self.assertTrue(validate_phone("123-456-7890"))
        self.assertTrue(validate_phone("(123) 456-7890"))
    
    def test_invalid_phone(self):
        """Test invalid phone formats."""
        self.assertFalse(validate_phone(""))
        self.assertFalse(validate_phone(None))
        self.assertFalse(validate_phone("123"))  # Too short
    
    def test_valid_email(self):
        """Test valid email formats."""
        self.assertTrue(validate_email("test@example.com"))
        self.assertTrue(validate_email("user.name@domain.org"))
    
    def test_invalid_email(self):
        """Test invalid email formats."""
        self.assertFalse(validate_email(""))
        self.assertFalse(validate_email("invalid"))
        self.assertFalse(validate_email("no@domain"))
        self.assertFalse(validate_email("@nodomain.com"))


class TestSerialization(unittest.TestCase):
    """Tests for serialization functions."""
    
    def test_serialize_list(self):
        """Test serialization of list."""
        result = serialize_for_db(["a", "b", "c"])
        self.assertEqual(json.loads(result), ["a", "b", "c"])
    
    def test_serialize_dict(self):
        """Test serialization of dict."""
        result = serialize_for_db({"key": "value"})
        self.assertEqual(json.loads(result), {"key": "value"})
    
    def test_serialize_none(self):
        """Test serialization of None."""
        self.assertIsNone(serialize_for_db(None))
    
    def test_serialize_empty_list(self):
        """Test serialization of empty list."""
        self.assertIsNone(serialize_for_db([]))
    
    def test_deserialize(self):
        """Test deserialization."""
        result = deserialize_from_db('["a", "b", "c"]', [])
        self.assertEqual(result, ["a", "b", "c"])
    
    def test_deserialize_invalid(self):
        """Test deserialization of invalid JSON."""
        result = deserialize_from_db("invalid json", [])
        self.assertEqual(result, [])


class TestRateLimiter(unittest.TestCase):
    """Tests for RateLimiter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.limiter = RateLimiter(min_delay=0.1, max_delay=1.0, backoff_factor=2.0, jitter=0.05)
    
    def test_initial_state(self):
        """Test initial state of rate limiter."""
        self.assertEqual(self.limiter.current_delay, 0.1)
        self.assertEqual(self.limiter.failure_count, 0)
    
    def test_record_success(self):
        """Test success recording decreases delay."""
        self.limiter.record_failure()  # Increase delay
        self.limiter.record_success()  # Should decrease
        self.assertLess(self.limiter.current_delay, 2.0)
    
    def test_record_failure(self):
        """Test failure recording increases delay."""
        initial = self.limiter.current_delay
        self.limiter.record_failure()
        self.assertGreater(self.limiter.current_delay, initial)
    
    def test_failure_threshold(self):
        """Test delay doesn't exceed max."""
        for _ in range(10):
            self.limiter.record_failure()
        self.assertEqual(self.limiter.current_delay, 1.0)  # Max
    
    def test_reset(self):
        """Test reset functionality."""
        self.limiter.record_failure()
        self.limiter.record_failure()
        self.limiter.reset()
        self.assertEqual(self.limiter.current_delay, 0.1)
        self.assertEqual(self.limiter.failure_count, 0)


class TestCircuitBreaker(unittest.TestCase):
    """Tests for CircuitBreaker class."""
    
    def test_initial_state(self):
        """Test initial state is closed."""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        self.assertEqual(breaker.state, "closed")
    
    def test_success_transitions(self):
        """Test success doesn't change state."""
        breaker = CircuitBreaker()
        with breaker:
            pass  # Success
        self.assertEqual(breaker.state, "closed")
    
    def test_failure_transitions(self):
        """Test failure count increases."""
        breaker = CircuitBreaker(failure_threshold=3)
        # The context manager catches the exception, so failure_count should be 1
        with breaker:
            raise Exception("Test failure")
        self.assertEqual(breaker.failure_count, 1)
    
    def test_opens_after_threshold(self):
        """Test circuit opens after threshold."""
        breaker = CircuitBreaker(failure_threshold=2)
        
        # First failure
        with breaker:
            raise Exception("Fail 1")
        self.assertEqual(breaker.failure_count, 1)
        self.assertEqual(breaker.state, "closed")
        
        # Second failure - should open circuit
        with breaker:
            raise Exception("Fail 2")
        self.assertEqual(breaker.failure_count, 2)
        self.assertEqual(breaker.state, "open")
        
        # Now should raise CircuitBreakerOpenError
        with self.assertRaises(CircuitBreakerOpenError):
            with breaker:
                pass
    
    def test_reset(self):
        """Test reset functionality."""
        breaker = CircuitBreaker()
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_failure()
        breaker.reset()
        self.assertEqual(breaker.state, "closed")
        self.assertEqual(breaker.failure_count, 0)


class TestDatabasePaths(unittest.TestCase):
    """Tests for database path handling."""
    
    def test_listings_dir_exists(self):
        """Test that LISTINGS_DIR exists."""
        self.assertTrue(os.path.isdir(LISTINGS_DIR))
    
    def test_default_db_path(self):
        """Test default database path format."""
        self.assertTrue(DEFAULT_DB_PATH.endswith('.db'))
        self.assertIn('listings', DEFAULT_DB_PATH)


if __name__ == '__main__':
    unittest.main()

