"""
Comprehensive test suite for the Event Conflation System.

Tests cover:
1. Single and multiple events per ID
2. Bursty vs spaced-out events
3. Error scenarios (malformed events, out of order events)
4. Clock skew handling
5. System integration
"""

import pytest
import json
import tempfile
import os
import sys
import time
from datetime import datetime, timezone, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import load_config, parse_timestamp
from src.ingestion import EventIngestionHandler
from src.conflation import EventConflator
from src.downstream import FileEmitter, LogEmitter
from src.dataclass import Event, ConflatedRecord


class TestSingleEvents:
    """Test cases for single events per ID."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conflator = EventConflator(conflation_interval=0.1)
        self.ingestion_handler = EventIngestionHandler(max_workers=2)

    def test_single_event_per_id(self):
        """Test conflation with single events per ID."""
        events_data = [
            {
                "id": "user_001",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "login"},
            },
            {
                "id": "user_002",
                "timestamp": "2025-01-01T14:23:10Z",
                "payload": {"action": "view_page"},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            assert len(events) == 2

            # Process through conflation engine
            for event in events:
                self.conflator.ingest_event(event)

            # Wait for conflation
            time.sleep(0.2)

            # Check stats
            stats = self.conflator.get_stats()
            assert stats["total_events_processed"] == 2
            assert stats["total_conflated_records"] == 2
            assert stats["active_timers"] == 0

        finally:
            os.unlink(temp_path)


class TestMultipleEvents:
    """Test cases for multiple events per ID."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conflator = EventConflator(conflation_interval=0.1)
        self.ingestion_handler = EventIngestionHandler(max_workers=2)

    def test_multiple_events_same_id(self):
        """Test conflation with multiple events for the same ID."""
        events_data = [
            {
                "id": "user_001",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "login"},
            },
            {
                "id": "user_001",
                "timestamp": "2025-01-01T14:23:10Z",
                "payload": {"action": "view_page"},
            },
            {
                "id": "user_001",
                "timestamp": "2025-01-01T14:23:15Z",
                "payload": {"action": "click_button"},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            assert len(events) == 3

            # Process through conflation engine
            for event in events:
                self.conflator.ingest_event(event)

            # Wait for conflation
            time.sleep(0.2)

            # Check stats
            stats = self.conflator.get_stats()
            assert stats["total_events_processed"] == 3
            assert stats["total_conflated_records"] == 1  # All events for same ID
            assert stats["active_timers"] == 0

        finally:
            os.unlink(temp_path)


class TestBurstyEvents:
    """Test cases for bursty event patterns."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conflator = EventConflator(conflation_interval=0.1)
        self.ingestion_handler = EventIngestionHandler(max_workers=2)

    def test_bursty_events(self):
        """Test handling of bursty events (many events in short time)."""
        events_data = [
            {
                "id": "user_burst",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "start"},
            },
            {
                "id": "user_burst",
                "timestamp": "2025-01-01T14:23:05.1Z",
                "payload": {"action": "step1"},
            },
            {
                "id": "user_burst",
                "timestamp": "2025-01-01T14:23:05.2Z",
                "payload": {"action": "step2"},
            },
            {
                "id": "user_burst",
                "timestamp": "2025-01-01T14:23:05.3Z",
                "payload": {"action": "step3"},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            assert len(events) == 4

            # Process through conflation engine
            for event in events:
                self.conflator.ingest_event(event)

            # Wait for conflation
            time.sleep(0.2)

            # Check stats
            stats = self.conflator.get_stats()
            assert stats["total_events_processed"] == 4
            assert stats["total_conflated_records"] == 1  # All events for same ID
            assert stats["active_timers"] == 0

        finally:
            os.unlink(temp_path)


class TestErrorScenarios:
    """Test cases for error scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conflator = EventConflator(conflation_interval=0.1)
        self.ingestion_handler = EventIngestionHandler(max_workers=2)

    def test_malformed_events(self):
        """Test handling of malformed events."""
        events_data = [
            {
                "id": "user_valid",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "valid"},
            },
            {"id": "", "timestamp": "2025-01-01T14:23:10Z", "payload": {}},
            {"timestamp": "2025-01-01T14:23:15Z", "payload": {}},
            {
                "id": "user_invalid_timestamp",
                "timestamp": "invalid-timestamp",
                "payload": {},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            malformed_events = self.ingestion_handler.get_malformed_events()

            # Should only process valid event
            assert len(events) == 1
            assert events[0].id == "user_valid"

            # Should have 3 malformed events
            assert len(malformed_events) == 3

            # Check malformed event details
            malformed_ids = [
                me["event_data"].get("id", "missing") for me in malformed_events
            ]
            assert "" in malformed_ids
            assert "user_invalid_timestamp" in malformed_ids
            assert "missing" in malformed_ids  # Missing ID field

        finally:
            os.unlink(temp_path)

    def test_out_of_order_events(self):
        """Test handling of out-of-order events."""
        events_data = [
            {
                "id": "user_ooo",
                "timestamp": "2025-01-01T14:23:10Z",
                "payload": {"action": "second"},
            },
            {
                "id": "user_ooo",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "first"},
            },
            {
                "id": "user_ooo",
                "timestamp": "2025-01-01T14:23:15Z",
                "payload": {"action": "third"},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            assert len(events) == 3

            # Process through conflation engine
            for event in events:
                self.conflator.ingest_event(event)

            # Wait for conflation
            time.sleep(0.2)

            # Check stats - should handle out-of-order gracefully
            stats = self.conflator.get_stats()
            assert stats["total_events_processed"] == 3
            assert stats["total_conflated_records"] == 1
            assert stats["active_timers"] == 0

        finally:
            os.unlink(temp_path)

    def test_clock_skew_events(self):
        """Test handling of clock skew events."""
        # Create events with large time differences
        base_time = datetime(2025, 1, 1, 14, 23, 5, tzinfo=timezone.utc)

        events = [
            Event(id="user_skew", timestamp=base_time, payload={"action": "first"}),
            Event(
                id="user_skew",
                timestamp=base_time + timedelta(seconds=400),  # 6+ minutes later
                payload={"action": "second"},
            ),
        ]

        # Process events
        for event in events:
            self.conflator.ingest_event(event)

        # Wait for conflation
        time.sleep(0.2)

        # Check stats - should detect clock skew
        stats = self.conflator.get_stats()
        assert stats["total_events_processed"] == 2
        assert stats["clock_skew_events"] == 1  # Should detect clock skew
        assert stats["total_conflated_records"] == 1


class TestSystemIntegration:
    """Test cases for full system integration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conflator = EventConflator(conflation_interval=0.1)
        self.ingestion_handler = EventIngestionHandler(max_workers=2)
        self.file_emitter = FileEmitter(output_file="test_output.json", batch_size=1)

    def teardown_method(self):
        """Clean up test files."""
        if os.path.exists("test_output.json"):
            os.unlink("test_output.json")

    def test_end_to_end_conflation(self):
        """Test complete end-to-end conflation process."""
        events_data = [
            {
                "id": "user_e2e",
                "timestamp": "2025-01-01T14:23:05Z",
                "payload": {"action": "start"},
            },
            {
                "id": "user_e2e",
                "timestamp": "2025-01-01T14:23:10Z",
                "payload": {"action": "middle"},
            },
            {
                "id": "user_e2e",
                "timestamp": "2025-01-01T14:23:15Z",
                "payload": {"action": "end"},
            },
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(events_data, f)
            temp_path = f.name

        try:
            # Set up conflation with file emitter
            self.conflator.downstream_emitter = self.file_emitter.emit

            # Ingest events
            events = self.ingestion_handler.ingest_events_from_file(temp_path)
            assert len(events) == 3

            # Sort events by timestamp to ensure correct first/last timestamp tracking
            events_sorted = sorted(events, key=lambda e: e.timestamp)

            # Process through conflation engine
            for event in events_sorted:
                self.conflator.ingest_event(event)

            # Wait for conflation
            time.sleep(0.2)

            # Flush file emitter
            self.file_emitter.flush()

            # Check output file
            assert os.path.exists("test_output.json")

            with open("test_output.json", "r") as f:
                output_data = json.load(f)

            assert len(output_data) == 1
            record = output_data[0]

            # Verify record structure
            assert record["operation"] == "put_item"
            assert record["table_name"] == "conflated_events"

            item = record["item"]
            assert item["id"]["S"] == "user_e2e"
            assert item["event_count"]["N"] == "3"
            assert item["first_event_timestamp"]["S"] == "2025-01-01T14:23:05+00:00"
            assert item["last_event_timestamp"]["S"] == "2025-01-01T14:23:15+00:00"

        finally:
            os.unlink(temp_path)


class TestTimestampParsing:
    """Test cases for timestamp parsing edge cases."""

    def test_various_timestamp_formats(self):
        """Test parsing of various timestamp formats."""
        # ISO format with Z
        dt1 = parse_timestamp("2025-01-01T14:23:05Z")
        assert dt1.tzinfo == timezone.utc

        # ISO format with timezone
        dt2 = parse_timestamp("2025-01-01T14:23:05+00:00")
        assert dt2.tzinfo == timezone.utc

        # Unix timestamp
        dt3 = parse_timestamp(1704116585.0)
        assert dt3.tzinfo == timezone.utc

        # Datetime object
        dt4 = parse_timestamp(datetime(2025, 1, 1, 14, 23, 5, tzinfo=timezone.utc))
        assert dt4.tzinfo == timezone.utc

    def test_invalid_timestamp_formats(self):
        """Test handling of invalid timestamp formats."""
        with pytest.raises(ValueError, match="Invalid ISO timestamp format"):
            parse_timestamp("invalid-timestamp")

        with pytest.raises(ValueError, match="Unsupported timestamp type"):
            parse_timestamp(["not", "a", "timestamp"])


if __name__ == "__main__":
    pytest.main([__file__])
