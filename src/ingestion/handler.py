"""
Event ingestion handler implementation.

This module contains the EventIngestionHandler class for concurrent
event processing from various sources.
"""

import json
import logging
import threading
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from ..dataclass import Event
from ..utils import parse_timestamp

logger = logging.getLogger(__name__)


class EventIngestionHandler:
    """
    Handles concurrent ingestion of events from various sources.
    """

    def __init__(self, max_workers: int = 4):
        """
        Initialize the event ingestion handler.

        Args:
            max_workers: Maximum number of concurrent worker threads
        """
        self.max_workers = max_workers
        self.ingested_events: List[Event] = []
        self.lock = threading.Lock()
        self.malformed_events: List[Dict[str, Any]] = []

    def _ingest_single_event(self, event_data: Dict[str, Any]) -> Event:
        """
        Ingest a single event from raw data.

        Args:
            event_data: Raw event data dictionary

        Returns:
            Event object

        Raises:
            ValueError: If event data is invalid
        """
        try:
            # Validate required fields
            if not isinstance(event_data, dict):
                raise ValueError(
                    f"Event data must be a dictionary, got {type(event_data)}"
                )

            if "id" not in event_data:
                raise ValueError("Event data missing required field: 'id'")

            if "timestamp" not in event_data:
                raise ValueError("Event data missing required field: 'timestamp'")

            if "payload" not in event_data:
                raise ValueError("Event data missing required field: 'payload'")

            # Validate ID is not empty
            if not event_data["id"] or not str(event_data["id"]).strip():
                raise ValueError("Event ID cannot be empty")

            # Parse and validate timestamp
            try:
                timestamp = parse_timestamp(event_data["timestamp"])
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {e}")

            # Validate payload is a dictionary
            if not isinstance(event_data["payload"], dict):
                raise ValueError(
                    f"Event payload must be a dictionary, got {type(event_data['payload'])}"
                )

            event = Event(
                id=str(event_data["id"]).strip(),
                timestamp=timestamp,
                payload=event_data["payload"],
            )

            with self.lock:
                self.ingested_events.append(event)

            logger.debug(f"Ingested event: {event.id} at {event.timestamp}")
            return event

        except KeyError as e:
            raise ValueError(f"Invalid event data, missing key: {e}")
        except Exception as e:
            raise ValueError(f"Failed to process event data: {e}")

    def ingest_events_from_file(self, file_path: str) -> List[Event]:
        """
        Ingest events from a JSON file concurrently.

        Args:
            file_path: Path to the JSON file containing events

        Returns:
            List of ingested Event objects
        """
        logger.info(f"Starting concurrent ingestion from {file_path}")

        try:
            with open(file_path, "r") as f:
                events_data = json.load(f)

            if not isinstance(events_data, list):
                logger.error("Input file must contain a list of events")
                return []

            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all events for concurrent processing
                future_to_event = {
                    executor.submit(self._ingest_single_event, event_data): event_data
                    for event_data in events_data
                }

                # Collect results as they complete
                successful_events = []
                failed_events = []

                for future in as_completed(future_to_event):
                    event_data = future_to_event[future]
                    try:
                        event = future.result()
                        successful_events.append(event)
                    except Exception as e:
                        event_id = event_data.get("id", "unknown")
                        logger.error(f"Failed to ingest event {event_id}: {e}")
                        failed_events.append(event_data)

                        # Store malformed event for analysis
                        with self.lock:
                            self.malformed_events.append(
                                {
                                    "event_data": event_data,
                                    "error": str(e),
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                }
                            )

                logger.info(
                    f"Ingestion complete: {len(successful_events)} successful, {len(failed_events)} failed"
                )
                return successful_events

        except FileNotFoundError:
            logger.error(f"Input file not found: {file_path}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in input file: {e}")
            return []

    def get_ingested_events(self) -> List[Event]:
        """
        Get all ingested events.

        Returns:
            List of all ingested Event objects
        """
        with self.lock:
            return self.ingested_events.copy()

    def clear_ingested_events(self) -> None:
        """Clear all ingested events."""
        with self.lock:
            self.ingested_events.clear()

    def get_malformed_events(self) -> List[Dict[str, Any]]:
        """
        Get all malformed events that failed to ingest.

        Returns:
            List of malformed event records with error details
        """
        with self.lock:
            return self.malformed_events.copy()

    def clear_malformed_events(self) -> None:
        """Clear all malformed event records."""
        with self.lock:
            self.malformed_events.clear()
