"""
Dataclasses for the Event Conflation System.

This module defines the core data structures used throughout the system.
"""

from dataclasses import dataclass
from typing import Dict, Any
from datetime import datetime


@dataclass
class Event:
    """
    Represents an incoming event with ID, timestamp, and payload.

    Attributes:
        id: Unique identifier for the event
        timestamp: When the event occurred (timezone-aware datetime)
        payload: Event data as a dictionary
    """

    id: str
    timestamp: datetime
    payload: Dict[str, Any]


@dataclass
class ConflatedRecord:
    """
    Represents a conflated record ready for downstream processing.

    This record is created after the conflation interval has passed
    without new events for a given ID.

    Attributes:
        id: The original event ID
        first_event_timestamp: Timestamp of the first event for this ID
        last_event_timestamp: Timestamp of the last event for this ID
        total_events: Number of events that were conflated
        final_payload: Payload from the most recent event
        conflation_timestamp: When the conflation was completed
    """

    id: str
    first_event_timestamp: datetime
    last_event_timestamp: datetime
    total_events: int
    final_payload: Dict[str, Any]
    conflation_timestamp: datetime
