"""
Timestamp utility functions.

This module provides functions for parsing and handling timestamps
in various formats.
"""

from typing import Union
from datetime import datetime, timezone


def parse_timestamp(timestamp_input: Union[str, float, datetime]) -> datetime:
    """
    Parse timestamp from various input formats.

    Args:
        timestamp_input: Timestamp as string (ISO format), float (Unix timestamp), or datetime

    Returns:
        datetime object in UTC timezone

    Raises:
        ValueError: If timestamp format is invalid
    """
    if isinstance(timestamp_input, datetime):
        # Ensure it's timezone-aware
        if timestamp_input.tzinfo is None:
            return timestamp_input.replace(tzinfo=timezone.utc)
        return timestamp_input

    elif isinstance(timestamp_input, str):
        # Parse ISO format string
        try:
            # Handle ISO format with 'Z' suffix
            if timestamp_input.endswith("Z"):
                timestamp_input = timestamp_input[:-1] + "+00:00"

            dt = datetime.fromisoformat(timestamp_input)

            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt
        except ValueError as e:
            raise ValueError(f"Invalid ISO timestamp format: {timestamp_input}") from e

    elif isinstance(timestamp_input, (int, float)):
        # Parse Unix timestamp
        return datetime.fromtimestamp(timestamp_input, tz=timezone.utc)

    else:
        raise ValueError(f"Unsupported timestamp type: {type(timestamp_input)}")
