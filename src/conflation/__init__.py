"""
Event conflation module for the Event Conflation System.

This module handles the core conflation logic, grouping events by ID
and emitting conflated records after configurable idle periods.
"""

from .engine import EventConflator

__all__ = ["EventConflator"]
