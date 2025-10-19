"""
Event ingestion module for the Event Conflation System.

This module handles concurrent ingestion of events from various sources.
"""

from .handler import EventIngestionHandler

__all__ = ["EventIngestionHandler"]
