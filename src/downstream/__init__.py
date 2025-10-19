"""
Downstream emission module for the Event Conflation System.

This module handles emission of conflated records to various downstream systems.
"""

from .emitter import DownstreamEmitter, DynamoDBEmitter, LogEmitter, FileEmitter

__all__ = ["DownstreamEmitter", "DynamoDBEmitter", "LogEmitter", "FileEmitter"]
