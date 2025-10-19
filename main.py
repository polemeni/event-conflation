"""
Event Conflation System - Main Entry Point

A cloud-native system that ingests bursty, async events for individual object IDs
and conflates them by waiting until a configurable idle period has passed since
the last event for a given ID before emitting a single downstream record.

This simulates AWS EventBridge inputs and DynamoDB outputs locally.
"""

import logging
import signal
import sys
from src.utils import load_config
from src.ingestion import EventIngestionHandler
from src.conflation import EventConflator
from src.downstream import LogEmitter, DynamoDBEmitter, FileEmitter

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
conflator = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    if conflator:
        conflator.shutdown()
    sys.exit(0)


def create_downstream_emitter(config: dict):
    """
    Create appropriate downstream emitter based on configuration.

    Args:
        config: Configuration dictionary

    Returns:
        DownstreamEmitter instance
    """
    downstream_config = config.get("downstream", {})
    emitter_type = downstream_config.get("type", "log")

    if emitter_type == "dynamodb_simulation":
        # Use file emitter to simulate DynamoDB with local JSON file
        output_file = downstream_config.get("output_file", "conflated_events.json")
        logger.info(f"Using file emitter to simulate DynamoDB output: {output_file}")
        return FileEmitter(output_file=output_file)
    elif emitter_type == "file":
        output_file = downstream_config.get("output_file", "conflated_events.json")
        batch_size = downstream_config.get("batch_size", 1)
        return FileEmitter(output_file=output_file, batch_size=batch_size)
    elif emitter_type == "dynamodb":
        table_name = downstream_config.get("table_name", "conflated_events")
        region = downstream_config.get("region", "us-east-1")
        return DynamoDBEmitter(table_name=table_name, region=region)
    else:
        # Default to log emitter
        return LogEmitter()


def main():
    """Main entry point for the event conflation system."""
    global conflator

    logger.info("Starting Event Conflation System")

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load configuration
    config = load_config()
    conflation_interval = config.get("conflation_interval", 30.0)

    logger.info(f"Conflation interval: {conflation_interval} seconds")

    # Create downstream emitter
    downstream_emitter = create_downstream_emitter(config)

    # Initialize event conflation engine
    conflator = EventConflator(
        conflation_interval=conflation_interval,
        downstream_emitter=downstream_emitter.emit,
    )

    # Initialize event ingestion handler
    ingestion_handler = EventIngestionHandler(max_workers=4)

    # Ingest events from input file
    input_file = config.get("input_file", "input.json")
    events = ingestion_handler.ingest_events_from_file(input_file)

    logger.info(f"Successfully ingested {len(events)} events")

    # Log some statistics about the ingested events
    event_ids = [event.id for event in events]
    unique_ids = set(event_ids)
    logger.info(f"Events span {len(unique_ids)} unique IDs")

    # Process events through conflation engine
    logger.info("Processing events through conflation engine...")

    # Sort events by timestamp to ensure correct first/last timestamp tracking
    events_sorted = sorted(events, key=lambda e: e.timestamp)
    logger.info(f"Processing {len(events_sorted)} events in timestamp order")

    for event in events_sorted:
        conflator.ingest_event(event)

    # Log initial statistics
    stats = conflator.get_stats()
    logger.info(f"Conflation engine stats: {stats}")

    # Keep the system running to allow conflation timers to complete
    logger.info("System running, waiting for conflation timers...")
    try:
        import time

        while True:
            time.sleep(10)  # Check every 10 seconds
            stats = conflator.get_stats()
            if stats["active_timers"] == 0:
                logger.info("All conflation timers completed, shutting down")
                break
            logger.info(
                f"Active timers: {stats['active_timers']}, "
                f"Total processed: {stats['total_events_processed']}, "
                f"Total conflated: {stats['total_conflated_records']}"
            )
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    finally:
        if conflator:
            conflator.shutdown()

        # Flush any remaining records in the downstream emitter
        if "downstream_emitter" in locals() and hasattr(downstream_emitter, "flush"):
            downstream_emitter.flush()
            stats = downstream_emitter.get_stats()
            logger.info(f"Downstream emitter stats: {stats}")

        logger.info("Event Conflation System shutdown complete")


if __name__ == "__main__":
    main()
