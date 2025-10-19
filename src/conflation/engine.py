"""
Event conflation engine implementation.

This module contains the core conflation logic that groups events by ID
and emits conflated records after configurable idle periods.

Designed for scalability:
- Stateless design where possible
- External state management ready
- Timer-based conflation with reset capability
- Thread-safe operations
"""

import logging
import threading
import time
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone

from ..dataclass import Event, ConflatedRecord

logger = logging.getLogger(__name__)


class EventConflator:
    """
    Core conflation engine that manages event batching and idle period tracking.

    This class is designed to be scalable and can be extended for multi-node deployment:
    - Uses external state storage (Redis/DynamoDB) for shared state
    - Implements partitioning by event ID for horizontal scaling
    - Provides hooks for distributed timer management
    - Stateless design where possible
    """

    def __init__(
        self,
        conflation_interval: float = 30.0,
        max_events_per_id: int = 10000,
        downstream_emitter: Optional[Callable[[ConflatedRecord], None]] = None,
        clock_skew_tolerance: float = 300.0,  # 5 minutes default
    ):
        """
        Initialize the event conflator.

        Args:
            conflation_interval: Time in seconds to wait after last event before emitting
            max_events_per_id: Maximum events to track per ID (memory protection)
            downstream_emitter: Optional callback for emitting conflated records
            clock_skew_tolerance: Maximum time difference in seconds to accept for out-of-order events
        """
        self.conflation_interval = conflation_interval
        self.max_events_per_id = max_events_per_id
        self.downstream_emitter = downstream_emitter or self._default_emitter
        self.clock_skew_tolerance = clock_skew_tolerance

        # In-memory state (will be externalized for multi-node deployment)
        self.pending_events: Dict[str, Event] = {}
        self.event_counts: Dict[str, int] = {}
        self.first_event_timestamps: Dict[str, datetime] = {}
        self.timers: Dict[str, threading.Timer] = {}
        self.lock = threading.Lock()

        # Statistics for monitoring
        self.stats = {
            "total_events_processed": 0,
            "total_conflated_records": 0,
            "active_timers": 0,
            "memory_usage_estimate": 0,
            "events_dropped": 0,
            "clock_skew_events": 0,
        }

    def ingest_event(self, event: Event) -> None:
        """
        Ingest a new event and update conflation state.

        This method is designed to be stateless where possible and can be
        extended to work with distributed state management.

        Args:
            event: The event to ingest
        """
        with self.lock:
            logger.debug(f"Ingesting event for ID: {event.id}")

            # Update statistics
            self.stats["total_events_processed"] += 1

            # Check memory limits (protection against memory exhaustion)
            if self.event_counts.get(event.id, 0) >= self.max_events_per_id:
                logger.warning(
                    f"Max events reached for ID {event.id}, forcing conflation"
                )
                self._force_conflation(event.id)

            # Handle clock skew and out-of-order events
            if event.id in self.pending_events:
                existing_event = self.pending_events[event.id]
                time_diff = abs(
                    (event.timestamp - existing_event.timestamp).total_seconds()
                )

                # Check for significant clock skew
                if time_diff > self.clock_skew_tolerance:
                    logger.warning(
                        f"Large time difference detected for ID {event.id}: "
                        f"{time_diff:.2f}s (tolerance: {self.clock_skew_tolerance}s). "
                        f"Existing: {existing_event.timestamp}, New: {event.timestamp}"
                    )
                    self.stats["clock_skew_events"] += 1

                # For out-of-order events, keep the latest timestamp but log the issue
                if event.timestamp < existing_event.timestamp:
                    logger.info(
                        f"Out-of-order event detected for ID {event.id}: "
                        f"New event timestamp {event.timestamp} is before existing "
                        f"{existing_event.timestamp}. Keeping latest event."
                    )

            # Update or create event record
            self.pending_events[event.id] = event
            self.event_counts[event.id] = self.event_counts.get(event.id, 0) + 1

            # Track first event timestamp (only set on first event for this ID)
            if event.id not in self.first_event_timestamps:
                self.first_event_timestamps[event.id] = event.timestamp
                logger.debug(f"Set first timestamp for {event.id}: {event.timestamp}")
            else:
                logger.debug(
                    f"Updated last timestamp for {event.id}: {event.timestamp}"
                )

            # Cancel existing timer if it exists
            if event.id in self.timers:
                self.timers[event.id].cancel()
                self.stats["active_timers"] -= 1

            # Set new timer for conflation
            timer = threading.Timer(
                self.conflation_interval, self._emit_conflated_record, args=[event.id]
            )
            self.timers[event.id] = timer
            timer.start()
            self.stats["active_timers"] += 1

            # Update memory usage estimate
            self._update_memory_estimate()

            logger.debug(
                f"Timer set for ID {event.id}, will emit in {self.conflation_interval}s"
            )

    def _emit_conflated_record(self, event_id: str) -> None:
        """
        Emit a conflated record for the given event ID.

        This method handles the actual conflation and emission process.

        Args:
            event_id: The ID to emit a conflated record for
        """
        with self.lock:
            if event_id not in self.pending_events:
                logger.warning(f"No pending events found for ID: {event_id}")
                return

            event = self.pending_events[event_id]
            total_events = self.event_counts[event_id]
            first_timestamp = self.first_event_timestamps[event_id]

            conflated_record = ConflatedRecord(
                id=event_id,
                first_event_timestamp=first_timestamp,
                last_event_timestamp=event.timestamp,
                total_events=total_events,
                final_payload=event.payload,
                conflation_timestamp=datetime.now(timezone.utc),
            )

            # Clean up state
            del self.pending_events[event_id]
            del self.event_counts[event_id]
            del self.first_event_timestamps[event_id]
            if event_id in self.timers:
                del self.timers[event_id]
                self.stats["active_timers"] -= 1

            # Update statistics
            self.stats["total_conflated_records"] += 1
            self._update_memory_estimate()

            # Emit the record
            self.downstream_emitter(conflated_record)

    def _force_conflation(self, event_id: str) -> None:
        """
        Force conflation for an event ID (used when memory limits are reached).

        Args:
            event_id: The ID to force conflation for
        """
        logger.info(f"Forcing conflation for ID: {event_id}")
        self._emit_conflated_record(event_id)

    def _default_emitter(self, record: ConflatedRecord) -> None:
        """
        Default downstream emitter (logs the record).

        In a production system, this would emit to DynamoDB, Kafka, etc.

        Args:
            record: The conflated record to emit
        """
        logger.info(
            f"Emitting conflated record for ID: {record.id}, "
            f"total events: {record.total_events}, "
            f"last timestamp: {record.last_event_timestamp}"
        )

    def _update_memory_estimate(self) -> None:
        """Update memory usage estimate for monitoring."""
        # Rough estimate: events + counts + first timestamps + timers
        self.stats["memory_usage_estimate"] = (
            len(self.pending_events) * 100  # Rough bytes per event
            + len(self.event_counts) * 20  # Rough bytes per count
            + len(self.first_event_timestamps) * 30  # Rough bytes per timestamp
            + len(self.timers) * 50  # Rough bytes per timer
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get current statistics for monitoring.

        Returns:
            Dictionary of statistics
        """
        with self.lock:
            return self.stats.copy()

    def get_active_ids(self) -> list[str]:
        """
        Get list of currently active event IDs.

        Returns:
            List of event IDs with pending events
        """
        with self.lock:
            return list(self.pending_events.keys())

    def force_conflation_all(self) -> None:
        """
        Force conflation for all pending events.

        Useful for graceful shutdown or memory pressure.
        """
        with self.lock:
            active_ids = list(self.pending_events.keys())

        for event_id in active_ids:
            self._force_conflation(event_id)

    def shutdown(self) -> None:
        """
        Gracefully shutdown the conflator.

        Forces conflation of all pending events and cancels timers.
        """
        logger.info("Shutting down EventConflator")

        # Cancel all timers
        with self.lock:
            for timer in self.timers.values():
                timer.cancel()
            self.timers.clear()
            self.stats["active_timers"] = 0

        # Force conflation of all pending events
        self.force_conflation_all()

        logger.info("EventConflator shutdown complete")


# Multi-Node Deployment Design Documentation
"""
SCALABILITY AND REDUNDANCY DESIGN

1. HORIZONTAL SCALING STRATEGY:
   - Event partitioning by ID hash (consistent hashing)
   - Each node handles a subset of event IDs
   - Load balancer distributes events based on ID hash
   - External state storage (Redis/DynamoDB) for shared state

2. STATE MANAGEMENT:
   - Current: In-memory state (single node)
   - Future: External state storage with Redis/DynamoDB
   - State includes: pending_events, event_counts, active_timers
   - Atomic operations for state updates

3. TIMER MANAGEMENT:
   - Current: Threading.Timer (single node)
   - Future: Distributed timer service (Redis with TTL, or dedicated service)
   - Timer persistence across node failures
   - Timer coordination between nodes

4. FAILURE HANDLING:
   - Node failure: Other nodes take over via consistent hashing
   - State recovery: Rebuild state from external storage
   - Timer recovery: Recreate timers from persisted state
   - Graceful degradation: Continue processing with reduced capacity

5. LOAD BALANCING:
   - Consistent hashing for event ID distribution
   - Health checks for node availability
   - Automatic failover on node failure
   - Dynamic node addition/removal

6. MONITORING AND OBSERVABILITY:
   - Metrics: events processed, conflation rate, memory usage
   - Health endpoints for load balancer
   - Distributed tracing for event flow
   - Alerting on node failures or high memory usage

7. DEPLOYMENT PATTERNS:
   - Container-based deployment (Docker/Kubernetes)
   - Auto-scaling based on event volume
   - Rolling deployments for zero downtime
   - Blue-green deployments for major updates
"""
