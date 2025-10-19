"""
Downstream emission implementations.

This module provides various emitters for sending conflated records
to downstream systems like DynamoDB, Kafka, etc.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

from ..dataclass import ConflatedRecord

logger = logging.getLogger(__name__)


class DownstreamEmitter(ABC):
    """
    Abstract base class for downstream emitters.

    This design allows for easy extension to different downstream systems
    and supports the multi-node deployment architecture.
    """

    @abstractmethod
    def emit(self, record: ConflatedRecord) -> bool:
        """
        Emit a conflated record to the downstream system.

        Args:
            record: The conflated record to emit

        Returns:
            True if emission was successful, False otherwise
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the downstream system is healthy.

        Returns:
            True if healthy, False otherwise
        """
        pass


class LogEmitter(DownstreamEmitter):
    """
    Simple log-based emitter for development and testing.

    This emitter logs conflated records to the application log.
    """

    def __init__(self, log_level: int = logging.INFO):
        """
        Initialize the log emitter.

        Args:
            log_level: Logging level for emitted records
        """
        self.log_level = log_level
        self.logger = logging.getLogger(f"{__name__}.LogEmitter")

    def emit(self, record: ConflatedRecord) -> bool:
        """
        Emit a conflated record to the log.

        Args:
            record: The conflated record to emit

        Returns:
            Always True (logging rarely fails)
        """
        self.logger.log(
            self.log_level,
            f"CONFLATED_RECORD: id={record.id}, "
            f"total_events={record.total_events}, "
            f"last_timestamp={record.last_event_timestamp}, "
            f"conflation_timestamp={record.conflation_timestamp}, "
            f"payload={record.final_payload}",
        )
        return True

    def health_check(self) -> bool:
        """
        Check if logging is healthy.

        Returns:
            Always True (logging is always available)
        """
        return True


class DynamoDBEmitter(DownstreamEmitter):
    """
    DynamoDB emitter for production use.

    This emitter sends conflated records to a DynamoDB table.
    Designed for scalability and redundancy.
    """

    def __init__(
        self,
        table_name: str,
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """
        Initialize the DynamoDB emitter.

        Args:
            table_name: Name of the DynamoDB table
            region: AWS region
            aws_access_key_id: AWS access key (optional, can use IAM roles)
            aws_secret_access_key: AWS secret key (optional, can use IAM roles)
        """
        self.table_name = table_name
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # DynamoDB client will be initialized lazily
        self._client = None
        self._healthy = True

    def _get_client(self):
        """Get or create DynamoDB client."""
        if self._client is None:
            try:
                import boto3

                # Create DynamoDB client
                if self.aws_access_key_id and self.aws_secret_access_key:
                    self._client = boto3.client(
                        "dynamodb",
                        region_name=self.region,
                        aws_access_key_id=self.aws_access_key_id,
                        aws_secret_access_key=self.aws_secret_access_key,
                    )
                else:
                    # Use default credentials (IAM roles, environment variables, etc.)
                    self._client = boto3.client("dynamodb", region_name=self.region)

                logger.info(f"DynamoDB client initialized for table: {self.table_name}")

            except ImportError:
                logger.error("boto3 not available, DynamoDB emitter will fail")
                self._client = None
            except Exception as e:
                logger.error(f"Failed to initialize DynamoDB client: {e}")
                self._client = None

        return self._client

    def emit(self, record: ConflatedRecord) -> bool:
        """
        Emit a conflated record to DynamoDB.

        Args:
            record: The conflated record to emit

        Returns:
            True if emission was successful, False otherwise
        """
        client = self._get_client()
        if not client:
            logger.error("DynamoDB client not available")
            return False

        try:
            # Convert record to DynamoDB item format
            item = {
                "id": {"S": record.id},
                "last_event_timestamp": {"S": record.last_event_timestamp.isoformat()},
                "total_events": {"N": str(record.total_events)},
                "conflation_timestamp": {"S": record.conflation_timestamp.isoformat()},
                "payload": {"S": str(record.final_payload)},  # Simplified for demo
            }

            # Put item to DynamoDB
            response = client.put_item(TableName=self.table_name, Item=item)

            logger.debug(f"Successfully emitted record for ID: {record.id}")
            self._healthy = True
            return True

        except Exception as e:
            logger.error(f"Failed to emit record to DynamoDB: {e}")
            self._healthy = False
            return False

    def health_check(self) -> bool:
        """
        Check if DynamoDB is healthy.

        Returns:
            True if healthy, False otherwise
        """
        client = self._get_client()
        if not client:
            return False

        try:
            # Simple health check - describe table
            client.describe_table(TableName=self.table_name)
            self._healthy = True
            return True
        except Exception as e:
            logger.warning(f"DynamoDB health check failed: {e}")
            self._healthy = False
            return False


class FileEmitter(DownstreamEmitter):
    """
    File-based emitter that simulates DynamoDB put_item operations.

    This emitter writes conflated records to a local JSON file,
    mimicking DynamoDB's behavior for development and testing.
    Designed for scalability and can be extended for multi-node deployment.
    """

    def __init__(
        self,
        output_file: str = "conflated_events.json",
        append_mode: bool = True,
        batch_size: int = 100,
    ):
        """
        Initialize the file emitter.

        Args:
            output_file: Path to the output JSON file
            append_mode: Whether to append to existing file or overwrite
            batch_size: Number of records to batch before writing
        """
        self.output_file = output_file
        self.append_mode = append_mode
        self.batch_size = batch_size
        self._buffer = []
        self._healthy = True
        self._records_written = 0

        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

    def _convert_to_dynamodb_format(self, record: ConflatedRecord) -> Dict[str, Any]:
        """
        Convert ConflatedRecord to DynamoDB item format.

        Args:
            record: The conflated record to convert

        Returns:
            Dictionary in DynamoDB item format
        """
        return {
            "id": {"S": record.id},
            "first_event_timestamp": {"S": record.first_event_timestamp.isoformat()},
            "last_event_timestamp": {"S": record.last_event_timestamp.isoformat()},
            "event_count": {"N": str(record.total_events)},
            "final_payload": {"S": json.dumps(record.final_payload)},
            "conflation_timestamp": {"S": record.conflation_timestamp.isoformat()},
        }

    def _write_buffer(self) -> bool:
        """
        Write buffered records to file.

        Returns:
            True if successful, False otherwise
        """
        if not self._buffer:
            return True

        try:
            # Prepare data for writing
            data_to_write = []
            for record in self._buffer:
                dynamodb_item = self._convert_to_dynamodb_format(record)
                data_to_write.append(
                    {
                        "operation": "put_item",
                        "table_name": "conflated_events",
                        "item": dynamodb_item,
                        "timestamp": datetime.now().isoformat(),
                    }
                )

            # Write to file
            if self.append_mode and os.path.exists(self.output_file):
                with open(self.output_file, "r") as f:
                    try:
                        existing_data = json.load(f)
                        if not isinstance(existing_data, list):
                            existing_data = []
                    except (json.JSONDecodeError, ValueError):
                        existing_data = []
            else:
                existing_data = []

            # Append new data
            existing_data.extend(data_to_write)

            with open(self.output_file, "w") as f:
                json.dump(existing_data, f, indent=2)

            # Update statistics
            self._records_written += len(self._buffer)
            self._buffer.clear()

            logger.debug(f"Wrote {len(data_to_write)} records to {self.output_file}")
            self._healthy = True
            return True

        except Exception as e:
            logger.error(f"Failed to write to file {self.output_file}: {e}")
            self._healthy = False
            return False

    def emit(self, record: ConflatedRecord) -> bool:
        """
        Emit a conflated record to file.

        Args:
            record: The conflated record to emit

        Returns:
            True if emission was successful, False otherwise
        """
        try:
            # Add to buffer
            self._buffer.append(record)

            # Write if buffer is full
            if len(self._buffer) >= self.batch_size:
                return self._write_buffer()

            logger.debug(f"Buffered record for ID: {record.id}")
            self._healthy = True
            return True

        except Exception as e:
            logger.error(f"Failed to emit record to file: {e}")
            self._healthy = False
            return False

    def flush(self) -> bool:
        """
        Flush any remaining buffered records to file.

        Returns:
            True if successful, False otherwise
        """
        return self._write_buffer()

    def health_check(self) -> bool:
        """
        Check if file emission is healthy.

        Returns:
            True if healthy, False otherwise
        """
        try:
            # Try to write a test record to check file accessibility
            test_file = f"{self.output_file}.health_check"
            with open(test_file, "w") as f:
                json.dump({"health_check": True}, f)
            os.remove(test_file)
            self._healthy = True
            return True
        except Exception as e:
            logger.warning(f"File emitter health check failed: {e}")
            self._healthy = False
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get emission statistics.

        Returns:
            Dictionary of statistics
        """
        return {
            "records_written": self._records_written,
            "buffer_size": len(self._buffer),
            "output_file": self.output_file,
            "healthy": self._healthy,
        }


# Multi-Node Deployment Design for Downstream Emission
"""
DOWNSTREAM EMISSION SCALABILITY DESIGN

1. LOAD DISTRIBUTION:
   - Multiple emitter instances across nodes
   - Round-robin or hash-based distribution
   - Connection pooling for database connections
   - Batch emission for high throughput

2. FAILURE HANDLING:
   - Retry logic with exponential backoff
   - Dead letter queues for failed emissions
   - Circuit breaker pattern for downstream failures
   - Graceful degradation when downstream is unavailable

3. MONITORING:
   - Emission success/failure rates
   - Latency metrics for downstream calls
   - Health check endpoints
   - Alerting on emission failures

4. CONFIGURATION:
   - Environment-based configuration
   - Dynamic configuration updates
   - A/B testing for different emission strategies
   - Feature flags for emission behavior

5. SECURITY:
   - IAM roles for AWS services
   - Encryption in transit and at rest
   - VPC endpoints for private communication
   - Audit logging for compliance

6. PERFORMANCE OPTIMIZATION:
   - Connection pooling
   - Batch operations where possible
   - Async emission for non-blocking operation
   - Compression for large payloads
"""
