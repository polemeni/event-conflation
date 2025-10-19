# Event Conflation System

A Python-based, cloud-native system that ingests bursty, asynchronous events for individual object IDs and "conflates" them - waiting until a configurable idle period has passed since the last event for a given ID before emitting a single downstream record. This system simulates AWS EventBridge inputs and DynamoDB outputs locally.

## Features

- **Event Conflation**: Groups events by ID and emits a single record after a configurable idle period
- **Concurrent Processing**: Handles high-volume event ingestion with configurable worker threads
- **Error Handling**: Robust handling of malformed events, clock skew, and out-of-order timestamps
- **Cloud-Native Design**: Built for scalability and multi-node deployment
- **DynamoDB Simulation**: Local file-based simulation of DynamoDB `put_item` operations
- **Comprehensive Testing**: Full test coverage including error scenarios and edge cases

## System Design

### Architecture Overview

The system is designed with scalability and reliability in mind:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Input Source  │───▶│  Event Ingestion │───▶│ Conflation Engine│───▶│ Downstream Emit │
│  (JSON Files)   │    │     Handler      │    │                 │    │  (DynamoDB Sim) │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

### Scalability & Multi-Node Deployment

The system is designed for horizontal scaling:

- **Event Partitioning**: Events are partitioned by ID, allowing different nodes to handle different ID ranges
- **Stateless Components**: Ingestion and conflation logic can be distributed across multiple nodes
- **External State Management**: Timer state and pending events can be stored in external systems (Redis, DynamoDB)
- **Load Balancing**: Events can be distributed across nodes using consistent hashing
- **Message Queues**: Integration points for Kafka, SQS, or other message brokers

### Reliability & Failure Handling

- **Graceful Degradation**: System continues operating even with partial failures
- **No Single Points of Failure**: All components can be replicated
- **State Recovery**: Timer state can be reconstructed from event logs
- **Health Monitoring**: Built-in health checks and statistics
- **Memory Protection**: Configurable limits prevent memory exhaustion

### Clock Skew Handling

The system handles clock skew and out-of-order events:

- **Tolerance Window**: Configurable clock skew tolerance (default: 5 minutes)
- **Out-of-Order Detection**: Logs and handles events arriving out of chronological order
- **Timestamp Validation**: Ensures timestamps are within acceptable ranges
- **Graceful Processing**: Continues processing while logging anomalies

## Project Structure

```
event-conflation/
├── src/
│   ├── dataclass/          # Data models (Event, ConflatedRecord)
│   ├── utils/              # Utility functions (config, timestamp parsing)
│   ├── ingestion/          # Event ingestion and validation
│   ├── conflation/         # Core conflation logic and timers
│   └── downstream/         # Output emitters (file, log, DynamoDB)
├── tests/                  # Comprehensive test suite
├── test_data/              # Test event files
├── main.py                 # Main application entry point
├── config.json             # Configuration file
├── input.json              # Sample input events
└── requirements.txt        # Python dependencies
```

## Quick Start

### Prerequisites

- Python 3.8+
- pip

### Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd event-conflation
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

### Configuration

Edit `config.json` to customize system behavior:

```json
{
  "conflation_interval": 30.0,
  "input_file": "input.json",
  "log_level": "INFO",
  "downstream": {
    "type": "dynamodb_simulation",
    "table_name": "conflated_events",
    "region": "us-east-1",
    "output_file": "conflated_events.json",
    "batch_size": 10
  }
}
```

### Running the System

1. **Prepare input events** in `input.json`:

```json
[
  {
    "id": "user_123",
    "timestamp": "2025-01-01T14:23:05Z",
    "payload": { "action": "login" }
  },
  {
    "id": "user_123",
    "timestamp": "2025-01-01T14:23:10Z",
    "payload": { "action": "view_page" }
  }
]
```

2. **Run the system**:

```bash
python main.py
```

3. **Check output** in `conflated_events.json`:

```json
[
  {
    "operation": "put_item",
    "table_name": "conflated_events",
    "item": {
      "id": { "S": "user_123" },
      "first_event_timestamp": { "S": "2025-01-01T14:23:05+00:00" },
      "last_event_timestamp": { "S": "2025-01-01T14:23:10+00:00" },
      "event_count": { "N": "2" },
      "final_payload": { "S": "{\"action\": \"view_page\"}" },
      "conflation_timestamp": { "S": "2025-01-01T14:23:40.123456+00:00" }
    },
    "timestamp": "2025-01-01T14:23:40.123456"
  }
]
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/test_conflation_system.py -v  # Comprehensive system tests
pytest tests/test_main.py -v               # Unit tests
```

### Test Coverage

The test suite covers:

1. **Single Events**: One event per ID
2. **Multiple Events**: Multiple events for the same ID
3. **Bursty Events**: High-frequency events in short time periods
4. **Error Scenarios**:
   - Malformed JSON events
   - Missing required fields
   - Invalid timestamp formats
   - Out-of-order events
   - Clock skew detection
5. **System Integration**: End-to-end conflation process
6. **Edge Cases**: Various timestamp formats and data types

## Configuration Options

### Core Settings

- `conflation_interval`: Time in seconds to wait after last event before emitting (default: 30.0)
- `input_file`: Path to input JSON file (default: "input.json")
- `log_level`: Logging level (DEBUG, INFO, WARNING, ERROR)

### Downstream Settings

- `type`: Emitter type ("dynamodb_simulation", "log", "file")
- `output_file`: Output file path for file-based emitters
- `batch_size`: Number of records to buffer before writing
- `table_name`: DynamoDB table name for simulation

### Advanced Settings

- `max_events_per_id`: Memory protection limit (default: 10000)
- `clock_skew_tolerance`: Maximum acceptable time difference in seconds (default: 300)
- `max_workers`: Number of concurrent ingestion threads (default: 4)

## Event Format

### Input Events

```json
{
  "id": "string", // Required: Unique identifier
  "timestamp": "string", // Required: ISO 8601 timestamp or Unix timestamp
  "payload": {} // Required: Event data (must be object)
}
```

### Output Records

```json
{
  "operation": "put_item",
  "table_name": "conflated_events",
  "item": {
    "id": { "S": "event_id" },
    "first_event_timestamp": { "S": "2025-01-01T14:23:05+00:00" },
    "last_event_timestamp": { "S": "2025-01-01T14:23:10+00:00" },
    "event_count": { "N": "2" },
    "final_payload": { "S": "{\"action\": \"view_page\"}" },
    "conflation_timestamp": { "S": "2025-01-01T14:23:40.123456+00:00" }
  },
  "timestamp": "2025-01-01T14:23:40.123456"
}
```

## Assumptions & Limitations

### Current Assumptions

1. **Single Process**: Current implementation runs in a single process
2. **Local Storage**: State is stored in memory (not persistent across restarts)
3. **File Input**: Events are read from local JSON files
4. **Synchronous Processing**: Events are processed in timestamp order

### Production Considerations

1. **Persistence**: Timer state should be stored in external systems (Redis, DynamoDB)
2. **Message Queues**: Use Kafka, SQS, or similar for event ingestion
3. **Monitoring**: Add metrics collection (Prometheus, CloudWatch)
4. **Configuration**: Use environment variables or external config services
5. **Security**: Add authentication and authorization for production use

## Development

### Code Organization

- **Modular Design**: Each component is in its own module
- **Type Hints**: Full type annotation coverage
- **Error Handling**: Comprehensive error handling and logging
- **Documentation**: Detailed docstrings and comments

### Adding New Features

1. **New Emitters**: Extend `DownstreamEmitter` base class
2. **New Input Sources**: Extend `EventIngestionHandler`
3. **New Conflation Logic**: Modify `EventConflator` class
4. **New Data Types**: Add to `dataclass` module

## Future Enhancements

- **Real DynamoDB Integration**: Replace simulation with actual DynamoDB client
- **Kafka Integration**: Add Kafka consumer for real-time event ingestion
- **Metrics & Monitoring**: Add Prometheus metrics and health endpoints
- **Configuration Management**: Add support for environment-based configuration
- **Docker Support**: Add containerization for easy deployment
- **CI/CD Pipeline**: Add automated testing and deployment

## License

[Add your license information here]
