# External Sync Service

A comprehensive system-to-system synchronization service that transforms and routes contact records Currently Performs syncing froming internal systems to external platforms with schema transformation, retry logic, and rate limiting.

## Overview

This service handles real-time synchronization of contact records between internal databases and external systems (Salesforce, HubSpot). It processes streaming events, transforms schemas, and dispatches data to appropriate external systems based on contact type.

## Architecture

```
Internal DB → Event Stream → Event Consumer → Schema Transformer → API Dispatcher → External Systems
```

## Key Features

- **Event Streaming**: Consumes contact events from internal database streams
- **Schema Transformation**: Maps internal schemas to external formats using Pydantic models
- **Multi-System Routing**: Routes contacts to different external systems based on type
- **Adapter Pattern**: Consistent interface for multiple external systems
- **Retry Logic**: Exponential backoff with jitter for external API calls
- **Configuration Management**: Flexible routing and system settings

## Quick Start

### Prerequisites

- Python 3.11+
- Virtual environment (recommended)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd system-to-system-record

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Application

```bash
# Terminal 1: Start mock external service
uvicorn mock_external_api:app --reload --port 8000

# Terminal 2: Start main application
python -m app.main
```
# Run with custom configuration
export EVENTS_PER_SECOND=10
export BATCH_SIZE=5
export ENABLE_ASYNC=true
export LOG_LEVEL=INFO
python -m app.main
```

### Mock External Service

To test the full system with external API calls, run the mock external service:

```bash
# Start the mock external API service using uvicorn
uvicorn mock_external_api:app --reload --port 8000
```

This starts a local FastAPI server that simulates Salesforce and HubSpot APIs, allowing you to test the complete data flow without connecting to real external systems.

**Mock Service Features:**
- **Salesforce API**: Simulates Salesforce REST API endpoints
- **HubSpot API**: Simulates HubSpot API with properties structure
- **Rate Limiting**: Configurable rate limits to test retry logic

**Default Configuration:**
- **Port**: 8000
- **Salesforce Endpoint**: `http://localhost:8000/salesforce`
- **HubSpot Endpoint**: `http://localhost:8000/hubspot`
- **Rate Limit**: 5 requests per second per system




## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_PER_SECOND` | 5 | Number of events generated per second |
| `BATCH_SIZE` | 10 | Number of events per batch |
| `ENABLE_ASYNC` | true | Use async processing |
| `LOG_LEVEL` | INFO | Logging level |
| `MOCK_MODE` | true | Use mock external APIs |

### Contact Type Routing

**Default Configuration:**
- **Salesforce**: lead, prospect, employee
- **HubSpot**: customer, partner, vendor



# Salesforce-only configuration
update_routing_config({
    "lead": "salesforce",
    "customer": "salesforce", 
    "prospect": "salesforce",
    "vendor": "salesforce",
    "partner": "salesforce",
    "employee": "salesforce"
})
```

## Data Flow

### 1. Event Generation
Mock stream generator creates realistic contact events:
```json
{
  "record": "contacts",
  "operation": "create",
  "timestamp": "2024-01-15T10:30:00.123456",
  "item": {
    "id": "A1234",
    "name": "Alice Smith",
    "email": "alice.smith@example.com",
    "phone": "+1-555-123-4567",
    "contact_type": "lead",
    "created_at": "2024-01-15T10:30:00.123456",
    "updated_at": "2024-01-15T10:30:00.123456"
  }
}
```

### 2. Event Processing
- **Validation**: Events are validated against internal schema
- **Filtering**: Non-contact events are filtered out
- **Queue Pipeline**: Events flow through three queues:
  - `raw_events_queue`: Fresh events from stream
  - `processed_events_queue`: Validated events
  - `transformed_events_queue`: Schema-transformed events

### 3. Schema Transformation
Pydantic models handle transformation between schemas:
- **Internal Schema**: Standardized internal contact format
- **Salesforce Schema**: Salesforce-specific field mappings
- **HubSpot Schema**: HubSpot properties structure

### 4. API Dispatch
- **Adapter Selection**: Based on contact type routing
- **Rate Limiting**: Configurable limits per external system
- **Retry Logic**: Exponential backoff with jitter
- **Error Handling**: Graceful degradation and logging

## Project Structure

```
app/
├── main.py                      # Application entry point
├── adapters/                    # External system adapters
│   ├── base_adapter.py         # Abstract adapter interface
│   ├── salesforce_adapter.py   # Salesforce integration
│   └── hubspot_adapter.py      # HubSpot integration
├── models/                      # Pydantic data models
│   ├── internal_schema.py      # Internal contact schema
│   ├── salesforce_schema.py    # Salesforce schema
│   └── hubspot_schema.py       # HubSpot schema
├── services/                    # Core business logic
│   ├── mock_stream_generator.py     # Event generation
│   ├── event_consumer_service.py    # Event processing
│   ├── schema_transformer.py        # Schema transformation
│   ├── api_dispatcher_service.py    # External API calls
│   └── adapter_manager.py           # Adapter lifecycle
├── utils/                       # Utilities
│   ├── config_manager.py       # Configuration management
│   └── external_config.py      # External system routing
└── __init__.py
```

## API Usage

### Basic Service Usage

```python
from app.services.mock_stream_generator import MockStreamGenerator
from app.services.event_consumer_service import InternalEventConsumer
from app.services.schema_transformer import SchemaTransformer
from app.services.api_dispatcher_service import APIDispatcherService

# Create services
consumer = InternalEventConsumer()
transformer = SchemaTransformer()
dispatcher = APIDispatcherService()

# Generate and process events
generator = MockStreamGenerator(consumer)
await generator.start_streaming()
```

### Configuration Management

```python
from app.utils.config_manager import update_config, get_system_settings
from app.utils.external_config import update_routing_config

# Update system settings
update_config(events_per_second=20, batch_size=10)

# Update routing configuration
update_routing_config({
    "lead": "salesforce",
    "customer": "hubspot"
})


### Adding New External Systems

1. **Create Adapter**: Extend `BaseAdapter` in `adapters/`
2. **Define Schema**: Create Pydantic model in `models/`
3. **Update Routing**: Add to routing configuration
4. **Test Integration**: Verify with mock data

### Adding New Contact Types

1. **Update Enum**: Add to `ContactType` in models
2. **Update Routing**: Configure which system handles the type
3. **Test Generation**: Verify mock generator creates correct data

## Design Decisions

### Architecture Choices

- **Pipeline Architecture**: Multi-stage processing with queues for scalability
- **Async Processing**: Non-blocking I/O for better performance
- **Adapter Pattern**: Consistent interface across external systems
- **Pydantic Models**: Strong typing and validation
- **Event-Driven**: Audit trail and replay capability

