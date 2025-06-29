# System-to-System Record Sync Service

A comprehensive sync service that synchronizes records from internal to external systems with schema transformation, retry logic, and rate limiting.

## Overview

This service handles real-time synchronization of contact records between internal and external systems. It includes:

- **Mock Stream Generator**: Simulates Kafka-like event streams for testing
- **Schema Transformation**: Maps internal schemas to external formats
- **API Dispatcher**: Handles rate limiting and retries
- **Event Consumer**: Processes incoming events
- **Configuration Management**: Flexible system settings

## Architecture

```
Internal DB → Event Stream → Schema Transformer → API Dispatcher → External Systems
     ↑              ↑              ↑                    ↑
Mock Generator → Event Consumer → Validation → Rate Limiter + Retries
```

## Features

### Mock Stream Generator
- Generates realistic contact events (create, update, delete)
- Configurable event frequency and batch sizes
- Multiple contact types (lead, customer, prospect, vendor, partner, employee)
- Realistic data generation (names, emails, phone numbers)
- Async/sync processing modes

### Event Consumer Service
- Validates incoming events
- Tracks processing statistics
- Handles batch processing
- Provides detailed logging

### Configuration Management
- Environment variable support
- JSON configuration files
- Runtime configuration updates
- Validation and error handling

## Quick Start

### 1. Run the Demo

```bash
python demo_mock_stream.py
```

This will show:
- Single event generation
- Batch event processing
- Continuous streaming demo
- Configuration options

### 2. Start the Application

```bash
python -m app.main
```

### 3. Configure via Environment Variables

```bash
export EVENTS_PER_SECOND=10
export BATCH_SIZE=5
export ENABLE_ASYNC=true
export LOG_LEVEL=INFO
python -m app.main
```

## Configuration

### System Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `events_per_second` | 5 | Number of events generated per second |
| `batch_size` | 10 | Number of events per batch |
| `enable_async` | true | Use async processing |
| `contact_type_distribution` | See below | Distribution of contact types |
| `operation_distribution` | See below | Distribution of operations |

### Default Distributions

**Contact Types:**
- lead: 40%
- customer: 25%
- prospect: 20%
- vendor: 10%
- partner: 3%
- employee: 2%

**Operations:**
- create: 60%
- update: 35%
- delete: 5%

## Sample Event Format

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
    "contact": "lead",
    "created_at": "2024-01-15T10:30:00.123456",
    "updated_at": "2024-01-15T10:30:00.123456"
  }
}
```

## API Usage

### Basic Usage

```python
from app.services.mock_stream_generator import MockStreamGenerator, SystemSettings
from app.services.event_consumer_service import InternalEventConsumer

# Create settings
settings = SystemSettings(events_per_second=10, batch_size=5)

# Create consumer
consumer = InternalEventConsumer()

# Create generator
generator = MockStreamGenerator(settings, consumer)

# Generate single event
event = generator.generate_event()

# Start streaming
await generator.start_streaming()
```

### Configuration Management

```python
from app.utils.config_manager import update_config, get_system_settings

# Update configuration
update_config(events_per_second=20, batch_size=10)

# Get system settings
settings = get_system_settings()
```

## Project Structure

```
app/
├── main.py                 # Application entry point
├── services/
│   ├── mock_stream_generator.py      # Event generator
│   ├── event_consumer_service.py     # Event consumer
│   └── app_startup_service.py        # Startup orchestration
├── utils/
│   └── config_manager.py             # Configuration management
└── models/                           # Data models (future)
```

## Design Decisions

### 1. Streaming vs Polling
- **Streaming**: Async nature, no unnecessary API calls, better performance
- **Polling**: Requires constant polling even when no updates

### 2. Schema Transformation
- **Pydantic Models**: Strong typing, validation, custom business logic
- **Config-driven Mapping**: Simple, maintainable for standard mappings

### 3. API Dispatcher
- **Exponential Backoff**: Start with short delay, double with each retry
- **Jitter**: Add randomness to prevent thundering herd
- **Token Bucket**: Sophisticated rate limiting

### 4. Multiple External Systems
- **Strategy Pattern**: Different strategies for different external systems

## Development

### Running Tests

```bash
# Run demo
python demo_mock_stream.py

# Run main application
python -m app.main
```

### Adding New Features

1. **New Contact Types**: Add to `ContactType` enum in `mock_stream_generator.py`
2. **New Operations**: Add to `OperationType` enum
3. **Custom Data**: Extend `generate_realistic_contact()` method
4. **Configuration**: Add to `AppConfig` in `config_manager.py`

## Future Enhancements

- [ ] Schema transformation engine
- [ ] API dispatcher with retry logic
- [ ] Rate limiting implementation
- [ ] Database integration
- [ ] Web dashboard for monitoring
- [ ] Metrics and alerting
- [ ] Multi-tenant support



---

🔧 Key Components
1. Base Adapter (base_adapter.py)
Abstract base class defining the adapter interface
Common utility methods (name extraction, validation, logging)
Metadata handling and error logging
2. Salesforce Adapter (salesforce_adapter.py)
Handles Salesforce-specific field mappings
Supports: lead, prospect, employee contact types
Transforms to/from Salesforce format with proper field mapping
3. HubSpot Adapter (hubspot_adapter.py)
Handles HubSpot-specific field mappings
Supports: customer, partner, vendor contact types
Transforms to/from HubSpot format with properties structure
4. Schema Transformer (schema_transformer.py)
Configuration-based routing system
Dynamic adapter selection based on contact type
Factory methods for different configurations
Runtime configuration updates
⚙️ Configuration-Based Routing
Default Routing:
lead, prospect, employee → Salesforce
customer, partner, vendor → HubSpot
Custom Configurations:
Salesforce-only: All contact types → Salesforce
HubSpot-only: All contact types → HubSpot
Custom routing: Mix and match as needed
�� Key Features
Modular Design: Each adapter is self-contained and easily extensible
Configuration-Driven: Routing can be changed without code modifications
Dynamic Updates: Runtime configuration changes supported
Factory Patterns: Pre-built configurations for common scenarios
Validation: Each adapter validates its own data format
Logging: Comprehensive logging for debugging and monitoring
Error Handling: Robust error handling with detailed logging
�� Demo Results
The demo shows:
✅ Default configuration routing works correctly
✅ Custom configurations are applied properly
✅ Salesforce-only and HubSpot-only modes work
✅ Dynamic routing updates function correctly
✅ Event processing with different configurations
✅ Adapter validation and capabilities
✅ Full integration with the main application
🎯 Benefits
Maintainability: Each adapter can be modified independently
Extensibility: Easy to add new external systems
Flexibility: Configuration changes without code deployment
Testability: Each component can be tested in isolation
Scalability: New adapters can be added without affecting existing ones
The refactored architecture provides a solid foundation for adding more external systems and implementing more complex routing logic in the future!

-----

You now have full Pydantic model-based schema mapping, validation, and serialization for internal and external schemas (Salesforce, HubSpot).
All adapters and the schema transformer use Pydantic for type safety and validation.
Demos show validation, transformation, event processing, and serialization.
Errors and edge cases are caught early, and the code is much more robust.