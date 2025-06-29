#!/usr/bin/env python3
"""
Demo script for the Refactored Schema Transformer
Shows the new adapter-based architecture with configuration-based routing
"""

import asyncio
import json
import logging
from app.services.schema_transformer import SchemaTransformer, SchemaTransformerFactory, ExternalSystem
from app.services.event_consumer_service import InternalEventConsumer
from app.services.adapters import SalesforceAdapter, HubSpotAdapter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_default_configuration():
    """Demo: Show default configuration routing"""
    print("\n" + "="*60)
    print("DEMO: Default Configuration")
    print("="*60)
    
    # Create transformer with default config
    transformer = SchemaTransformerFactory.create_default()
    
    # Show routing configuration
    config = transformer.get_routing_configuration()
    print("Default Routing Configuration:")
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    print(f"  Available Adapters: {config['adapters']}")
    print(f"  Supported Contact Types: {config['supported_contact_types']}")
    
    # Test routing for each contact type
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print(f"\nRouting Results:")
    for contact_type in contact_types:
        adapter = transformer.get_adapter(contact_type)
        external_system = transformer.get_external_system(contact_type)
        print(f"  {contact_type:10} → {external_system.value:10} ({adapter.__class__.__name__})")


def demo_custom_configuration():
    """Demo: Show custom configuration routing"""
    print("\n" + "="*60)
    print("DEMO: Custom Configuration")
    print("="*60)
    
    # Create custom configuration
    custom_config = {
        "contact_type_routing": {
            "lead": "hubspot",      # Route leads to HubSpot instead
            "customer": "hubspot", 
            "prospect": "salesforce",
            "vendor": "hubspot",
            "partner": "hubspot",
            "employee": "salesforce"
        },
        "salesforce": {
            "api_version": "v58.0",
            "org_id": "00D123456789"
        },
        "hubspot": {
            "portal_id": "123456",
            "api_key": "demo_key"
        }
    }
    
    # Create transformer with custom config
    transformer = SchemaTransformerFactory.create_with_config(custom_config)
    
    # Show routing configuration
    config = transformer.get_routing_configuration()
    print("Custom Routing Configuration:")
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    
    # Test routing for each contact type
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print(f"\nRouting Results:")
    for contact_type in contact_types:
        adapter = transformer.get_adapter(contact_type)
        external_system = transformer.get_external_system(contact_type)
        print(f"  {contact_type:10} → {external_system.value:10} ({adapter.__class__.__name__})")


def demo_salesforce_only():
    """Demo: Show Salesforce-only configuration"""
    print("\n" + "="*60)
    print("DEMO: Salesforce-Only Configuration")
    print("="*60)
    
    # Create Salesforce-only transformer
    transformer = SchemaTransformerFactory.create_salesforce_only()
    
    # Show routing configuration
    config = transformer.get_routing_configuration()
    print("Salesforce-Only Routing Configuration:")
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    
    # Test routing for each contact type
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print(f"\nRouting Results:")
    for contact_type in contact_types:
        adapter = transformer.get_adapter(contact_type)
        external_system = transformer.get_external_system(contact_type)
        print(f"  {contact_type:10} → {external_system.value:10} ({adapter.__class__.__name__})")


def demo_hubspot_only():
    """Demo: Show HubSpot-only configuration"""
    print("\n" + "="*60)
    print("DEMO: HubSpot-Only Configuration")
    print("="*60)
    
    # Create HubSpot-only transformer
    transformer = SchemaTransformerFactory.create_hubspot_only()
    
    # Show routing configuration
    config = transformer.get_routing_configuration()
    print("HubSpot-Only Routing Configuration:")
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    
    # Test routing for each contact type
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print(f"\nRouting Results:")
    for contact_type in contact_types:
        adapter = transformer.get_adapter(contact_type)
        external_system = transformer.get_external_system(contact_type)
        print(f"  {contact_type:10} → {external_system.value:10} ({adapter.__class__.__name__})")


def demo_dynamic_routing_update():
    """Demo: Show dynamic routing configuration updates"""
    print("\n" + "="*60)
    print("DEMO: Dynamic Routing Configuration Updates")
    print("="*60)
    
    # Create transformer with default config
    transformer = SchemaTransformerFactory.create_default()
    
    print("Initial Configuration:")
    config = transformer.get_routing_configuration()
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    
    # Update routing configuration
    new_routing = {
        "lead": "hubspot",      # Change lead routing to HubSpot
        "prospect": "hubspot"   # Change prospect routing to HubSpot
    }
    
    transformer.update_routing_configuration(new_routing)
    
    print(f"\nAfter Update:")
    config = transformer.get_routing_configuration()
    print(f"  Contact Type Routing: {config['contact_type_routing']}")
    
    # Test routing for each contact type
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print(f"\nUpdated Routing Results:")
    for contact_type in contact_types:
        adapter = transformer.get_adapter(contact_type)
        external_system = transformer.get_external_system(contact_type)
        print(f"  {contact_type:10} → {external_system.value:10} ({adapter.__class__.__name__})")


async def demo_event_processing_with_config():
    """Demo: Process events with different configurations"""
    print("\n" + "="*60)
    print("DEMO: Event Processing with Different Configurations")
    print("="*60)
    
    # Sample events
    sample_events = [
        {
            "record": "contacts",
            "operation": "create",
            "timestamp": "2024-01-15T10:30:00.123456",
            "item": {
                "id": "L1234",
                "name": "Alice Johnson",
                "email": "alice.johnson@example.com",
                "phone": "+1-555-123-4567",
                "contact": "lead",
                "created_at": "2024-01-15T10:30:00.123456",
                "updated_at": "2024-01-15T10:30:00.123456"
            }
        },
        {
            "record": "contacts",
            "operation": "create",
            "timestamp": "2024-01-15T11:00:00.123456",
            "item": {
                "id": "C5678",
                "name": "Bob Smith",
                "email": "bob.smith@acme.com",
                "phone": "+1-555-987-6543",
                "contact": "customer",
                "created_at": "2024-01-15T11:00:00.123456",
                "updated_at": "2024-01-15T11:00:00.123456"
            }
        },
        {
            "record": "contacts",
            "operation": "create",
            "timestamp": "2024-01-15T11:30:00.123456",
            "item": {
                "id": "P9012",
                "name": "Carol Davis",
                "email": "carol.davis@startup.io",
                "phone": "+1-555-456-7890",
                "contact": "prospect",
                "created_at": "2024-01-15T11:30:00.123456",
                "updated_at": "2024-01-15T11:30:00.123456"
            }
        }
    ]
    
    # Test with default configuration
    print("1. Processing with Default Configuration:")
    consumer_default = InternalEventConsumer()
    await consumer_default.consume_batch(sample_events)
    
    stats_default = consumer_default.get_stats()
    print(f"  External System Counters: {stats_default['external_system_counters']}")
    
    # Test with custom configuration (all to HubSpot)
    print(f"\n2. Processing with Custom Configuration (All to HubSpot):")
    custom_config = {
        "contact_type_routing": {
            "lead": "hubspot",
            "customer": "hubspot",
            "prospect": "hubspot",
            "vendor": "hubspot",
            "partner": "hubspot",
            "employee": "hubspot"
        }
    }
    
    consumer_custom = InternalEventConsumer(custom_config)
    await consumer_custom.consume_batch(sample_events)
    
    stats_custom = consumer_custom.get_stats()
    print(f"  External System Counters: {stats_custom['external_system_counters']}")
    
    # Show routing configurations
    print(f"\n3. Routing Configurations:")
    print(f"  Default: {consumer_default.get_routing_configuration()['contact_type_routing']}")
    print(f"  Custom:  {consumer_custom.get_routing_configuration()['contact_type_routing']}")


def demo_adapter_capabilities():
    """Demo: Show adapter capabilities and supported contact types"""
    print("\n" + "="*60)
    print("DEMO: Adapter Capabilities")
    print("="*60)
    
    # Create adapters
    salesforce_adapter = SalesforceAdapter()
    hubspot_adapter = HubSpotAdapter()
    
    print("Salesforce Adapter:")
    print(f"  Supported Contact Types: {salesforce_adapter.get_supported_contact_types()}")
    print(f"  Adapter Name: {salesforce_adapter.name}")
    
    print(f"\nHubSpot Adapter:")
    print(f"  Supported Contact Types: {hubspot_adapter.get_supported_contact_types()}")
    print(f"  Adapter Name: {hubspot_adapter.name}")
    
    # Test validation
    print(f"\nValidation Tests:")
    
    # Valid Salesforce data
    valid_salesforce_data = {
        "Id": "12345",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com"
    }
    
    # Valid HubSpot data
    valid_hubspot_data = {
        "id": "12345",
        "properties": {
            "firstname": "Jane",
            "lastname": "Smith",
            "email": "jane.smith@example.com"
        }
    }
    
    print(f"  Salesforce Validation: {salesforce_adapter.validate_external_data(valid_salesforce_data)}")
    print(f"  HubSpot Validation: {hubspot_adapter.validate_external_data(valid_hubspot_data)}")


async def main():
    """Run all demos"""
    print("Refactored Schema Transformer Demo")
    print("="*60)
    
    try:
        # Run demos
        demo_default_configuration()
        demo_custom_configuration()
        demo_salesforce_only()
        demo_hubspot_only()
        demo_dynamic_routing_update()
        await demo_event_processing_with_config()
        demo_adapter_capabilities()
        
        print("\n" + "="*60)
        print("All demos completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 