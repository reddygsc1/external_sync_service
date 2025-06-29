#!/usr/bin/env python3
"""
Demo script for the Schema Transformer
Shows how contacts are transformed to different external systems based on contact type
"""

import asyncio
import json
import logging
from app.services.schema_transformer import SchemaTransformerFactory, ExternalSystem
from app.services.event_consumer_service import InternalEventConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_single_transformations():
    """Demo: Show single contact transformations"""
    print("\n" + "="*60)
    print("DEMO: Single Contact Transformations")
    print("="*60)
    
    # Create transformer factory
    factory = SchemaTransformerFactory()
    
    # Sample contacts for different types
    sample_contacts = [
        {
            "id": "L1234",
            "name": "Alice Johnson",
            "email": "alice.johnson@example.com",
            "phone": "+1-555-123-4567",
            "contact": "lead",
            "created_at": "2024-01-15T10:30:00.123456",
            "updated_at": "2024-01-15T10:30:00.123456"
        },
        {
            "id": "C5678",
            "name": "Bob Smith",
            "email": "bob.smith@acme.com",
            "phone": "+1-555-987-6543",
            "contact": "customer",
            "created_at": "2024-01-15T11:00:00.123456",
            "updated_at": "2024-01-15T11:00:00.123456"
        },
        {
            "id": "P9012",
            "name": "Carol Davis",
            "email": "carol.davis@startup.io",
            "phone": "+1-555-456-7890",
            "contact": "prospect",
            "created_at": "2024-01-15T11:30:00.123456",
            "updated_at": "2024-01-15T11:30:00.123456"
        },
        {
            "id": "V3456",
            "name": "David Wilson",
            "email": "david.wilson@vendor.net",
            "phone": "+1-555-789-0123",
            "contact": "vendor",
            "created_at": "2024-01-15T12:00:00.123456",
            "updated_at": "2024-01-15T12:00:00.123456"
        },
        {
            "id": "P7890",
            "name": "Eve Brown",
            "email": "eve.brown@partner.co",
            "phone": "+1-555-321-6540",
            "contact": "partner",
            "created_at": "2024-01-15T12:30:00.123456",
            "updated_at": "2024-01-15T12:30:00.123456"
        },
        {
            "id": "E2345",
            "name": "Frank Miller",
            "email": "frank.miller@company.com",
            "phone": "+1-555-654-3210",
            "contact": "employee",
            "created_at": "2024-01-15T13:00:00.123456",
            "updated_at": "2024-01-15T13:00:00.123456"
        }
    ]
    
    for contact in sample_contacts:
        print(f"\n{'='*40}")
        print(f"Contact: {contact['name']} ({contact['contact']})")
        print(f"{'='*40}")
        
        # Show routing decision
        external_system = factory.get_external_system(contact['contact'])
        print(f"Routing to: {external_system.value.upper()}")
        
        # Transform the contact
        try:
            external_data = factory.transform_contact(contact)
            
            print(f"Transformation successful!")
            print(f"External System: {external_data['_metadata']['external_system']}")
            print(f"Contact Type: {external_data['_metadata']['contact_type']}")
            
            # Show transformed data (formatted)
            if external_system == ExternalSystem.SALESFORCE:
                print("\nSalesforce Format:")
                print(f"  ID: {external_data.get('Id', 'N/A')}")
                print(f"  First Name: {external_data.get('FirstName', 'N/A')}")
                print(f"  Last Name: {external_data.get('LastName', 'N/A')}")
                print(f"  Email: {external_data.get('Email', 'N/A')}")
                print(f"  Phone: {external_data.get('Phone', 'N/A')}")
                print(f"  Type: {external_data.get('Type', 'N/A')}")
                print(f"  Status: {external_data.get('Status', 'N/A')}")
                print(f"  Lead Source: {external_data.get('LeadSource', 'N/A')}")
                print(f"  Company: {external_data.get('Company', 'N/A')}")
                
                if contact['contact'] in ['customer', 'partner', 'vendor']:
                    print(f"  Account ID: {external_data.get('AccountId', 'N/A')}")
                    print(f"  Title: {external_data.get('Title', 'N/A')}")
                    print(f"  Department: {external_data.get('Department', 'N/A')}")
            
            elif external_system == ExternalSystem.HUBSPOT:
                print("\nHubSpot Format:")
                properties = external_data.get('properties', {})
                print(f"  ID: {external_data.get('id', 'N/A')}")
                print(f"  First Name: {properties.get('firstname', 'N/A')}")
                print(f"  Last Name: {properties.get('lastname', 'N/A')}")
                print(f"  Email: {properties.get('email', 'N/A')}")
                print(f"  Phone: {properties.get('phone', 'N/A')}")
                print(f"  Lifecycle Stage: {properties.get('lifecyclestage', 'N/A')}")
                print(f"  Lead Status: {properties.get('hs_lead_status', 'N/A')}")
                print(f"  Company: {properties.get('company', 'N/A')}")
                print(f"  Job Title: {properties.get('jobtitle', 'N/A')}")
                print(f"  Department: {properties.get('department', 'N/A')}")
                
                if contact['contact'] in ['customer', 'partner', 'vendor']:
                    print(f"  Customer Type: {properties.get('customer_type', 'N/A')}")
                    print(f"  Account ID: {properties.get('account_id', 'N/A')}")
                    print(f"  Relationship Type: {properties.get('relationship_type', 'N/A')}")
            
        except Exception as e:
            print(f"Transformation failed: {e}")


async def demo_batch_processing():
    """Demo: Process a batch of events with schema transformation"""
    print("\n" + "="*60)
    print("DEMO: Batch Processing with Schema Transformation")
    print("="*60)
    
    # Create consumer
    consumer = InternalEventConsumer()
    
    # Create sample events
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
            "operation": "update",
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
        },
        {
            "record": "contacts",
            "operation": "create",
            "timestamp": "2024-01-15T12:00:00.123456",
            "item": {
                "id": "V3456",
                "name": "David Wilson",
                "email": "david.wilson@vendor.net",
                "phone": "+1-555-789-0123",
                "contact": "vendor",
                "created_at": "2024-01-15T12:00:00.123456",
                "updated_at": "2024-01-15T12:00:00.123456"
            }
        },
        {
            "record": "contacts",
            "operation": "create",
            "timestamp": "2024-01-15T12:30:00.123456",
            "item": {
                "id": "E2345",
                "name": "Frank Miller",
                "email": "frank.miller@company.com",
                "phone": "+1-555-654-3210",
                "contact": "employee",
                "created_at": "2024-01-15T13:00:00.123456",
                "updated_at": "2024-01-15T13:00:00.123456"
            }
        }
    ]
    
    # Process the batch
    success = await consumer.consume_batch(sample_events)
    
    print(f"Batch processing completed: {'Success' if success else 'Failed'}")
    
    # Show statistics
    stats = consumer.get_stats()
    print(f"\nProcessing Statistics:")
    print(f"  Total Processed: {stats['total_processed']}")
    print(f"  Event Counters: {stats['event_counters']}")
    print(f"  Contact Type Counters: {stats['contact_type_counters']}")
    print(f"  External System Counters: {stats['external_system_counters']}")
    print(f"  Transformation Errors: {stats['transformation_errors']}")
    
    # Show transformation summary
    summary = consumer.get_transformation_summary()
    print(f"\nTransformation Summary:")
    print(f"  Total Events: {summary['total_events']}")
    print(f"  Successful Transformations: {summary['successful_transformations']}")
    print(f"  Failed Transformations: {summary['failed_transformations']}")
    print(f"  Success Rate: {summary['success_rate']:.1f}%")
    print(f"  External System Distribution: {summary['external_system_distribution']}")
    
    # Show recent events with transformation results
    print(f"\nRecent Events with Transformations:")
    recent_events = consumer.get_recent_events(3)
    for i, event in enumerate(recent_events, 1):
        print(f"\nEvent {i}:")
        print(f"  Contact: {event['event']['item']['name']} ({event['event']['item']['contact']})")
        print(f"  Operation: {event['event']['operation']}")
        print(f"  Transformation: {'Success' if event['transformation_success'] else 'Failed'}")
        if event['external_data']:
            external_system = event['external_data']['_metadata']['external_system']
            print(f"  External System: {external_system}")


def demo_routing_logic():
    """Demo: Show routing logic for different contact types"""
    print("\n" + "="*60)
    print("DEMO: Contact Type Routing Logic")
    print("="*60)
    
    factory = SchemaTransformerFactory()
    
    contact_types = ["lead", "customer", "prospect", "vendor", "partner", "employee"]
    
    print("Contact Type Routing Rules:")
    print("  HubSpot: customer, partner, vendor")
    print("  Salesforce: lead, prospect, employee")
    print()
    
    for contact_type in contact_types:
        external_system = factory.get_external_system(contact_type)
        transformer = factory.get_transformer(contact_type)
        
        print(f"{contact_type:10} → {external_system.value:10} ({transformer.__class__.__name__})")


async def main():
    """Run all demos"""
    print("Schema Transformer Demo")
    print("="*60)
    
    try:
        # Run demos
        demo_single_transformations()
        await demo_batch_processing()
        demo_routing_logic()
        
        print("\n" + "="*60)
        print("All demos completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 