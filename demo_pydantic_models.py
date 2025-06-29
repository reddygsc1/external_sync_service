#!/usr/bin/env python3
"""
Demo script for Pydantic Model-Based Schema Transformer
Shows type safety, validation, and improved data handling
"""

import asyncio
import json
import logging
from datetime import datetime
from app.models.internal_schema import InternalContact, InternalContactEvent
from app.models.salesforce_schema import SalesforceContact, SalesforceContactResponse
from app.models.hubspot_schema import HubSpotContact, HubSpotContactProperties
from app.services.schema_transformer import SchemaTransformer, SchemaTransformerFactory
from app.services.event_consumer_service import InternalEventConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_pydantic_model_validation():
    """Demo: Show Pydantic model validation capabilities"""
    print("\n" + "="*60)
    print("DEMO: Pydantic Model Validation")
    print("="*60)
    
    # Valid internal contact
    print("1. Valid Internal Contact:")
    try:
        valid_contact = InternalContact(
            id="C12345",
            name="John Doe",
            email="john.doe@example.com",
            phone="+1-555-123-4567",
            contact="customer",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            company="Acme Corp",
            title="Senior Manager",
            department="Sales"
        )
        print(f"  ✅ Valid contact created: {valid_contact.name} ({valid_contact.contact})")
        print(f"  📧 Email: {valid_contact.email}")
        print(f"  🏢 Company: {valid_contact.company}")
        
    except Exception as e:
        print(f"  ❌ Validation error: {e}")
    
    # Invalid internal contact (missing required fields)
    print(f"\n2. Invalid Internal Contact (Missing Required Fields):")
    try:
        invalid_contact = InternalContact(
            id="C12346",
            name="",  # Empty name
            email="invalid-email",  # Invalid email
            phone="123",  # Too short phone
            contact="invalid_type",  # Invalid contact type
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        print(f"  ✅ Contact created: {invalid_contact.name}")
    except Exception as e:
        print(f"  ❌ Validation error: {e}")
    
    # Valid Salesforce contact
    print(f"\n3. Valid Salesforce Contact:")
    try:
        valid_salesforce = SalesforceContact(
            Id="0031234567890ABC",
            FirstName="Jane",
            LastName="Smith",
            Email="jane.smith@example.com",
            Phone="+1-555-987-6543",
            LeadSource="Website",
            Status="Active",
            Type="Customer",
            CreatedDate=datetime.now(),
            LastModifiedDate=datetime.now(),
            Company="Tech Corp",
            Title="Manager"
        )
        print(f"  ✅ Valid Salesforce contact: {valid_salesforce.FirstName} {valid_salesforce.LastName}")
        print(f"  📧 Email: {valid_salesforce.Email}")
        print(f"  🏢 Company: {valid_salesforce.Company}")
        
    except Exception as e:
        print(f"  ❌ Validation error: {e}")
    
    # Valid HubSpot contact
    print(f"\n4. Valid HubSpot Contact:")
    try:
        valid_hubspot_props = HubSpotContactProperties(
            firstname="Alice",
            lastname="Johnson",
            email="alice.johnson@example.com",
            phone="+1-555-456-7890",
            lifecyclestage="customer",
            hs_lead_status="CUSTOMER",
            createdate=datetime.now(),
            lastmodifieddate=datetime.now(),
            company="Startup Inc",
            jobtitle="CEO"
        )
        
        valid_hubspot = HubSpotContact(
            id="12345",
            properties=valid_hubspot_props
        )
        print(f"  ✅ Valid HubSpot contact: {valid_hubspot.properties.firstname} {valid_hubspot.properties.lastname}")
        print(f"  📧 Email: {valid_hubspot.properties.email}")
        print(f"  🏢 Company: {valid_hubspot.properties.company}")
        
    except Exception as e:
        print(f"  ❌ Validation error: {e}")


def demo_schema_transformation_with_pydantic():
    """Demo: Show schema transformation using Pydantic models"""
    print("\n" + "="*60)
    print("DEMO: Schema Transformation with Pydantic Models")
    print("="*60)
    
    # Create transformer
    transformer = SchemaTransformerFactory.create_default()
    
    # Create valid internal contacts using Pydantic models
    contacts = [
        InternalContact(
            id="L1234",
            name="Lead Contact",
            email="lead@example.com",
            phone="+1-555-111-1111",
            contact="lead",
            created_at=datetime.now(),
            updated_at=datetime.now()
        ),
        InternalContact(
            id="C5678",
            name="Customer Contact",
            email="customer@acme.com",
            phone="+1-555-222-2222",
            contact="customer",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            company="Acme Corp",
            title="Manager"
        ),
        InternalContact(
            id="P9012",
            name="Prospect Contact",
            email="prospect@startup.io",
            phone="+1-555-333-3333",
            contact="prospect",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            company="Startup Inc"
        )
    ]
    
    print("Transforming contacts to external formats:")
    
    for contact in contacts:
        try:
            # Transform to external format
            external_data = transformer.transform_contact(contact)
            
            # Get adapter info
            adapter = transformer.get_adapter(contact.contact)
            external_system = transformer.get_external_system(contact.contact)
            
            print(f"\n  📋 Contact: {contact.name} ({contact.contact})")
            print(f"  🔄 Adapter: {adapter.__class__.__name__}")
            print(f"  🌐 External System: {external_system.value}")
            print(f"  📧 Email: {contact.email}")
            
            # Show some external data fields
            if external_system.value == "salesforce":
                print(f"  🏷️  Salesforce Type: {external_data.get('Type', 'N/A')}")
                print(f"  📊 Lead Source: {external_data.get('LeadSource', 'N/A')}")
            elif external_system.value == "hubspot":
                props = external_data.get('properties', {})
                print(f"  🏷️  HubSpot Lifecycle: {props.get('lifecyclestage', 'N/A')}")
                print(f"  📊 Lead Status: {props.get('hs_lead_status', 'N/A')}")
            
            print(f"  ✅ Transformation successful")
            
        except Exception as e:
            print(f"  ❌ Transformation error: {e}")


async def demo_event_processing_with_pydantic():
    """Demo: Process events using Pydantic models"""
    print("\n" + "="*60)
    print("DEMO: Event Processing with Pydantic Models")
    print("="*60)
    
    # Create event consumer
    consumer = InternalEventConsumer()
    
    # Create events using Pydantic models
    events = [
        InternalContactEvent(
            record="contacts",
            operation="create",
            timestamp=datetime.now(),
            item=InternalContact(
                id="E1234",
                name="Event Contact 1",
                email="event1@example.com",
                phone="+1-555-444-4444",
                contact="lead",
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        ),
        InternalContactEvent(
            record="contacts",
            operation="create",
            timestamp=datetime.now(),
            item=InternalContact(
                id="E5678",
                name="Event Contact 2",
                email="event2@acme.com",
                phone="+1-555-555-5555",
                contact="customer",
                created_at=datetime.now(),
                updated_at=datetime.now(),
                company="Acme Corp"
            )
        )
    ]
    
    print("Processing events with Pydantic models:")
    
    for event in events:
        try:
            # Convert event to dict for processing
            event_dict = event.dict()
            
            # Process event
            success = await consumer.consume_event(event_dict)
            
            if success:
                print(f"  ✅ Processed {event.operation} event for {event.item.name} ({event.item.contact})")
            else:
                print(f"  ❌ Failed to process event for {event.item.name}")
                
        except Exception as e:
            print(f"  ❌ Event processing error: {e}")
    
    # Show statistics
    stats = consumer.get_stats()
    print(f"\n📊 Processing Statistics:")
    print(f"  Total Processed: {stats['total_processed']}")
    print(f"  External System Counters: {stats['external_system_counters']}")
    print(f"  Contact Type Counters: {stats['contact_type_counters']}")


def demo_validation_features():
    """Demo: Show advanced validation features"""
    print("\n" + "="*60)
    print("DEMO: Advanced Validation Features")
    print("="*60)
    
    transformer = SchemaTransformer()
    
    # Test internal data validation
    print("1. Internal Data Validation:")
    
    valid_data = {
        "id": "V1234",
        "name": "Valid Contact",
        "email": "valid@example.com",
        "phone": "+1-555-666-6666",
        "contact": "vendor",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
    
    is_valid = transformer.validate_internal_data(valid_data)
    print(f"  Valid data: {'✅' if is_valid else '❌'}")
    
    invalid_data = {
        "id": "I1234",
        "name": "",  # Empty name
        "email": "invalid-email",
        "phone": "123",  # Too short
        "contact": "invalid_type",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
    
    is_valid = transformer.validate_internal_data(invalid_data)
    print(f"  Invalid data: {'✅' if is_valid else '❌'}")
    
    # Test external data validation
    print(f"\n2. External Data Validation:")
    
    valid_salesforce_data = {
        "Id": "0031234567890ABC",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com",
        "Phone": "+1-555-777-7777",
        "LeadSource": "Website",
        "Status": "Active",
        "Type": "Customer",
        "CreatedDate": datetime.now(),
        "LastModifiedDate": datetime.now()
    }
    
    is_valid = transformer.validate_external_data(valid_salesforce_data, "salesforce")
    print(f"  Valid Salesforce data: {'✅' if is_valid else '❌'}")
    
    valid_hubspot_data = {
        "id": "12345",
        "properties": {
            "firstname": "Jane",
            "lastname": "Smith",
            "email": "jane.smith@example.com",
            "phone": "+1-555-888-8888",
            "lifecyclestage": "customer",
            "hs_lead_status": "CUSTOMER",
            "createdate": datetime.now(),
            "lastmodifieddate": datetime.now()
        }
    }
    
    is_valid = transformer.validate_external_data(valid_hubspot_data, "hubspot")
    print(f"  Valid HubSpot data: {'✅' if is_valid else '❌'}")


def demo_serialization_features():
    """Demo: Show Pydantic serialization features"""
    print("\n" + "="*60)
    print("DEMO: Pydantic Serialization Features")
    print("="*60)
    
    # Create a contact
    contact = InternalContact(
        id="S1234",
        name="Serialization Demo",
        email="demo@example.com",
        phone="+1-555-999-9999",
        contact="customer",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        company="Demo Corp",
        title="Developer"
    )
    
    print("1. JSON Serialization:")
    json_data = contact.json()
    print(f"  JSON: {json_data[:100]}...")
    
    print(f"\n2. Dictionary Conversion:")
    dict_data = contact.dict()
    print(f"  Keys: {list(dict_data.keys())}")
    print(f"  Company: {dict_data.get('company')}")
    print(f"  Title: {dict_data.get('title')}")
    
    print(f"\n3. Dictionary with Exclusions:")
    dict_excluded = contact.dict(exclude_none=True)
    print(f"  Keys (exclude_none): {list(dict_excluded.keys())}")
    
    print(f"\n4. Model Schema:")
    schema = contact.schema()
    print(f"  Required fields: {schema.get('required', [])}")
    print(f"  Properties: {list(schema.get('properties', {}).keys())}")


async def main():
    """Run all Pydantic model demos"""
    print("Pydantic Model-Based Schema Transformer Demo")
    print("="*60)
    
    try:
        # Run demos
        demo_pydantic_model_validation()
        demo_schema_transformation_with_pydantic()
        await demo_event_processing_with_pydantic()
        demo_validation_features()
        demo_serialization_features()
        
        print("\n" + "="*60)
        print("All Pydantic model demos completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 