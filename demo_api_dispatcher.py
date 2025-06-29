import asyncio
from datetime import datetime
from app.models.internal_schema import InternalContact
from app.services.schema_transformer import SchemaTransformerFactory
from app.services.api_dispatcher_service import APIDispatcherService

async def main():
    print("\n=== API Dispatcher Demo: Rate Limits & Jitter ===\n")
    transformer = SchemaTransformerFactory.create_default()
    dispatcher = APIDispatcherService(transformer, max_retries=4, base_delay=0.3)

    # Create a batch of contacts (mix of types, enough to trigger rate limits)
    contacts = []
    for i in range(12):
        if i % 2 == 0:
            contacts.append(InternalContact(
                id=f"S{i:03}",
                name=f"Salesforce User {i}",
                email=f"sf{i}@example.com",
                phone=f"+1-555-100-{1000+i}",
                contact="lead" if i % 4 == 0 else "prospect",
                created_at=datetime.now(),
                updated_at=datetime.now()
            ))
        else:
            contacts.append(InternalContact(
                id=f"H{i:03}",
                name=f"HubSpot User {i}",
                email=f"hs{i}@example.com",
                phone=f"+1-555-200-{2000+i}",
                contact="customer" if i % 3 == 0 else "vendor",
                created_at=datetime.now(),
                updated_at=datetime.now()
            ))

    # Dispatch all contacts concurrently
    results = await asyncio.gather(*[
        dispatcher.dispatch_contact(contact) for contact in contacts
    ])

    # Print results
    for i, (contact, result) in enumerate(zip(contacts, results)):
        print(f"{i+1:2d}. {contact.name:18} ({contact.contact:9}) → {result.get('system', 'FAIL'):10} | Success: {result.get('success', False)} | {result.get('error', '')}")

if __name__ == "__main__":
    asyncio.run(main()) 