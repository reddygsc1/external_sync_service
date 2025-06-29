#!/usr/bin/env python3
"""
Demo script for the Mock Stream Generator
Shows how to use the service and displays sample events
"""

import asyncio
import json
import logging
from app.services.mock_stream_generator import MockStreamGenerator, SystemSettings
from app.services.event_consumer_service import InternalEventConsumer
from app.utils.config_manager import update_config, get_system_settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def demo_single_event():
    """Demo: Generate and display a single event"""
    print("\n" + "="*60)
    print("DEMO: Single Event Generation")
    print("="*60)
    
    # Create settings for demo
    settings = SystemSettings(
        events_per_second=1,
        batch_size=1,
        enable_async=True
    )
    
    # Create consumer
    consumer = InternalEventConsumer()
    
    # Create generator
    generator = MockStreamGenerator(settings, consumer)
    
    # Generate one event
    event = generator.generate_event()
    
    print("Generated Event:")
    print(json.dumps(event, indent=2))
    
    # Process the event
    await consumer.consume_event(event)
    
    print(f"\nEvent processed successfully!")
    print(f"Contact ID: {event['item']['id']}")
    print(f"Operation: {event['operation']}")
    print(f"Contact Type: {event['item']['contact']}")


async def demo_batch_events():
    """Demo: Generate and process a batch of events"""
    print("\n" + "="*60)
    print("DEMO: Batch Event Generation")
    print("="*60)
    
    # Create settings for demo
    settings = SystemSettings(
        events_per_second=10,
        batch_size=5,
        enable_async=True
    )
    
    # Create consumer
    consumer = InternalEventConsumer()
    
    # Create generator
    generator = MockStreamGenerator(settings, consumer)
    
    # Generate batch
    batch = await generator.generate_batch()
    
    print(f"Generated {len(batch)} events:")
    for i, event in enumerate(batch, 1):
        print(f"\nEvent {i}:")
        print(f"  ID: {event['item']['id']}")
        print(f"  Operation: {event['operation']}")
        print(f"  Contact Type: {event['item']['contact']}")
        print(f"  Name: {event['item']['name']}")
        print(f"  Email: {event['item']['email']}")
    
    # Show consumer stats
    stats = consumer.get_stats()
    print(f"\nConsumer Statistics:")
    print(f"  Total Processed: {stats['total_processed']}")
    print(f"  Event Counters: {stats['event_counters']}")
    print(f"  Contact Type Counters: {stats['contact_type_counters']}")


async def demo_streaming():
    """Demo: Start streaming for a few seconds"""
    print("\n" + "="*60)
    print("DEMO: Continuous Streaming (5 seconds)")
    print("="*60)
    
    # Create settings for demo
    settings = SystemSettings(
        events_per_second=3,
        batch_size=3,
        enable_async=True
    )
    
    # Create consumer
    consumer = InternalEventConsumer()
    
    # Create generator
    generator = MockStreamGenerator(settings, consumer)
    
    print("Starting stream... (will run for 5 seconds)")
    
    # Start streaming in background
    streaming_task = asyncio.create_task(generator.start_streaming())
    
    # Let it run for 5 seconds
    await asyncio.sleep(5)
    
    # Stop streaming
    generator.stop_streaming()
    streaming_task.cancel()
    
    try:
        await streaming_task
    except asyncio.CancelledError:
        pass
    
    # Show final stats
    stats = consumer.get_stats()
    print(f"\nStreaming Complete!")
    print(f"  Total Events Processed: {stats['total_processed']}")
    print(f"  Event Distribution: {stats['event_counters']}")
    print(f"  Contact Type Distribution: {stats['contact_type_counters']}")
    
    # Show generator stats
    gen_stats = generator.get_stats()
    print(f"  Generator Stats: {gen_stats}")


async def demo_configuration():
    """Demo: Show configuration options"""
    print("\n" + "="*60)
    print("DEMO: Configuration Options")
    print("="*60)
    
    # Show default settings
    default_settings = SystemSettings()
    print("Default Settings:")
    print(f"  Events per second: {default_settings.events_per_second}")
    print(f"  Batch size: {default_settings.batch_size}")
    print(f"  Async enabled: {default_settings.enable_async}")
    print(f"  Contact type distribution: {default_settings.contact_type_distribution}")
    print(f"  Operation distribution: {default_settings.operation_distribution}")
    
    # Show custom settings
    custom_settings = SystemSettings(
        events_per_second=20,
        batch_size=5,
        contact_type_distribution={
            "lead": 0.5,
            "customer": 0.3,
            "prospect": 0.2
        },
        operation_distribution={
            "create": 0.7,
            "update": 0.25,
            "delete": 0.05
        }
    )
    
    print(f"\nCustom Settings:")
    print(f"  Events per second: {custom_settings.events_per_second}")
    print(f"  Batch size: {custom_settings.batch_size}")
    print(f"  Contact type distribution: {custom_settings.contact_type_distribution}")
    print(f"  Operation distribution: {custom_settings.operation_distribution}")


async def main():
    """Run all demos"""
    print("Mock Stream Generator Demo")
    print("="*60)
    
    try:
        # Run demos
        await demo_single_event()
        await demo_batch_events()
        await demo_streaming()
        await demo_configuration()
        
        print("\n" + "="*60)
        print("All demos completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 