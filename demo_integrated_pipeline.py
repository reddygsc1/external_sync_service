#!/usr/bin/env python3
"""
Demo: Integrated Pipeline Service
Connects stream generator to schema transformer and external APIs
"""

import asyncio
import signal
import sys
import logging
from datetime import datetime

# Add app to path
sys.path.append('.')

from app.services.integrated_pipeline_service import (
    IntegratedPipelineService, 
    PipelineFactory,
    SystemSettings
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineDemo:
    """Demo class for showcasing the integrated pipeline"""
    
    def __init__(self):
        self.pipeline = None
        self.running = False
        
    async def run_default_pipeline(self):
        """Run the default pipeline configuration"""
        logger.info("=== Running Default Pipeline ===")
        
        # Create default pipeline
        self.pipeline = PipelineFactory.create_default_pipeline()
        
        # Start pipeline
        await self._run_pipeline("Default Pipeline")
    
    async def run_test_pipeline(self):
        """Run test pipeline with mock mode"""
        logger.info("=== Running Test Pipeline (Mock Mode) ===")
        
        # Create test pipeline
        self.pipeline = PipelineFactory.create_test_pipeline()
        
        # Start pipeline
        await self._run_pipeline("Test Pipeline (Mock Mode)")
    
    async def run_high_volume_pipeline(self):
        """Run high volume pipeline"""
        logger.info("=== Running High Volume Pipeline ===")
        
        # Create high volume pipeline
        self.pipeline = PipelineFactory.create_high_volume_pipeline()
        
        # Start pipeline
        await self._run_pipeline("High Volume Pipeline")
    
    async def run_salesforce_only_pipeline(self):
        """Run pipeline that routes everything to Salesforce"""
        logger.info("=== Running Salesforce-Only Pipeline ===")
        
        # Create Salesforce-only pipeline
        self.pipeline = PipelineFactory.create_salesforce_only_pipeline()
        
        # Start pipeline
        await self._run_pipeline("Salesforce-Only Pipeline")
    
    async def run_hubspot_only_pipeline(self):
        """Run pipeline that routes everything to HubSpot"""
        logger.info("=== Running HubSpot-Only Pipeline ===")
        
        # Create HubSpot-only pipeline
        self.pipeline = PipelineFactory.create_hubspot_only_pipeline()
        
        # Start pipeline
        await self._run_pipeline("HubSpot-Only Pipeline")
    
    async def run_custom_pipeline(self):
        """Run custom pipeline with specific configuration"""
        logger.info("=== Running Custom Pipeline ===")
        
        # Custom stream settings
        custom_settings = SystemSettings(
            events_per_second=3,
            batch_size=8,
            enable_async=True,
            contact_type_distribution={
                "lead": 0.3,
                "customer": 0.4,
                "prospect": 0.2,
                "vendor": 0.05,
                "partner": 0.03,
                "employee": 0.02
            }
        )
        
        # Custom transformer config
        custom_config = {
            "contact_type_routing": {
                "lead": "salesforce",
                "customer": "hubspot",
                "prospect": "salesforce",
                "vendor": "hubspot",
                "partner": "hubspot",
                "employee": "salesforce"
            }
        }
        
        # Create custom pipeline
        self.pipeline = IntegratedPipelineService(
            stream_settings=custom_settings,
            transformer_config=custom_config,
            enable_api_calls=False,  # Use mock mode for demo
            enable_mock_mode=True
        )
        
        # Start pipeline
        await self._run_pipeline("Custom Pipeline")
    
    async def _run_pipeline(self, pipeline_name: str):
        """Run a pipeline with monitoring"""
        try:
            self.running = True
            
            # Start pipeline in background
            pipeline_task = asyncio.create_task(self.pipeline.start_pipeline())
            
            # Monitor pipeline stats
            monitor_task = asyncio.create_task(self._monitor_pipeline(pipeline_name))
            
            # Wait for both tasks
            await asyncio.gather(pipeline_task, monitor_task)
            
        except KeyboardInterrupt:
            logger.info(f"Stopping {pipeline_name}...")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            self.running = False
            if self.pipeline:
                await self.pipeline.stop_pipeline()
    
    async def _monitor_pipeline(self, pipeline_name: str):
        """Monitor pipeline statistics"""
        logger.info(f"Starting monitoring for {pipeline_name}")
        
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                if self.pipeline:
                    stats = self.pipeline.get_pipeline_stats()
                    
                    logger.info(f"""
=== {pipeline_name} Stats ===
Status: {'Running' if stats['is_running'] else 'Stopped'}
Runtime: {stats['runtime']}
Events Generated: {stats['events_generated']}
Events Processed: {stats['events_processed']}
Events Transformed: {stats['events_transformed']}
Events Dispatched: {stats['events_dispatched']}
Events Failed: {stats['events_failed']}
Queue Size: {stats['queue_size']}
                    """)
                    
                    # Show routing configuration
                    routing = stats.get('routing_config', {})
                    if routing:
                        logger.info(f"Routing: {routing.get('contact_type_routing', {})}")
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    def show_pipeline_options(self):
        """Show available pipeline options"""
        logger.info("""
=== Available Pipeline Configurations ===

1. Default Pipeline
   - Balanced settings (5 events/sec, batch size 10)
   - Mixed routing (customers/partners/vendors → HubSpot, others → Salesforce)

2. Test Pipeline (Mock Mode)
   - Low volume (2 events/sec, batch size 5)
   - Mock API calls (no actual external calls)
   - Good for testing

3. High Volume Pipeline
   - High throughput (20 events/sec, batch size 50)
   - Mixed routing
   - For performance testing

4. Salesforce-Only Pipeline
   - Routes all contacts to Salesforce
   - Useful for Salesforce-only deployments

5. HubSpot-Only Pipeline
   - Routes all contacts to HubSpot
   - Useful for HubSpot-only deployments

6. Custom Pipeline
   - Custom stream settings and routing
   - Mock mode enabled
   - Demonstrates configuration flexibility
        """)
    
    async def interactive_demo(self):
        """Run interactive demo with user choice"""
        self.show_pipeline_options()
        
        while True:
            try:
                choice = input("\nSelect pipeline (1-6) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    break
                
                pipeline_map = {
                    '1': self.run_default_pipeline,
                    '2': self.run_test_pipeline,
                    '3': self.run_high_volume_pipeline,
                    '4': self.run_salesforce_only_pipeline,
                    '5': self.run_hubspot_only_pipeline,
                    '6': self.run_custom_pipeline
                }
                
                if choice in pipeline_map:
                    await pipeline_map[choice]()
                    
                    # Ask if user wants to continue
                    continue_choice = input("\nRun another pipeline? (y/n): ").strip().lower()
                    if continue_choice != 'y':
                        break
                else:
                    logger.info("Invalid choice. Please select 1-6 or 'q'.")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Demo error: {e}")


async def main():
    """Main demo function"""
    logger.info("=== Integrated Pipeline Demo ===")
    logger.info("This demo showcases the complete pipeline from stream generation to external API calls")
    
    demo = PipelineDemo()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        pipeline_type = sys.argv[1].lower()
        
        if pipeline_type == "default":
            await demo.run_default_pipeline()
        elif pipeline_type == "test":
            await demo.run_test_pipeline()
        elif pipeline_type == "high-volume":
            await demo.run_high_volume_pipeline()
        elif pipeline_type == "salesforce":
            await demo.run_salesforce_only_pipeline()
        elif pipeline_type == "hubspot":
            await demo.run_hubspot_only_pipeline()
        elif pipeline_type == "custom":
            await demo.run_custom_pipeline()
        else:
            logger.error(f"Unknown pipeline type: {pipeline_type}")
            logger.info("Available types: default, test, high-volume, salesforce, hubspot, custom")
    else:
        # Run interactive demo
        await demo.interactive_demo()
    
    logger.info("Demo completed!")


if __name__ == "__main__":
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal, stopping demo...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the demo
    asyncio.run(main()) 