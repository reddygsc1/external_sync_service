import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

from .mock_stream_generator import MockStreamGenerator, SystemSettings
from .schema_transformer import SchemaTransformer
from .api_dispatcher_service import APIDispatcherService
from .event_consumer_service import InternalEventConsumer
from app.models.internal_schema import InternalContact, InternalContactEvent

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """Statistics for the integrated pipeline"""
    events_generated: int = 0
    events_processed: int = 0
    events_transformed: int = 0
    events_dispatched: int = 0
    events_failed: int = 0
    start_time: Optional[datetime] = None
    last_event_time: Optional[datetime] = None
    
    def reset(self):
        self.events_generated = 0
        self.events_processed = 0
        self.events_transformed = 0
        self.events_dispatched = 0
        self.events_failed = 0
        self.start_time = None
        self.last_event_time = None


class IntegratedPipelineService:
    """Integrated pipeline service that connects stream generator to external APIs"""
    
    def __init__(
        self,
        stream_settings: Optional[SystemSettings] = None,
        transformer_config: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        enable_api_calls: bool = True,
        enable_mock_mode: bool = False
    ):
        # Initialize components
        self.stream_settings = stream_settings or SystemSettings()
        self.transformer_config = transformer_config or {}
        self.max_retries = max_retries
        self.enable_api_calls = enable_api_calls
        self.enable_mock_mode = enable_mock_mode
        
        # Create separate queues for each stage
        self.raw_events_queue = asyncio.Queue()
        self.processed_events_queue = asyncio.Queue()
        self.transformed_events_queue = asyncio.Queue()
        
        # Initialize services
        self.stream_generator = MockStreamGenerator(self.stream_settings)
        self.event_consumer = InternalEventConsumer()
        self.schema_transformer = SchemaTransformer(self.transformer_config)
        self.api_dispatcher = APIDispatcherService(
            self.schema_transformer, 
            max_retries=self.max_retries
        )
        
        # Pipeline state
        self.is_running = False
        self.stats = PipelineStats()
        self.tasks = []
    
    async def start_pipeline(self):
        """Start the integrated pipeline"""
        if self.is_running:
            logger.warning("Pipeline is already running")
            return
        
        self.is_running = True
        self.stats.start_time = datetime.now()
        self.stats.reset()
        
        logger.info("Starting integrated pipeline")
        
        # Start all pipeline tasks
        self.tasks = [
            asyncio.create_task(self._stream_generator_task()),
            asyncio.create_task(self._event_processor_task()),
            asyncio.create_task(self._transformer_task()),
            asyncio.create_task(self._api_dispatcher_task()),
            asyncio.create_task(self._api_monitor_task())
        ]
        
        try:
            # Wait for all tasks
            await asyncio.gather(*self.tasks)
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            self.is_running = False
            raise
    
    async def stop_pipeline(self):
        """Stop the pipeline"""
        self.is_running = False
        self.stream_generator.stop_streaming()
        logger.info("Pipeline stopped")
    
    async def _stream_generator_task(self):
        """Task for generating stream events"""
        logger.info("Starting stream generator task")
        
        while self.is_running:
            try:
                # Generate batch of events
                batch = await self.stream_generator.generate_batch()
                
                for event in batch:
                    if not self.is_running:
                        break
                    
                    # Add to raw events queue
                    await self.raw_events_queue.put(event)
                    self.stats.events_generated += 1
                    self.stats.last_event_time = datetime.now()
                
                # Wait between batches
                await asyncio.sleep(1 / self.stream_settings.events_per_second)
                
            except Exception as e:
                logger.error(f"Stream generator error: {e}")
                await asyncio.sleep(1)
    
    async def _event_processor_task(self):
        """Task for processing events through consumer"""
        logger.info("Starting event processor task")
        
        while self.is_running:
            try:
                # Get event from raw events queue
                event = await asyncio.wait_for(
                    self.raw_events_queue.get(), 
                    timeout=1.0
                )
                
                # Process through event consumer
                processed_event = await self.event_consumer.consume_event(event)
                
                if processed_event:
                    self.stats.events_processed += 1
                    # Add to processed events queue
                    await self.processed_events_queue.put(event)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Event processor error: {e}")
                self.stats.events_failed += 1
    
    async def _transformer_task(self):
        """Task for transforming events"""
        logger.info("Starting transformer task")
        
        while self.is_running:
            try:
                # Get event from processed events queue
                event = await asyncio.wait_for(
                    self.processed_events_queue.get(), 
                    timeout=1.0
                )
                
                # Transform the contact data
                try:
                    contact_data = event.get("item", {})
                    transformed_data = self.schema_transformer.transform_contact(contact_data)
                    
                    self.stats.events_transformed += 1
                    
                    # Add to transformed events queue
                    await self.transformed_events_queue.put(transformed_data)
                    
                except Exception as e:
                    logger.error(f"Transformation error: {e}")
                    self.stats.events_failed += 1
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Transformer error: {e}")
    
    async def _api_dispatcher_task(self):
        """Task for dispatching to external APIs"""
        logger.info("Starting API dispatcher task")
        
        while self.is_running:
            try:
                # Get event from transformed events queue
                transformed_data = await asyncio.wait_for(
                    self.transformed_events_queue.get(), 
                    timeout=1.0
                )
                
                if self.enable_api_calls:
                    # Dispatch to external API
                    try:
                        result = await self.api_dispatcher.dispatch_contact(transformed_data)
                        
                        if result.get("success", False):
                            self.stats.events_dispatched += 1
                            logger.info(f"Successfully dispatched to external API: {result}")
                        else:
                            logger.error(f"API dispatch failed: {result.get('error', 'Unknown error')}")
                            self.stats.events_failed += 1
                            
                    except Exception as e:
                        logger.error(f"API dispatch error: {e}")
                        self.stats.events_failed += 1
                else:
                    # Mock mode - just log
                    external_system = transformed_data.get('_metadata', {}).get('external_system', 'unknown')
                    logger.info(f"Mock mode: Would dispatch to {external_system}")
                    self.stats.events_dispatched += 1
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"API dispatcher error: {e}")
    
    async def _api_monitor_task(self):
        """Task for monitoring API health and performance"""
        logger.info("Starting API monitor task")
        
        while self.is_running:
            try:
                # Log pipeline stats every 30 seconds
                await asyncio.sleep(30)
                self._log_pipeline_stats()
                
            except Exception as e:
                logger.error(f"API monitor error: {e}")
    
    def _log_pipeline_stats(self):
        """Log current pipeline statistics"""
        if self.stats.start_time:
            runtime = datetime.now() - self.stats.start_time
            events_per_second = self.stats.events_dispatched / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
            
            logger.info(f"""
Pipeline Stats:
- Runtime: {runtime}
- Events Generated: {self.stats.events_generated}
- Events Processed: {self.stats.events_processed}
- Events Transformed: {self.stats.events_transformed}
- Events Dispatched: {self.stats.events_dispatched}
- Events Failed: {self.stats.events_failed}
- Events/Second: {events_per_second:.2f}
- Raw Queue Size: {self.raw_events_queue.qsize()}
- Processed Queue Size: {self.processed_events_queue.qsize()}
- Transformed Queue Size: {self.transformed_events_queue.qsize()}
            """)
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics"""
        runtime = None
        if self.stats.start_time:
            runtime = datetime.now() - self.stats.start_time
        
        return {
            "is_running": self.is_running,
            "runtime": str(runtime) if runtime else None,
            "events_generated": self.stats.events_generated,
            "events_processed": self.stats.events_processed,
            "events_transformed": self.stats.events_transformed,
            "events_dispatched": self.stats.events_dispatched,
            "events_failed": self.stats.events_failed,
            "raw_queue_size": self.raw_events_queue.qsize(),
            "processed_queue_size": self.processed_events_queue.qsize(),
            "transformed_queue_size": self.transformed_events_queue.qsize(),
            "stream_stats": self.stream_generator.get_stats(),
            "consumer_stats": self.event_consumer.get_stats(),
            "routing_config": self.schema_transformer.get_routing_configuration()
        }
    
    def update_stream_settings(self, new_settings: SystemSettings):
        """Update stream generator settings"""
        self.stream_settings = new_settings
        logger.info("Updated stream settings")
    
    def update_transformer_config(self, new_config: Dict[str, Any]):
        """Update schema transformer configuration"""
        self.transformer_config = new_config
        # Recreate transformer with new config
        self.schema_transformer = SchemaTransformer(self.transformer_config)
        self.api_dispatcher = APIDispatcherService(
            self.schema_transformer, 
            max_retries=self.max_retries
        )
        logger.info("Updated transformer configuration")
    
    def reset_stats(self):
        """Reset pipeline statistics"""
        self.stats.reset()
        logger.info("Pipeline stats reset")


class PipelineFactory:
    """Factory for creating different pipeline configurations"""
    
    @staticmethod
    def create_default_pipeline() -> IntegratedPipelineService:
        """Create default pipeline with API calls enabled"""
        return IntegratedPipelineService(
            enable_api_calls=True,
            enable_mock_mode=False
        )
    
    @staticmethod
    def create_high_volume_pipeline() -> IntegratedPipelineService:
        """Create high-volume pipeline"""
        settings = SystemSettings(
            events_per_second=50,
            batch_size=25,
            contact_types=["customer", "lead", "prospect", "vendor", "partner"]
        )
        return IntegratedPipelineService(
            stream_settings=settings,
            enable_api_calls=True,
            enable_mock_mode=False
        )
    
    @staticmethod
    def create_test_pipeline() -> IntegratedPipelineService:
        """Create test pipeline with mock mode"""
        settings = SystemSettings(
            events_per_second=5,
            batch_size=3,
            contact_types=["customer", "lead"]
        )
        return IntegratedPipelineService(
            stream_settings=settings,
            enable_api_calls=False,
            enable_mock_mode=True
        )
    
    @staticmethod
    def create_salesforce_only_pipeline() -> IntegratedPipelineService:
        """Create pipeline that only routes to Salesforce"""
        config = {
            "routing_rules": {
                "customer": "salesforce",
                "vendor": "salesforce", 
                "partner": "salesforce",
                "lead": "salesforce",
                "prospect": "salesforce",
                "employee": "salesforce"
            }
        }
        return IntegratedPipelineService(
            transformer_config=config,
            enable_api_calls=True,
            enable_mock_mode=False
        )
    
    @staticmethod
    def create_hubspot_only_pipeline() -> IntegratedPipelineService:
        """Create pipeline that only routes to HubSpot"""
        config = {
            "routing_rules": {
                "customer": "hubspot",
                "vendor": "hubspot",
                "partner": "hubspot", 
                "lead": "hubspot",
                "prospect": "hubspot",
                "employee": "hubspot"
            }
        }
        return IntegratedPipelineService(
            transformer_config=config,
            enable_api_calls=True,
            enable_mock_mode=False
        ) 