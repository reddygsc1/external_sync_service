import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from .mock_stream_generator import MockStreamGenerator, SystemSettings
from .schema_transformer import SchemaTransformerFactory
from .api_dispatcher_service import APIDispatcherService
from .event_consumer_service import InternalEventConsumer
from app.models.internal_schema import InternalContact, InternalContactEvent

logger = logging.getLogger(__name__)


class IntegratedPipelineService:
    """Integrated pipeline service that connects stream generator to external APIs"""

    def __init__(
        self,
        stream_settings: Optional[SystemSettings] = None,
        transformer_config: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        enable_api_calls: bool = True,
        enable_mock_mode: bool = False,
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
        self.schema_transformer = SchemaTransformerFactory.create_with_config(self.transformer_config)
        self.api_dispatcher = APIDispatcherService(max_retries=self.max_retries)

        # Pipeline state
        self.is_running = False
        self.tasks = []

    async def start_pipeline(self):
        """Start the integrated pipeline"""
        if self.is_running:
            logger.warning("Pipeline is already running")
            return

        self.is_running = True
        logger.info("Starting integrated pipeline")

        # Start all pipeline tasks
        self.tasks = [
            asyncio.create_task(self._stream_generator_task()),
            asyncio.create_task(self._event_processor_task()),
            asyncio.create_task(self._transformer_task()),
            asyncio.create_task(self._api_dispatcher_task()),
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
                event = await asyncio.wait_for(self.raw_events_queue.get(), timeout=1.0)

                # Process through event consumer
                processed_event = await self.event_consumer.consume_event(event)

                if processed_event:
                    # Add to processed events queue
                    await self.processed_events_queue.put(event)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Event processor error: {e}")

    async def _transformer_task(self):
        """Task for transforming events"""
        logger.info("Starting transformer task")

        while self.is_running:
            try:
                # Get event from processed events queue
                event = await asyncio.wait_for(
                    self.processed_events_queue.get(), timeout=1.0
                )

                # Transform the contact data
                try:
                    contact_data = event.get("item", {})
                    transformed_data = self.schema_transformer.transform_contact(
                        contact_data
                    )

                    # Add to transformed events queue
                    await self.transformed_events_queue.put(transformed_data)

                except Exception as e:
                    logger.error(f"Transformation error: {e}")

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
                    self.transformed_events_queue.get(), timeout=1.0
                )

                if self.enable_api_calls:
                    # Dispatch to external API
                    try:
                        result = await self.api_dispatcher.dispatch_transformed_contact(
                            transformed_data
                        )

                        if result.get("success", False):
                            logger.info(
                                f"Successfully dispatched to external API: {result}"
                            )
                        else:
                            logger.error(
                                f"API dispatch failed: {result.get('error', 'Unknown error')}"
                            )

                    except Exception as e:
                        logger.error(f"API dispatch error: {e}")
                else:
                    # Mock mode - just log
                    external_system = transformed_data.get("_metadata", {}).get(
                        "external_system", "unknown"
                    )
                    logger.info(f"Mock mode: Would dispatch to {external_system}")

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"API dispatcher error: {e}")

    def update_stream_settings(self, new_settings: SystemSettings):
        """Update stream generator settings"""
        self.stream_settings = new_settings
        logger.info("Updated stream settings")

    def update_transformer_config(self, new_config: Dict[str, Any]):
        """Update schema transformer configuration"""
        self.transformer_config = new_config
        # Recreate transformer with new config
        self.schema_transformer = SchemaTransformerFactory.create_with_config(self.transformer_config)
        self.api_dispatcher = APIDispatcherService(max_retries=self.max_retries)
        logger.info("Updated transformer configuration")


class PipelineFactory:
    """Factory for creating different pipeline configurations"""

    @staticmethod
    def create_default_pipeline() -> IntegratedPipelineService:
        """Create default pipeline with API calls enabled"""
        return IntegratedPipelineService(enable_api_calls=True, enable_mock_mode=False)

    @staticmethod
    def create_high_volume_pipeline() -> IntegratedPipelineService:
        """Create high-volume pipeline"""
        settings = SystemSettings(
            events_per_second=50,
            batch_size=25,
            contact_types=["customer", "lead", "prospect", "vendor", "partner"],
        )
        return IntegratedPipelineService(
            stream_settings=settings, enable_api_calls=True, enable_mock_mode=False
        )
