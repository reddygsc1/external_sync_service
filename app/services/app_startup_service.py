import asyncio
import logging
from typing import Optional
from .mock_stream_generator import MockStreamGenerator, SystemSettings
from .event_consumer_service import InternalEventConsumer, MockEventConsumer

logger = logging.getLogger(__name__)


class AppStartupService:
    """Service responsible for starting the mock stream generator on app startup"""
    
    def __init__(self):
        self.mock_generator: Optional[MockStreamGenerator] = None
        self.event_consumer: Optional[InternalEventConsumer] = None
        self.is_started = False
        self.startup_task: Optional[asyncio.Task] = None
    
    async def start_services(self, settings: Optional[SystemSettings] = None):
        """Start the mock stream generator and event consumer services"""
        try:
            logger.info("Starting application services...")
            
            # Initialize settings if not provided
            if settings is None:
                settings = SystemSettings()
            
            # Initialize event consumer
            self.event_consumer = InternalEventConsumer()
            logger.info("Event consumer service initialized")
            
            # Initialize mock stream generator
            self.mock_generator = MockStreamGenerator(
                settings=settings,
                event_consumer_service=self.event_consumer
            )
            logger.info("Mock stream generator initialized")
            
            # Start the streaming in background
            self.startup_task = asyncio.create_task(self.mock_generator.start_streaming())
            
            self.is_started = True
            logger.info("Application services started successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start application services: {e}")
            return False
    
    async def stop_services(self):
        """Stop the mock stream generator and event consumer services"""
        try:
            logger.info("Stopping application services...")
            
            if self.mock_generator:
                self.mock_generator.stop_streaming()
            
            if self.startup_task and not self.startup_task.done():
                self.startup_task.cancel()
                try:
                    await self.startup_task
                except asyncio.CancelledError:
                    pass
            
            self.is_started = False
            logger.info("Application services stopped successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop application services: {e}")
            return False
    
    def get_service_status(self) -> dict:
        """Get status of all services"""
        status = {
            "is_started": self.is_started,
            "mock_generator": None,
            "event_consumer": None
        }
        
        if self.mock_generator:
            status["mock_generator"] = self.mock_generator.get_stats()
        
        if self.event_consumer:
            status["event_consumer"] = self.event_consumer.get_stats()
        
        return status
    
    def update_settings(self, new_settings: SystemSettings):
        """Update system settings (requires restart)"""
        if self.mock_generator:
            self.mock_generator.settings = new_settings
            logger.info("System settings updated")
        else:
            logger.warning("Mock generator not initialized, settings not updated")


# Global instance for easy access
app_startup_service = AppStartupService()


async def start_app_services():
    """Convenience function to start app services"""
    return await app_startup_service.start_services()


async def stop_app_services():
    """Convenience function to stop app services"""
    return await app_startup_service.stop_services()


def get_app_status():
    """Convenience function to get app status"""
    return app_startup_service.get_service_status() 