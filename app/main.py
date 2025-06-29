import asyncio
import logging
import signal
import sys
from typing import Optional

from .services.app_startup_service import start_app_services, stop_app_services, get_app_status
from .services.integrated_pipeline_service import IntegratedPipelineService, PipelineFactory
from .utils.config_manager import get_config, get_system_settings, config_manager

logger = logging.getLogger(__name__)


def setup_logging(config):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format=config.log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log')
        ]
    )


async def run_integrated_pipeline():
    """Run the integrated pipeline service"""
    logger.info("Starting Integrated Pipeline Service")
    
    # Create and start pipeline
    pipeline = PipelineFactory.create_default_pipeline()
    
    try:
        await pipeline.start_pipeline()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        await pipeline.stop_pipeline()
        logger.info("Pipeline shutdown complete")


async def run_legacy_services():
    """Run the legacy individual services"""
    logger.info("Starting Legacy Services")
    
    try:
        # Start application services
        success = await start_app_services()
        if not success:
            logger.error("Failed to start application services")
            sys.exit(1)
        
        logger.info("Application started successfully. Press Ctrl+C to stop.")
        
        # Keep the application running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        await stop_app_services()
        logger.info("Application shutdown complete")


async def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = get_config()
        
        # Setup logging
        setup_logging(config)
        logger.info("Starting System-to-System Record Sync Service")
        
        # Validate configuration
        if not config_manager.validate_config():
            logger.error("Invalid configuration. Exiting.")
            sys.exit(1)
        
        # Get system settings
        settings = get_system_settings()
        logger.info(f"Configuration loaded: {settings.events_per_second} events/sec, "
                   f"batch_size: {settings.batch_size}")
        
        # Check command line arguments for mode
        if len(sys.argv) > 1:
            mode = sys.argv[1].lower()
            
            if mode == "pipeline" or mode == "integrated":
                await run_integrated_pipeline()
            elif mode == "legacy" or mode == "services":
                await run_legacy_services()
            else:
                logger.error(f"Unknown mode: {mode}")
                logger.info("Available modes: pipeline (or integrated), legacy (or services)")
                sys.exit(1)
        else:
            # Default to integrated pipeline
            logger.info("No mode specified, running integrated pipeline (use --help for options)")
            await run_integrated_pipeline()
            
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    sys.exit(0)


if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Show help if requested
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h", "help"]:
        print("""
System-to-System Record Sync Service

Usage:
    python -m app.main [mode]

Modes:
    pipeline, integrated  - Run integrated pipeline (default)
    legacy, services     - Run legacy individual services

Examples:
    python -m app.main                    # Run integrated pipeline
    python -m app.main pipeline           # Run integrated pipeline
    python -m app.main legacy             # Run legacy services
    python -m app.main --help             # Show this help

The integrated pipeline connects stream generation, schema transformation,
and external API calls in a complete end-to-end flow.
        """)
        sys.exit(0)
    
    # Run the application
    asyncio.run(main()) 