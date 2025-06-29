import asyncio
import logging
import signal
import sys


from .services.external_sync_service import PipelineFactory
from .utils.config_manager import get_config, get_system_settings, config_manager

logger = logging.getLogger(__name__)


def setup_logging(config):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format=config.log_format,
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("app.log")],
    )


async def run_external_sync_service():
    """Run the external sync service"""
    logger.info("Starting External Sync Service")

    # Create and start pipeline
    pipeline = PipelineFactory.create_default_pipeline()

    try:
        await pipeline.start_pipeline()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        await pipeline.stop_pipeline()
        logger.info("Pipeline shutdown complete")


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
        logger.info(
            f"Configuration loaded: {settings.events_per_second} events/sec, "
            f"batch_size: {settings.batch_size}"
        )

        await run_external_sync_service()

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
    if len(sys.argv) > 1:
        if sys.argv[1] in ["--help", "-h", "help"]:
            print(
                """
    System-to-System Record Sync Service

    Usage:
        python -m app.main                    # Run integrated pipeline


    The integrated pipeline connects stream generation, schema transformation,
    and external API calls in a complete end-to-end flow.
            """
            )
            sys.exit(0)
        else:
            print("Unknown mode specified, exiting integrated pipeline")
            sys.exit(0)

    # Run the application
    asyncio.run(main())
