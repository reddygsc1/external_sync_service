import os
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from ..services.mock_stream_generator import SystemSettings

logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    """Application configuration"""

    # Mock Stream Generator Settings
    events_per_second: int = 5
    batch_size: int = 10
    enable_async: bool = True

    # Contact Type Distribution
    contact_type_distribution: Dict[str, float] = None

    # Operation Distribution
    operation_distribution: Dict[str, float] = None

    # Logging Settings
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Environment
    environment: str = "development"

    def __post_init__(self):
        if self.contact_type_distribution is None:
            self.contact_type_distribution = {
                "lead": 0.4,
                "customer": 0.25,
                "prospect": 0.2,
                "vendor": 0.1,
                "partner": 0.03,
                "employee": 0.02,
            }

        if self.operation_distribution is None:
            self.operation_distribution = {
                "create": 0.6,
                "update": 0.35,
                "delete": 0.05,
            }


class ConfigManager:
    """Manages application configuration"""

    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = AppConfig()
        self.load_config()

    def load_config(self):
        """Load configuration from file or environment variables"""
        try:
            # Try to load from file first
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    file_config = json.load(f)
                    self._update_config_from_dict(file_config)
                    logger.info(f"Configuration loaded from {self.config_file}")
            else:
                logger.info("No config file found, using default configuration")

            # Override with environment variables
            self._load_from_environment()

        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            logger.info("Using default configuration")

    def _load_from_environment(self):
        """Load configuration from environment variables"""
        env_mappings = {
            "EVENTS_PER_SECOND": "events_per_second",
            "BATCH_SIZE": "batch_size",
            "ENABLE_ASYNC": "enable_async",
            "LOG_LEVEL": "log_level",
            "ENVIRONMENT": "environment",
        }

        for env_var, config_attr in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                if config_attr == "enable_async":
                    setattr(self.config, config_attr, env_value.lower() == "true")
                elif config_attr in ["events_per_second", "batch_size"]:
                    setattr(self.config, config_attr, int(env_value))
                else:
                    setattr(self.config, config_attr, env_value)

        # Load distributions from environment if present
        contact_dist = os.getenv("CONTACT_TYPE_DISTRIBUTION")
        if contact_dist:
            try:
                self.config.contact_type_distribution = json.loads(contact_dist)
            except json.JSONDecodeError:
                logger.warning("Invalid CONTACT_TYPE_DISTRIBUTION format")

        operation_dist = os.getenv("OPERATION_DISTRIBUTION")
        if operation_dist:
            try:
                self.config.operation_distribution = json.loads(operation_dist)
            except json.JSONDecodeError:
                logger.warning("Invalid OPERATION_DISTRIBUTION format")

    def _update_config_from_dict(self, config_dict: Dict[str, Any]):
        """Update configuration from dictionary"""
        for key, value in config_dict.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    def save_config(self):
        """Save current configuration to file"""
        try:
            config_dict = asdict(self.config)
            with open(self.config_file, "w") as f:
                json.dump(config_dict, f, indent=2)
            logger.info(f"Configuration saved to {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False

    def get_system_settings(self) -> SystemSettings:
        """Convert AppConfig to SystemSettings"""
        return SystemSettings(
            events_per_second=self.config.events_per_second,
            batch_size=self.config.batch_size,
            enable_async=self.config.enable_async,
            contact_type_distribution=self.config.contact_type_distribution,
            operation_distribution=self.config.operation_distribution,
        )

    def update_config(self, **kwargs):
        """Update configuration with new values"""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                logger.info(f"Updated config: {key} = {value}")
            else:
                logger.warning(f"Unknown config key: {key}")

    def get_config(self) -> AppConfig:
        """Get current configuration"""
        return self.config

    def validate_config(self) -> bool:
        """Validate current configuration"""
        try:
            # Validate numeric values
            if self.config.events_per_second <= 0:
                logger.error("events_per_second must be positive")
                return False

            if self.config.batch_size <= 0:
                logger.error("batch_size must be positive")
                return False

            # Validate distributions
            contact_sum = sum(self.config.contact_type_distribution.values())
            if abs(contact_sum - 1.0) > 0.001:
                logger.error(
                    f"Contact type distribution must sum to 1.0, got {contact_sum}"
                )
                return False

            operation_sum = sum(self.config.operation_distribution.values())
            if abs(operation_sum - 1.0) > 0.001:
                logger.error(
                    f"Operation distribution must sum to 1.0, got {operation_sum}"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Configuration validation error: {e}")
            return False


# Global config manager instance
config_manager = ConfigManager()


def get_config() -> AppConfig:
    """Get application configuration"""
    return config_manager.get_config()


def get_system_settings() -> SystemSettings:
    """Get system settings for mock stream generator"""
    return config_manager.get_system_settings()


def update_config(**kwargs):
    """Update application configuration"""
    config_manager.update_config(**kwargs)


def save_config():
    """Save current configuration"""
    return config_manager.save_config()
