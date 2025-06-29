import logging
from typing import Dict, Any, Optional, Union

from app.models.internal_schema import InternalContact
from app.services.adapter_manager import AdapterManager

logger = logging.getLogger(__name__)


class SchemaTransformer:
    """Pure schema transformer - only handles data format conversions"""

    def __init__(self, adapter_manager: AdapterManager):
        self.adapter_manager = adapter_manager

    def transform_contact(
        self, internal_data: Union[InternalContact, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Transform contact data to appropriate external format using Pydantic models"""
        # Ensure we have an InternalContact instance
        internal_contact = InternalContact(**internal_data)
        contact_type = internal_contact.contact
        
        # Get appropriate adapter
        adapter = self.adapter_manager.get_adapter(contact_type)

        # Transform the data
        external_data = adapter.transform_to_external(internal_contact)

        # Add system metadata
        external_system = self.adapter_manager.get_external_system_for_contact_type(contact_type)
        external_data["_metadata"]["external_system"] = external_system.value

        return external_data

    def transform_from_external(
        self, external_data: Dict[str, Any], adapter_name: str
    ) -> InternalContact:
        """Transform external data back to internal format using Pydantic models"""
        adapter = self.adapter_manager.adapters.get(adapter_name)

        if not adapter:
            raise ValueError(f"Adapter '{adapter_name}' not found")

        return adapter.transform_from_external(external_data)


class SchemaTransformerFactory:
    """Factory for creating schema transformers"""

    @staticmethod
    def create_default() -> SchemaTransformer:
        """Create schema transformer with default adapter manager"""
        from app.services.adapter_manager import AdapterManagerFactory
        adapter_manager = AdapterManagerFactory.create_default()
        return SchemaTransformer(adapter_manager)

    @staticmethod
    def create_with_config(config: Dict[str, Any]) -> SchemaTransformer:
        """Create schema transformer with custom configuration"""
        from app.services.adapter_manager import AdapterManagerFactory
        adapter_manager = AdapterManagerFactory.create_with_config(config)
        return SchemaTransformer(adapter_manager)
