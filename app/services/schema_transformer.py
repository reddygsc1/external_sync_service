import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from enum import Enum

from .adapters import SalesforceAdapter, HubSpotAdapter, BaseAdapter
from app.models.internal_schema import InternalContact, InternalContactEvent

logger = logging.getLogger(__name__)


class ExternalSystem(Enum):
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"


class ContactType(Enum):
    LEAD = "lead"
    CUSTOMER = "customer"
    PROSPECT = "prospect"
    VENDOR = "vendor"
    PARTNER = "partner"
    EMPLOYEE = "employee"


class SchemaTransformer:
    """Schema transformer with configuration-based adapter routing using Pydantic models"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.adapters: Dict[str, BaseAdapter] = {}
        self.contact_type_routing: Dict[str, str] = {}
        
        # Initialize adapters
        self._initialize_adapters()
        
        # Setup routing configuration
        self._setup_routing()
    
    def _initialize_adapters(self):
        """Initialize available adapters"""
        try:
            # Get adapter configurations from config
            salesforce_config = self.config.get("salesforce", {})
            hubspot_config = self.config.get("hubspot", {})
            
            # Create adapters
            self.adapters["salesforce"] = SalesforceAdapter(salesforce_config)
            self.adapters["hubspot"] = HubSpotAdapter(hubspot_config)
            
            logger.info(f"Initialized {len(self.adapters)} adapters: {list(self.adapters.keys())}")
            
        except Exception as e:
            logger.error(f"Error initializing adapters: {e}")
            raise
    
    def _setup_routing(self):
        """Setup contact type to adapter routing"""
        # Default routing configuration
        default_routing = {
            "lead": "salesforce",
            "customer": "hubspot", 
            "prospect": "salesforce",
            "vendor": "hubspot",
            "partner": "hubspot",
            "employee": "salesforce"
        }
        
        # Override with config if provided
        config_routing = self.config.get("contact_type_routing", {})
        self.contact_type_routing = {**default_routing, **config_routing}
        
        logger.info(f"Contact type routing configured: {self.contact_type_routing}")
    
    def get_adapter(self, contact_type: str) -> BaseAdapter:
        """Get appropriate adapter based on contact type"""
        adapter_name = self.contact_type_routing.get(contact_type)
        
        if not adapter_name:
            logger.warning(f"No adapter configured for contact type '{contact_type}', using default")
            adapter_name = "salesforce"  # Default fallback
        
        adapter = self.adapters.get(adapter_name)
        
        if not adapter:
            raise ValueError(f"Adapter '{adapter_name}' not found for contact type '{contact_type}'")
        
        logger.info(f"Routing contact type '{contact_type}' to {adapter_name} adapter")
        return adapter
    
    def get_external_system(self, contact_type: str) -> ExternalSystem:
        """Get external system enum based on contact type"""
        adapter_name = self.contact_type_routing.get(contact_type, "salesforce")
        
        # Map adapter name to ExternalSystem enum
        if adapter_name == "salesforce":
            return ExternalSystem.SALESFORCE
        elif adapter_name == "hubspot":
            return ExternalSystem.HUBSPOT
        else:
            # Default fallback
            return ExternalSystem.SALESFORCE
    
    def transform_contact(self, internal_data: Union[InternalContact, Dict[str, Any]]) -> Dict[str, Any]:
        """Transform contact data to appropriate external format using Pydantic models"""
        # Ensure we have an InternalContact instance
        if isinstance(internal_data, dict):
            internal_contact = InternalContact(**internal_data)
        else:
            internal_contact = internal_data
        
        contact_type = internal_contact.contact
        adapter = self.get_adapter(contact_type)
        
        # Transform the data
        external_data = adapter.transform_to_external(internal_contact)
        
        # Add system metadata
        external_system = self.get_external_system(contact_type)
        external_data["_metadata"]["external_system"] = external_system.value
        
        return external_data
    
    def transform_from_external(self, external_data: Dict[str, Any], adapter_name: str) -> InternalContact:
        """Transform external data back to internal format using Pydantic models"""
        adapter = self.adapters.get(adapter_name)
        
        if not adapter:
            raise ValueError(f"Adapter '{adapter_name}' not found")
        
        return adapter.transform_from_external(external_data)
    
    def validate_external_data(self, data: Dict[str, Any], adapter_name: str) -> bool:
        """Validate external data using specified adapter"""
        adapter = self.adapters.get(adapter_name)
        
        if not adapter:
            logger.error(f"Adapter '{adapter_name}' not found for validation")
            return False
        
        return adapter.validate_external_data(data)
    
    def validate_internal_data(self, data: Union[InternalContact, Dict[str, Any]]) -> bool:
        """Validate internal data using Pydantic model"""
        try:
            if isinstance(data, dict):
                InternalContact(**data)
            else:
                # If it's already an InternalContact, validate it
                data.model_validate(data.dict())
            return True
        except Exception as e:
            logger.error(f"Internal data validation error: {e}")
            return False
    
    def get_supported_contact_types(self, adapter_name: str) -> List[str]:
        """Get list of contact types supported by specified adapter"""
        adapter = self.adapters.get(adapter_name)
        
        if not adapter:
            logger.error(f"Adapter '{adapter_name}' not found")
            return []
        
        return adapter.get_supported_contact_types()
    
    def get_all_supported_contact_types(self) -> Dict[str, List[str]]:
        """Get all supported contact types by adapter"""
        return {
            adapter_name: adapter.get_supported_contact_types()
            for adapter_name, adapter in self.adapters.items()
        }
    
    def get_routing_configuration(self) -> Dict[str, Any]:
        """Get current routing configuration"""
        return {
            "contact_type_routing": self.contact_type_routing,
            "adapters": list(self.adapters.keys()),
            "supported_contact_types": self.get_all_supported_contact_types()
        }
    
    def update_routing_configuration(self, new_routing: Dict[str, str]):
        """Update contact type routing configuration"""
        self.contact_type_routing.update(new_routing)
        logger.info(f"Updated routing configuration: {self.contact_type_routing}")
    
    def add_adapter(self, name: str, adapter: BaseAdapter):
        """Add a new adapter"""
        self.adapters[name] = adapter
        logger.info(f"Added adapter '{name}': {adapter.__class__.__name__}")
    
    def remove_adapter(self, name: str):
        """Remove an adapter"""
        if name in self.adapters:
            del self.adapters[name]
            logger.info(f"Removed adapter '{name}'")
        else:
            logger.warning(f"Adapter '{name}' not found for removal")


class SchemaTransformerFactory:
    """Factory for creating schema transformers with different configurations"""
    
    @staticmethod
    def create_default() -> SchemaTransformer:
        """Create schema transformer with default configuration"""
        return SchemaTransformer()
    
    @staticmethod
    def create_with_config(config: Dict[str, Any]) -> SchemaTransformer:
        """Create schema transformer with custom configuration"""
        return SchemaTransformer(config)
    
    @staticmethod
    def create_salesforce_only() -> SchemaTransformer:
        """Create schema transformer that routes everything to Salesforce"""
        config = {
            "contact_type_routing": {
                "lead": "salesforce",
                "customer": "salesforce",
                "prospect": "salesforce", 
                "vendor": "salesforce",
                "partner": "salesforce",
                "employee": "salesforce"
            }
        }
        return SchemaTransformer(config)
    
    @staticmethod
    def create_hubspot_only() -> SchemaTransformer:
        """Create schema transformer that routes everything to HubSpot"""
        config = {
            "contact_type_routing": {
                "lead": "hubspot",
                "customer": "hubspot",
                "prospect": "hubspot",
                "vendor": "hubspot", 
                "partner": "hubspot",
                "employee": "hubspot"
            }
        }
        return SchemaTransformer(config) 