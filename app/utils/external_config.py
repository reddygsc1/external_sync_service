from enum import Enum
from typing import Dict, Any


class ExternalSystem(Enum):
    """External system enum"""

    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"


# Default routing configuration
DEFAULT_CONTACT_TYPE_ROUTING = {
    "lead": "salesforce",
    "customer": "hubspot",
    "prospect": "salesforce",
    "vendor": "hubspot",
    "partner": "hubspot",
    "employee": "salesforce",
}


def get_contact_type_routing(config: Dict[str, Any] = None) -> Dict[str, str]:
    """Get contact type routing configuration with defaults and overrides"""
    config = config or {}
    config_routing = config.get("contact_type_routing", {})
    return {**DEFAULT_CONTACT_TYPE_ROUTING, **config_routing}


def get_external_system(contact_type: str, routing_config: Dict[str, str] = None) -> ExternalSystem:
    """Get external system enum based on contact type"""
    if routing_config is None:
        routing_config = DEFAULT_CONTACT_TYPE_ROUTING
    
    adapter_name = routing_config.get(contact_type, "salesforce")

    # Map adapter name to ExternalSystem enum
    if adapter_name == "salesforce":
        return ExternalSystem.SALESFORCE
    elif adapter_name == "hubspot":
        return ExternalSystem.HUBSPOT
    else:
        # Default fallback
        return ExternalSystem.SALESFORCE