# Adapters package
from .base_adapter import BaseAdapter
from .salesforce_adapter import SalesforceAdapter
from .hubspot_adapter import HubSpotAdapter

__all__ = ['BaseAdapter', 'SalesforceAdapter', 'HubSpotAdapter'] 