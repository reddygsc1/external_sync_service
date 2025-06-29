# Models package
from .internal_schema import InternalContact, InternalContactEvent
from .salesforce_schema import SalesforceContact
from .hubspot_schema import HubSpotContact, HubSpotContactProperties

__all__ = [
    "InternalContact",
    "InternalContactEvent",
    "SalesforceContact",
    "HubSpotContact",
    "HubSpotContactProperties",
]
