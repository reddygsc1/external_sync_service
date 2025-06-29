# Models package
from .internal_schema import InternalContact, InternalContactEvent
from .salesforce_schema import SalesforceContact, SalesforceContactResponse, SalesforceContactQuery
from .hubspot_schema import (
    HubSpotContact, 
    HubSpotContactProperties, 
    HubSpotContactResponse, 
    HubSpotContactQuery,
    HubSpotContactBatch
)

__all__ = [
    'InternalContact',
    'InternalContactEvent',
    'SalesforceContact',
    'SalesforceContactResponse', 
    'SalesforceContactQuery',
    'HubSpotContact',
    'HubSpotContactProperties',
    'HubSpotContactResponse',
    'HubSpotContactQuery',
    'HubSpotContactBatch'
] 