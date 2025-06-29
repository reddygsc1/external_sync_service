from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Literal, Dict, Any
from datetime import datetime


class HubSpotContactProperties(BaseModel):
    """HubSpot contact properties model"""
    
    # Core properties
    firstname: str = Field(..., min_length=1, max_length=255, description="First name")
    lastname: str = Field(..., min_length=1, max_length=255, description="Last name")
    email: EmailStr = Field(..., description="Email address")
    phone: str = Field(..., min_length=10, max_length=20, description="Phone number")
    
    # HubSpot-specific properties
    lifecyclestage: str = Field(..., max_length=255, description="Lifecycle stage")
    hs_lead_status: str = Field(..., max_length=255, description="Lead status")
    
    # Timestamps
    createdate: datetime = Field(..., description="Creation date")
    lastmodifieddate: datetime = Field(..., description="Last modified date")
    
    # Optional properties
    company: Optional[str] = Field(None, max_length=255, description="Company name")
    jobtitle: Optional[str] = Field(None, max_length=255, description="Job title")
    department: Optional[str] = Field(None, max_length=255, description="Department")
    address: Optional[str] = Field(None, max_length=500, description="Address")
    
    # HubSpot analytics
    hs_analytics_source: Optional[str] = Field(None, max_length=255, description="Analytics source")
    hs_analytics_source_data_1: Optional[str] = Field(None, max_length=255, description="Analytics source data")
    
    # Custom properties
    customer_type: Optional[str] = Field(None, max_length=255, description="Customer type")
    account_id: Optional[str] = Field(None, max_length=255, description="Account ID")
    relationship_type: Optional[str] = Field(None, max_length=255, description="Relationship type")
    internal_id: Optional[str] = Field(None, max_length=255, description="Internal system ID")
    
    @validator('phone')
    def validate_phone(cls, v):
        """Validate phone number format"""
        cleaned = ''.join(filter(str.isdigit, v))
        if len(cleaned) < 10:
            raise ValueError('Phone number must have at least 10 digits')
        return v
    
    @validator('firstname', 'lastname')
    def validate_name(cls, v):
        """Validate name format"""
        if not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "firstname": "John",
                "lastname": "Doe",
                "email": "john.doe@example.com",
                "phone": "+1-555-123-4567",
                "lifecyclestage": "customer",
                "hs_lead_status": "CUSTOMER",
                "createdate": "2024-01-15T10:30:00.000Z",
                "lastmodifieddate": "2024-01-15T10:30:00.000Z",
                "company": "Acme Corp",
                "jobtitle": "Senior Manager",
                "department": "Sales"
            }
        }


class HubSpotContact(BaseModel):
    """HubSpot contact schema model"""
    
    id: str = Field(..., description="HubSpot contact ID")
    properties: HubSpotContactProperties = Field(..., description="Contact properties")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "12345",
                "properties": {
                    "firstname": "John",
                    "lastname": "Doe",
                    "email": "john.doe@example.com",
                    "phone": "+1-555-123-4567",
                    "lifecyclestage": "customer",
                    "hs_lead_status": "CUSTOMER",
                    "createdate": "2024-01-15T10:30:00.000Z",
                    "lastmodifieddate": "2024-01-15T10:30:00.000Z"
                }
            }
        }


class HubSpotContactResponse(BaseModel):
    """HubSpot API response model"""
    
    id: str = Field(..., description="Contact ID")
    properties: Dict[str, Any] = Field(..., description="Contact properties")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Update timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "12345",
                "properties": {
                    "firstname": "John",
                    "lastname": "Doe",
                    "email": "john.doe@example.com"
                },
                "created_at": "2024-01-15T10:30:00.000Z",
                "updated_at": "2024-01-15T10:30:00.000Z"
            }
        }


class HubSpotContactQuery(BaseModel):
    """HubSpot contact query model"""
    
    id: str = Field(..., description="Contact ID to query")
    properties: Optional[list] = Field(None, description="Properties to retrieve")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "12345",
                "properties": ["firstname", "lastname", "email", "phone"]
            }
        }


class HubSpotContactBatch(BaseModel):
    """HubSpot batch contact model"""
    
    inputs: list[HubSpotContact] = Field(..., description="List of contacts to process")
    
    class Config:
        schema_extra = {
            "example": {
                "inputs": [
                    {
                        "id": "12345",
                        "properties": {
                            "firstname": "John",
                            "lastname": "Doe",
                            "email": "john.doe@example.com"
                        }
                    }
                ]
            }
        } 