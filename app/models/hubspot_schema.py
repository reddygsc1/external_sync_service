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

    # Timestamps
    createdate: datetime = Field(..., description="Creation date")
    lastmodifieddate: datetime = Field(..., description="Last modified date")

    @validator("phone")
    def validate_phone(cls, v):
        """Validate phone number format"""
        cleaned = "".join(filter(str.isdigit, v))
        if len(cleaned) < 10:
            raise ValueError("Phone number must have at least 10 digits")
        return v

    @validator("firstname", "lastname")
    def validate_name(cls, v):
        """Validate name format"""
        if not v.strip():
            raise ValueError("Name cannot be empty")
        return v.strip()

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
        schema_extra = {
            "example": {
                "firstname": "John",
                "lastname": "Doe",
                "email": "john.doe@example.com",
                "phone": "+1-555-123-4567",
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
                },
            }
        }
