from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Literal
from datetime import datetime


class SalesforceContact(BaseModel):
    """Salesforce contact schema model"""
    
    # Core fields
    Id: str = Field(..., description="Salesforce Contact ID")
    FirstName: str = Field(..., min_length=1, max_length=40, description="First name")
    LastName: str = Field(..., min_length=1, max_length=80, description="Last name")
    Email: EmailStr = Field(..., description="Email address")
    Phone: str = Field(..., min_length=10, max_length=40, description="Phone number")
    
    # Salesforce-specific fields
    LeadSource: str = Field(..., max_length=255, description="Lead source")
    Status: str = Field(..., max_length=255, description="Contact status")
    Type: str = Field(..., max_length=255, description="Contact type")
    
    # Timestamps
    CreatedDate: datetime = Field(..., description="Creation date")
    LastModifiedDate: datetime = Field(..., description="Last modified date")
    
    # Optional fields
    Company: Optional[str] = Field(None, max_length=255, description="Company name")
    Title: Optional[str] = Field(None, max_length=128, description="Job title")
    Department: Optional[str] = Field(None, max_length=80, description="Department")
    Description: Optional[str] = Field(None, max_length=32000, description="Description")
    AccountId: Optional[str] = Field(None, max_length=18, description="Account ID")
    
    # Custom fields
    Contact_Type__c: Optional[str] = Field(None, max_length=255, description="Custom contact type field")
    Internal_ID__c: Optional[str] = Field(None, max_length=255, description="Internal system ID")
    
    @validator('Phone')
    def validate_phone(cls, v):
        """Validate phone number format"""
        cleaned = ''.join(filter(str.isdigit, v))
        if len(cleaned) < 10:
            raise ValueError('Phone number must have at least 10 digits')
        return v
    
    @validator('FirstName', 'LastName')
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
                "Id": "0031234567890ABC",
                "FirstName": "John",
                "LastName": "Doe",
                "Email": "john.doe@example.com",
                "Phone": "+1-555-123-4567",
                "LeadSource": "Website",
                "Status": "Active",
                "Type": "Customer",
                "CreatedDate": "2024-01-15T10:30:00.000+0000",
                "LastModifiedDate": "2024-01-15T10:30:00.000+0000",
                "Company": "Acme Corp",
                "Title": "Senior Manager",
                "Department": "Sales"
            }
        }


class SalesforceContactResponse(BaseModel):
    """Salesforce API response model"""
    
    success: bool = Field(..., description="Operation success status")
    id: Optional[str] = Field(None, description="Created/Updated record ID")
    errors: Optional[list] = Field(None, description="Error messages")
    created: Optional[bool] = Field(None, description="Whether record was created")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "id": "0031234567890ABC",
                "created": True
            }
        }


class SalesforceContactQuery(BaseModel):
    """Salesforce contact query model"""
    
    Id: str = Field(..., description="Contact ID to query")
    fields: Optional[list] = Field(None, description="Fields to retrieve")
    
    class Config:
        schema_extra = {
            "example": {
                "Id": "0031234567890ABC",
                "fields": ["Id", "FirstName", "LastName", "Email", "Phone"]
            }
        } 