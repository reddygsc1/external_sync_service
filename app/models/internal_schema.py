from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Literal
from datetime import datetime


class InternalContact(BaseModel):
    """Internal contact schema model"""
    
    id: str = Field(..., description="Unique contact identifier")
    name: str = Field(..., min_length=1, max_length=255, description="Full name of the contact")
    email: EmailStr = Field(..., description="Email address")
    phone: str = Field(..., min_length=10, max_length=20, description="Phone number")
    contact: Literal["lead", "customer", "prospect", "vendor", "partner", "employee"] = Field(
        ..., description="Contact type"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    
    # Optional fields
    company: Optional[str] = Field(None, max_length=255, description="Company name")
    title: Optional[str] = Field(None, max_length=255, description="Job title")
    department: Optional[str] = Field(None, max_length=255, description="Department")
    address: Optional[str] = Field(None, max_length=500, description="Address")
    notes: Optional[str] = Field(None, max_length=1000, description="Additional notes")
    
    @validator('phone')
    def validate_phone(cls, v):
        """Validate phone number format"""
        # Remove common separators
        cleaned = ''.join(filter(str.isdigit, v))
        if len(cleaned) < 10:
            raise ValueError('Phone number must have at least 10 digits')
        return v
    
    @validator('name')
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
                "id": "C12345",
                "name": "John Doe",
                "email": "john.doe@example.com",
                "phone": "+1-555-123-4567",
                "contact": "customer",
                "created_at": "2024-01-15T10:30:00.123456",
                "updated_at": "2024-01-15T10:30:00.123456",
                "company": "Acme Corp",
                "title": "Senior Manager",
                "department": "Sales"
            }
        }


class InternalContactEvent(BaseModel):
    """Internal contact event schema"""
    
    record: Literal["contacts"] = Field(..., description="Record type")
    operation: Literal["create", "update", "delete"] = Field(..., description="Operation type")
    timestamp: datetime = Field(..., description="Event timestamp")
    item: InternalContact = Field(..., description="Contact data")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "record": "contacts",
                "operation": "create",
                "timestamp": "2024-01-15T10:30:00.123456",
                "item": {
                    "id": "C12345",
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "phone": "+1-555-123-4567",
                    "contact": "customer",
                    "created_at": "2024-01-15T10:30:00.123456",
                    "updated_at": "2024-01-15T10:30:00.123456"
                }
            }
        } 