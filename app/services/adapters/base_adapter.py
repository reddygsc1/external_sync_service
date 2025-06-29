import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union
from datetime import datetime

from app.models.internal_schema import InternalContact

logger = logging.getLogger(__name__)


class BaseAdapter(ABC):
    """Abstract base class for external system adapters using Pydantic models"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.name = self.__class__.__name__
    
    @abstractmethod
    def transform_to_external(self, internal_data: Union[InternalContact, Dict[str, Any]]) -> Dict[str, Any]:
        """Transform internal schema to external schema"""
        pass
    
    @abstractmethod
    def transform_from_external(self, external_data: Dict[str, Any]) -> InternalContact:
        """Transform external schema to internal schema"""
        pass
    
    @abstractmethod
    def validate_external_data(self, data: Dict[str, Any]) -> bool:
        """Validate external data format"""
        pass
    
    @abstractmethod
    def get_supported_contact_types(self) -> list:
        """Get list of contact types supported by this adapter"""
        pass
    
    def extract_first_name(self, full_name: str) -> str:
        """Extract first name from full name"""
        parts = full_name.split()
        return parts[0] if parts else ""
    
    def extract_last_name(self, full_name: str) -> str:
        """Extract last name from full name"""
        parts = full_name.split()
        return " ".join(parts[1:]) if len(parts) > 1 else ""
    
    def extract_company_from_email(self, email: str) -> str:
        """Extract company name from email domain"""
        if "@" in email:
            domain = email.split("@")[1]
            return domain.split(".")[0].title()
        return ""
    
    def add_metadata(self, data: Dict[str, Any], internal_data: Union[InternalContact, Dict[str, Any]]) -> Dict[str, Any]:
        """Add metadata to transformed data"""
        contact_id = internal_data.id if isinstance(internal_data, InternalContact) else internal_data.get("id", "")
        contact_type = internal_data.contact if isinstance(internal_data, InternalContact) else internal_data.get("contact", "")
        
        data["_metadata"] = {
            "adapter": self.name,
            "contact_type": contact_type,
            "transformed_at": datetime.now().isoformat(),
            "original_id": contact_id
        }
        return data
    
    def log_transformation(self, contact_id: str, direction: str):
        """Log transformation activity"""
        logger.info(f"{self.name} {direction} transformation for contact {contact_id}")
    
    def log_error(self, error: Exception, context: str = ""):
        """Log transformation error"""
        logger.error(f"{self.name} error {context}: {error}")
    
    def _ensure_internal_contact(self, data: Union[InternalContact, Dict[str, Any]]) -> InternalContact:
        """Ensure data is an InternalContact instance"""
        if isinstance(data, InternalContact):
            return data
        elif isinstance(data, dict):
            return InternalContact(**data)
        else:
            raise ValueError(f"Expected InternalContact or dict, got {type(data)}")
    
    def _validate_internal_data(self, data: Union[InternalContact, Dict[str, Any]]) -> InternalContact:
        """Validate and return InternalContact instance"""
        try:
            internal_contact = self._ensure_internal_contact(data)
            return internal_contact
        except Exception as e:
            logger.error(f"Invalid internal data: {e}")
            raise ValueError(f"Invalid internal contact data: {e}") 