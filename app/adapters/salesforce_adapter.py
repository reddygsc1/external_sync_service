from logging import Logger
from typing import Dict, Any, Union
from app.adapters.base_adapter import BaseAdapter
from app.models.internal_schema import InternalContact
from app.models.salesforce_schema import SalesforceContact


class SalesforceAdapter(BaseAdapter):
    """Salesforce adapter for contact data transformation using Pydantic models"""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.supported_contact_types = ["lead", "prospect", "employee"]

    def transform_to_external(
        self, internal_data: Union[InternalContact, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Transform internal contact data to Salesforce format using Pydantic models"""
        try:
            # Validate and get InternalContact instance
            internal_contact = self._validate_internal_data(internal_data)

            # Create Salesforce contact using Pydantic model
            salesforce_contact = SalesforceContact(
                Id=internal_contact.id,
                FirstName=self.extract_first_name(internal_contact.name),
                LastName=self.extract_last_name(internal_contact.name),
                Email=internal_contact.email,
                Phone=internal_contact.phone,
                CreatedDate=internal_contact.created_at,
                LastModifiedDate=internal_contact.updated_at,
            )

            # Convert to dict and add metadata
            salesforce_data = salesforce_contact.model_dump(exclude_none=True)
            salesforce_data = self.add_metadata(salesforce_data, internal_contact)

            self.log_transformation(internal_contact.id, "to external")
            return salesforce_data

        except Exception as e:
            self.log_error(e, "transforming to external")
            raise

    def transform_from_external(self, external_data: Dict[str, Any]) -> InternalContact:
        """Transform Salesforce data to internal format using Pydantic models"""
        try:
            # Validate external data using Pydantic model
            salesforce_contact = SalesforceContact(**external_data)

            # Create InternalContact using Pydantic model
            # Only use fields that are actually defined in the schema
            internal_contact = InternalContact(
                id=salesforce_contact.Id,
                name=f"{salesforce_contact.FirstName} {salesforce_contact.LastName}".strip(),
                email=salesforce_contact.Email,
                phone=salesforce_contact.Phone,
                contact="lead",  # Default since we don't have Type in schema
                created_at=salesforce_contact.CreatedDate,
                updated_at=salesforce_contact.LastModifiedDate,

            )

            self.log_transformation(salesforce_contact.Id, "from external")
            return internal_contact

        except Exception as e:
            self.log_error(e, "transforming from external")
            raise

    def validate_external_data(self, data: Dict[str, Any]) -> bool:
        """Validate Salesforce data format using Pydantic model"""
        try:
            SalesforceContact(**data)
            return True
        except Exception as e:
            Logger.error(f"Salesforce validation error: {e}")
            return False

    def get_supported_contact_types(self) -> list:
        """Get list of contact types supported by this adapter"""
        return self.supported_contact_types
