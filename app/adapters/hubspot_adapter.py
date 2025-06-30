from logging import Logger
from typing import Dict, Any, Union
from app.adapters.base_adapter import BaseAdapter
from app.models.internal_schema import InternalContact
from app.models.hubspot_schema import HubSpotContact, HubSpotContactProperties


class HubSpotAdapter(BaseAdapter):

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.supported_contact_types = ["customer", "partner", "vendor"]

    def transform_to_external(
        self, internal_data: Union[InternalContact, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Transform internal contact data to HubSpot format using Pydantic models"""
        try:
            # Validate and get InternalContact instance
            internal_contact = self._validate_internal_data(internal_data)

            # Create HubSpot contact properties using Pydantic model
            hubspot_properties = HubSpotContactProperties(
                firstname=self.extract_first_name(internal_contact.name),
                lastname=self.extract_last_name(internal_contact.name),
                email=internal_contact.email,
                phone=internal_contact.phone,
                createdate=internal_contact.created_at,
                lastmodifieddate=internal_contact.updated_at,
            )

            # Create HubSpot contact using Pydantic model
            hubspot_contact = HubSpotContact(
                id=internal_contact.id, properties=hubspot_properties
            )

            # Convert to dict and add metadata
            hubspot_data = hubspot_contact.model_dump(exclude_none=True)
            hubspot_data = self.add_metadata(hubspot_data, internal_contact)

            self.log_transformation(internal_contact.id, "to external")
            return hubspot_data

        except Exception as e:
            self.log_error(e, "transforming to external")
            raise

    def transform_from_external(self, external_data: Dict[str, Any]) -> InternalContact:
        """Transform HubSpot data to internal format using Pydantic models"""
        try:
            # Validate external data using Pydantic model
            hubspot_contact = HubSpotContact(**external_data)
            properties = hubspot_contact.properties

            # Create InternalContact using Pydantic model
            internal_contact = InternalContact(
                id=hubspot_contact.id,
                name=f"{properties.firstname} {properties.lastname}".strip(),
                email=properties.email,
                phone=properties.phone,
                contact="customer",  # Default since we don't have lifecycle stage in schema
                created_at=properties.createdate,
                updated_at=properties.lastmodifieddate,
            )

            self.log_transformation(hubspot_contact.id, "from external")
            return internal_contact

        except Exception as e:
            self.log_error(e, "transforming from external")
            raise

    def validate_external_data(self, data: Dict[str, Any]) -> bool:
        """Validate HubSpot data format using Pydantic model"""
        try:
            HubSpotContact(**data)
            return True
        except Exception as e:
            Logger.error(f"HubSpot validation error: {e}")
            return False

    def get_supported_contact_types(self) -> list:
        """Get list of contact types supported by this adapter"""
        return self.supported_contact_types


