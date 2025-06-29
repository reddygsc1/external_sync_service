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
                lifecyclestage=self._map_contact_type_to_lifecycle_stage(
                    internal_contact.contact
                ),
                hs_lead_status=self._map_contact_type_to_lead_status(
                    internal_contact.contact
                ),
                company=internal_contact.company
                or self.extract_company_from_email(internal_contact.email),
                jobtitle=internal_contact.title
                or self._map_contact_type_to_job_title(internal_contact.contact),
                department=internal_contact.department
                or self._map_contact_type_to_department(internal_contact.contact),
                address=internal_contact.address,
                hs_analytics_source="SYSTEM_SYNC",
                hs_analytics_source_data_1=internal_contact.contact,
                createdate=internal_contact.created_at,
                lastmodifieddate=internal_contact.updated_at,
                customer_type=internal_contact.contact,
                account_id=f"ACC_{internal_contact.id}",
                relationship_type=self._map_contact_type_to_relationship_type(
                    internal_contact.contact
                ),
                internal_id=internal_contact.id,
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
                contact=self._map_hubspot_stage_to_contact_type(
                    properties.lifecyclestage, properties.hs_lead_status
                ),
                created_at=properties.createdate,
                updated_at=properties.lastmodifieddate,
                company=properties.company,
                title=properties.jobtitle,
                department=properties.department,
                address=properties.address,
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
            logger.error(f"HubSpot validation error: {e}")
            return False

    def get_supported_contact_types(self) -> list:
        """Get list of contact types supported by this adapter"""
        return self.supported_contact_types

    def _map_contact_type_to_lifecycle_stage(self, contact_type: str) -> str:
        """Map internal contact type to HubSpot lifecycle stage"""
        mapping = {
            "lead": "lead",
            "customer": "customer",
            "prospect": "opportunity",
            "vendor": "customer",
            "partner": "customer",
            "employee": "customer",
        }
        return mapping.get(contact_type, "lead")

    def _map_contact_type_to_lead_status(self, contact_type: str) -> str:
        """Map internal contact type to HubSpot lead status"""
        mapping = {
            "lead": "NEW",
            "customer": "CUSTOMER",
            "prospect": "QUALIFIED",
            "vendor": "CUSTOMER",
            "partner": "CUSTOMER",
            "employee": "CUSTOMER",
        }
        return mapping.get(contact_type, "NEW")

    def _map_contact_type_to_job_title(self, contact_type: str) -> str:
        """Map contact type to job title"""
        mapping = {"customer": "Customer", "partner": "Partner", "vendor": "Vendor"}
        return mapping.get(contact_type, "")

    def _map_contact_type_to_department(self, contact_type: str) -> str:
        """Map contact type to department"""
        mapping = {
            "customer": "Customer Success",
            "partner": "Partnerships",
            "vendor": "Procurement",
        }
        return mapping.get(contact_type, "")

    def _map_contact_type_to_relationship_type(self, contact_type: str) -> str:
        """Map contact type to relationship type"""
        mapping = {"customer": "Customer", "partner": "Partner", "vendor": "Vendor"}
        return mapping.get(contact_type, "")

    def _map_hubspot_stage_to_contact_type(
        self, lifecycle_stage: str, lead_status: str
    ) -> str:
        """Map HubSpot lifecycle stage back to internal contact type"""
        if lead_status == "CUSTOMER":
            # Check for specific customer types in properties
            return "customer"  # Default to customer for now
        elif lifecycle_stage == "lead":
            return "lead"
        elif lifecycle_stage == "opportunity":
            return "prospect"
        else:
            return "lead"
