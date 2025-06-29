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
                LeadSource=self._map_contact_type_to_lead_source(
                    internal_contact.contact
                ),
                Status=self._map_contact_type_to_status(internal_contact.contact),
                Type=self._map_contact_type_to_type(internal_contact.contact),
                CreatedDate=internal_contact.created_at,
                LastModifiedDate=internal_contact.updated_at,
                Description=f"Contact synced from internal system - Type: {internal_contact.contact}",
                Company=internal_contact.company
                or self.extract_company_from_email(internal_contact.email),
                Title=internal_contact.title
                or self._map_contact_type_to_title(internal_contact.contact),
                Department=internal_contact.department
                or self._map_contact_type_to_department(internal_contact.contact),
                Contact_Type__c=internal_contact.contact,
                Internal_ID__c=internal_contact.id,
            )

            # Add custom fields based on contact type
            if internal_contact.contact in ["customer", "partner", "vendor"]:
                salesforce_contact.AccountId = f"ACC_{internal_contact.id}"

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
            internal_contact = InternalContact(
                id=salesforce_contact.Id,
                name=f"{salesforce_contact.FirstName} {salesforce_contact.LastName}".strip(),
                email=salesforce_contact.Email,
                phone=salesforce_contact.Phone,
                contact=self._map_salesforce_type_to_contact_type(
                    salesforce_contact.Type, salesforce_contact.LeadSource
                ),
                created_at=salesforce_contact.CreatedDate,
                updated_at=salesforce_contact.LastModifiedDate,
                company=salesforce_contact.Company,
                title=salesforce_contact.Title,
                department=salesforce_contact.Department,
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
            logger.error(f"Salesforce validation error: {e}")
            return False

    def get_supported_contact_types(self) -> list:
        """Get list of contact types supported by this adapter"""
        return self.supported_contact_types

    def _map_contact_type_to_lead_source(self, contact_type: str) -> str:
        """Map internal contact type to Salesforce LeadSource"""
        mapping = {
            "lead": "Website",
            "customer": "Customer Portal",
            "prospect": "Marketing Campaign",
            "vendor": "Partner Referral",
            "partner": "Partner Program",
            "employee": "Internal",
        }
        return mapping.get(contact_type, "Other")

    def _map_contact_type_to_status(self, contact_type: str) -> str:
        """Map internal contact type to Salesforce Status"""
        mapping = {
            "lead": "New",
            "customer": "Active",
            "prospect": "Qualified",
            "vendor": "Active",
            "partner": "Active",
            "employee": "Active",
        }
        return mapping.get(contact_type, "New")

    def _map_contact_type_to_type(self, contact_type: str) -> str:
        """Map internal contact type to Salesforce Type"""
        mapping = {
            "lead": "Lead",
            "customer": "Customer",
            "prospect": "Prospect",
            "vendor": "Vendor",
            "partner": "Partner",
            "employee": "Employee",
        }
        return mapping.get(contact_type, "Lead")

    def _map_contact_type_to_title(self, contact_type: str) -> str:
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

    def _map_salesforce_type_to_contact_type(
        self, sf_type: str, lead_source: str
    ) -> str:
        """Map Salesforce type back to internal contact type"""
        type_mapping = {
            "Lead": "lead",
            "Customer": "customer",
            "Prospect": "prospect",
            "Vendor": "vendor",
            "Partner": "partner",
            "Employee": "employee",
        }
        return type_mapping.get(sf_type, "lead")
