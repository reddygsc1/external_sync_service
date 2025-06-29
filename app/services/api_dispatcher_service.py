import asyncio
import random
import httpx
import json
from typing import Dict, Any, Union
from datetime import datetime
from app.services.schema_transformer import SchemaTransformer, ExternalSystem
from app.models.internal_schema import InternalContact
from pydantic import BaseModel

MOCK_API_BASE = "http://localhost:8000/mock"

class APIDispatcherService:
    def __init__(self, schema_transformer: SchemaTransformer, max_retries: int = 5, base_delay: float = 0.5):
        self.schema_transformer = schema_transformer
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.endpoints = {
            ExternalSystem.SALESFORCE: f"{MOCK_API_BASE}/salesforce/contact",
            ExternalSystem.HUBSPOT: f"{MOCK_API_BASE}/hubspot/contact"
        }

    async def dispatch_contact(self, contact: Union[InternalContact, Dict[str, Any]]) -> Dict[str, Any]:
        """Dispatch contact to external API"""
        # If it's already transformed data (has _metadata), send directly
        if isinstance(contact, dict) and "_metadata" in contact:
            external_system = contact["_metadata"]["external_system"]
            url = self.endpoints[ExternalSystem(external_system)]
            
            # Remove metadata for API call
            api_data = {k: v for k, v in contact.items() if k != "_metadata"}
            return await self._post_with_retries(url, api_data)
        
        # If it's raw contact data, transform first
        external_data = self.schema_transformer.transform_contact(contact)
        contact_type = external_data["_metadata"]["contact_type"]
        external_system = self.schema_transformer.get_external_system(contact_type)
        url = self.endpoints[external_system]
        
        # Remove metadata for API call
        api_data = {k: v for k, v in external_data.items() if k != "_metadata"}
        return await self._post_with_retries(url, api_data)

    async def _post_with_retries(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        attempt = 0
        while attempt <= self.max_retries:
            try:
                # Serialize data to JSON, handling datetime
                json_payload = self._to_json(data)
                headers = {"Content-Type": "application/json"}
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.post(url, content=json_payload, headers=headers)
                    if resp.status_code == 200:
                        return {"success": True, "external_system": url.split("/")[-2], "response": resp.json()}
                    elif resp.status_code == 429:
                        # Rate limit hit, retry with jitter
                        delay = self._jitter_delay(attempt)
                        print(f"429 Rate limit hit for {url}. Retrying in {delay:.2f}s (attempt {attempt+1})...")
                        await asyncio.sleep(delay)
                        attempt += 1
                    else:
                        print(f"Error {resp.status_code} from {url}: {resp.text}")
                        return {"success": False, "error": resp.text, "status_code": resp.status_code}
            except Exception as e:
                print(f"Exception posting to {url}: {e}")
                delay = self._jitter_delay(attempt)
                await asyncio.sleep(delay)
                attempt += 1
        return {"success": False, "error": "Max retries exceeded", "status_code": 429}

    def _jitter_delay(self, attempt: int) -> float:
        # Exponential backoff with jitter
        base = self.base_delay * (2 ** attempt)
        return random.uniform(base / 2, base * 1.5)

    def _to_json(self, data: Any) -> str:
        if isinstance(data, BaseModel):
            return data.model_dump_json(exclude_none=True)
        def default(o):
            if isinstance(o, datetime):
                return o.isoformat()
            if isinstance(o, BaseModel):
                return o.model_dump(exclude_none=True)
            raise TypeError(f"Object of type {type(o)} is not JSON serializable")
        return json.dumps(data, default=default) 