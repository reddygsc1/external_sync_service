import asyncio
import random
from app.utils.external_config import ExternalSystem
import httpx
import json
from typing import Dict, Any
from datetime import datetime
from pydantic import BaseModel

MOCK_API_BASE = "http://localhost:8000/mock"


class APIDispatcherService:
    def __init__(
        self,
        max_retries: int = 5,
        base_delay: float = 0.5,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.endpoints = {
            ExternalSystem.SALESFORCE: f"{MOCK_API_BASE}/salesforce/contact",
            ExternalSystem.HUBSPOT: f"{MOCK_API_BASE}/hubspot/contact",
        }

    async def dispatch_transformed_contact(
        self, transformed_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Dispatch already-transformed contact data to external API"""
        if "_metadata" not in transformed_data:
            raise ValueError("Transformed data must contain _metadata with external_system")
        
        external_system = transformed_data["_metadata"]["external_system"]
        url = self.endpoints[ExternalSystem(external_system)]

        # Remove metadata for API call
        api_data = {k: v for k, v in transformed_data.items() if k != "_metadata"}
        return await self._post_with_retries(url, api_data)

    async def _post_with_retries(
        self, url: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        attempt = 0
        while attempt <= self.max_retries:
            try:
                # Serialize data to JSON, handling datetime
                json_payload = self._to_json(data)
                headers = {"Content-Type": "application/json"}
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.post(url, content=json_payload, headers=headers)
                    if resp.status_code == 200:
                        return {
                            "success": True,
                            "external_system": url.split("/")[-2],
                            "response": resp.json(),
                        }
                    elif resp.status_code == 429:
                        # Rate limit hit, retry with jitter
                        delay = self._jitter_delay(attempt)
                        print(
                            f"429 Rate limit hit for {url}. Retrying in {delay:.2f}s (attempt {attempt+1})..."
                        )
                        await asyncio.sleep(delay)
                        attempt += 1
                    else:
                        print(f"Error {resp.status_code} from {url}: {resp.text}")
                        return {
                            "success": False,
                            "error": resp.text,
                            "status_code": resp.status_code,
                        }
            except Exception as e:
                print(f"Exception posting to {url}: {e}")
                delay = self._jitter_delay(attempt)
                await asyncio.sleep(delay)
                attempt += 1
        return {"success": False, "error": "Max retries exceeded", "status_code": 429}

    def _jitter_delay(self, attempt: int) -> float:
        # Exponential backoff with jitter
        base = self.base_delay * (2**attempt)
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
