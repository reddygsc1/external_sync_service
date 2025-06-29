from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from app.models.salesforce_schema import SalesforceContact
from app.models.hubspot_schema import HubSpotContact
import random
import time
import asyncio

app = FastAPI()

# Simple in-memory rate limiters
salesforce_rate_limit = {"count": 0, "reset": time.time() + 1}
hubspot_rate_limit = {"count": 0, "reset": time.time() + 1}
RATE_LIMIT = 5  # max requests per second per endpoint

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

@app.post("/mock/salesforce/contact")
async def mock_salesforce_contact(contact: SalesforceContact):
    print(f"Salesforce contact received: {contact.Id}")
    now = time.time()
    if now > salesforce_rate_limit["reset"]:
        salesforce_rate_limit["count"] = 0
        salesforce_rate_limit["reset"] = now + 1
    if salesforce_rate_limit["count"] >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Salesforce rate limit exceeded")
    salesforce_rate_limit["count"] += 1
    # Simulate random processing time
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return {"success": True, "external_id": contact.Id, "system": "salesforce"}

@app.post("/mock/hubspot/contact")
async def mock_hubspot_contact(contact: HubSpotContact):
    now = time.time()
    print(f"HubSpot contact received: {contact.id}")
    if now > hubspot_rate_limit["reset"]:
        hubspot_rate_limit["count"] = 0
        hubspot_rate_limit["reset"] = now + 1
    if hubspot_rate_limit["count"] >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="HubSpot rate limit exceeded")
    hubspot_rate_limit["count"] += 1
    # Simulate random processing time
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return {"success": True, "external_id": contact.id, "system": "hubspot"} 