import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ContactType(Enum):
    LEAD = "lead"
    CUSTOMER = "customer"
    PROSPECT = "prospect"
    VENDOR = "vendor"
    PARTNER = "partner"
    EMPLOYEE = "employee"


class OperationType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class SystemSettings:
    """Configurable system settings for the mock stream generator"""

    events_per_second: int = 1
    batch_size: int = 10
    enable_async: bool = True
    contact_type_distribution: Dict[str, float] = None
    operation_distribution: Dict[str, float] = None

    def __post_init__(self):
        if self.contact_type_distribution is None:
            self.contact_type_distribution = {
                "lead": 0.4,
                "customer": 0.25,
                "prospect": 0.2,
                "vendor": 0.1,
                "partner": 0.03,
                "employee": 0.02,
            }

        if self.operation_distribution is None:
            self.operation_distribution = {
                "create": 0.6,
                "update": 0.35,
                "delete": 0.05,
            }


class MockStreamGenerator:
    """Generates realistic contact events similar to Kafka stream"""

    def __init__(self, settings: SystemSettings, event_consumer_service=None):
        self.settings = settings
        self.event_consumer_service = event_consumer_service
        self.is_running = False
        self.generated_ids = set()
        self.contact_records = {}  # Store existing records for updates/deletes

        # Realistic data pools
        self.first_names = [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Henry",
            "Ivy",
            "Jack",
            "Kate",
            "Liam",
            "Mia",
            "Noah",
            "Olivia",
            "Paul",
            "Quinn",
            "Ruby",
            "Sam",
            "Tara",
            "Uma",
            "Victor",
            "Wendy",
            "Xavier",
            "Yara",
            "Zoe",
            "Alex",
            "Jordan",
            "Taylor",
            "Casey",
            "Morgan",
            "Riley",
        ]

        self.last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
            "Taylor",
            "Moore",
            "Jackson",
            "Martin",
            "Lee",
            "Perez",
            "Thompson",
            "White",
            "Harris",
            "Sanchez",
            "Clark",
            "Ramirez",
            "Lewis",
            "Robinson",
            "Walker",
            "Young",
            "Allen",
            "King",
        ]

        self.email_domains = [
            "gmail.com",
            "outlook.com",
        ]

        self.phone_formats = [
            "+1-{}-{}-{}",  # US format
            "+91-{}-{}-{}",  # India format
        ]

    def generate_realistic_contact(self) -> Dict[str, Any]:
        """Generate a realistic contact record"""
        first_name = random.choice(self.first_names)
        last_name = random.choice(self.last_names)
        name = f"{first_name} {last_name}"

        # Generate realistic email
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(self.email_domains)}"

        # Generate realistic phone number
        area_code = f"{random.randint(100, 999)}"
        prefix = f"{random.randint(100, 999)}"
        line_number = f"{random.randint(1000, 9999)}"
        phone_format = random.choice(self.phone_formats)
        phone = phone_format.format(area_code, prefix, line_number)

        # Generate unique ID
        contact_id = (
            f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1000, 9999)}"
        )
        while contact_id in self.generated_ids:
            contact_id = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1000, 9999)}"
        self.generated_ids.add(contact_id)

        # Select contact type based on distribution
        contact_type = random.choices(
            list(self.settings.contact_type_distribution.keys()),
            weights=list(self.settings.contact_type_distribution.values()),
        )[0]

        return {
            "id": contact_id,
            "name": name,
            "email": email,
            "phone": phone,
            "contact": contact_type,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

    def generate_event(self) -> Dict[str, Any]:
        """Generate a single event with realistic operation and data"""
        # Select operation based on distribution
        operation = random.choices(
            list(self.settings.operation_distribution.keys()),
            weights=list(self.settings.operation_distribution.values()),
        )[0]

        if operation == OperationType.CREATE.value:
            contact_data = self.generate_realistic_contact()
            self.contact_records[contact_data["id"]] = contact_data
            item = contact_data
        elif operation == OperationType.UPDATE.value:
            if not self.contact_records:
                # If no existing records, create one first
                contact_data = self.generate_realistic_contact()
                self.contact_records[contact_data["id"]] = contact_data
                item = contact_data
            else:
                # Update existing record
                existing_id = random.choice(list(self.contact_records.keys()))
                existing_record = self.contact_records[existing_id].copy()

                # Randomly update some fields
                update_fields = random.sample(
                    ["name", "email", "phone"], random.randint(1, 3)
                )

                if "name" in update_fields:
                    first_name = random.choice(self.first_names)
                    last_name = random.choice(self.last_names)
                    existing_record["name"] = f"{first_name} {last_name}"

                if "email" in update_fields:
                    first_name = existing_record["name"].split()[0].lower()
                    last_name = existing_record["name"].split()[1].lower()
                    existing_record["email"] = (
                        f"{first_name}.{last_name}@{random.choice(self.email_domains)}"
                    )

                if "phone" in update_fields:
                    area_code = f"{random.randint(100, 999)}"
                    prefix = f"{random.randint(100, 999)}"
                    line_number = f"{random.randint(1000, 9999)}"
                    phone_format = random.choice(self.phone_formats)
                    existing_record["phone"] = phone_format.format(
                        area_code, prefix, line_number
                    )

                existing_record["updated_at"] = datetime.now().isoformat()
                self.contact_records[existing_id] = existing_record
                item = existing_record
        else:  # DELETE
            if not self.contact_records:
                # If no existing records, create one first
                contact_data = self.generate_realistic_contact()
                self.contact_records[contact_data["id"]] = contact_data
                item = contact_data
            else:
                # Delete existing record
                existing_id = random.choice(list(self.contact_records.keys()))
                item = self.contact_records.pop(existing_id)

        return {
            "record": "contacts",
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "item": item,
        }

    async def send_to_consumer(self, event: Dict[str, Any]):
        """Send event to the internal event consumer service"""
        try:
            if self.event_consumer_service:
                await self.event_consumer_service.consume_event(event)
            else:
                # Fallback: just log the event
                logger.info(f"Generated event: {json.dumps(event, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to send event to consumer: {e}")

    async def generate_batch(self):
        """Generate and send a batch of events"""
        batch = []
        for _ in range(self.settings.batch_size):
            event = self.generate_event()
            batch.append(event)

        # Send batch to consumer
        for event in batch:
            await self.send_to_consumer(event)

        logger.info(f"Generated and sent batch of {len(batch)} events")
        return batch

    async def start_streaming(self):
        """Start the continuous event streaming"""
        self.is_running = True
        logger.info(
            f"Starting mock stream generator with {self.settings.events_per_second} events/sec"
        )

        try:
            while self.is_running:
                if self.settings.enable_async:
                    # Generate events asynchronously
                    await self.generate_batch()
                    # Calculate delay based on events per second
                    delay = self.settings.batch_size / self.settings.events_per_second
                    await asyncio.sleep(delay)
                else:
                    # Generate events synchronously
                    self.generate_batch()
                    time.sleep(1 / self.settings.events_per_second)

        except KeyboardInterrupt:
            logger.info("Stopping mock stream generator...")
        except Exception as e:
            logger.error(f"Error in stream generator: {e}")
        finally:
            self.is_running = False

    def stop_streaming(self):
        """Stop the event streaming"""
        self.is_running = False
        logger.info("Mock stream generator stopped")
