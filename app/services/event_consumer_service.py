import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from abc import ABC, abstractmethod
from .schema_transformer import SchemaTransformerFactory
from app.models.internal_schema import InternalContact, InternalContactEvent

logger = logging.getLogger(__name__)


class EventConsumerService(ABC):
    """Abstract base class for event consumer services"""

    @abstractmethod
    async def consume_event(self, event: Dict[str, Any]) -> bool:
        """Consume a single event"""
        pass

    @abstractmethod
    async def consume_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Consume a batch of events"""
        pass


class InternalEventConsumer(EventConsumerService):
    """Internal event consumer service that processes contact events with schema transformation"""

    def __init__(self, transformer_config: Optional[Dict[str, Any]] = None):
        self.processed_events = []
        self.event_counters = {"create": 0, "update": 0, "delete": 0}
        self.contact_type_counters = {
            "lead": 0,
            "customer": 0,
            "prospect": 0,
            "vendor": 0,
            "partner": 0,
            "employee": 0,
        }
        self.external_system_counters = {"salesforce": 0, "hubspot": 0}
        self.transformation_errors = 0
        self.is_processing = False

        # Initialize schema transformer with configuration
        self.schema_transformer = SchemaTransformerFactory.create_with_config(
            transformer_config or {}
        )

    async def consume_event(self, event: Dict[str, Any]) -> bool:
        """Process a single contact event with schema transformation"""
        try:
            self.is_processing = True

            # Skipping non contact events
            if event.get("record") != "contacts":
                logger.info(f"Skipping non-contact event: {event.get('record')}")
                return True  # Return True to indicate "successfully ignored"

            # Validate event structure
            if not self._validate_event(event):
                logger.error(f"Invalid event structure: {event}")
                return False

            # Extract event details
            operation = event.get("operation")
            item = event.get("item", {})
            contact_type = item.get("contact")

            # Update counters
            if operation in self.event_counters:
                self.event_counters[operation] += 1

            if contact_type in self.contact_type_counters:
                self.contact_type_counters[contact_type] += 1

            # Transform schema for external system
            try:
                external_data = self.schema_transformer.transform_contact(item)
                external_system = external_data["_metadata"]["external_system"]

                # Update external system counter
                if external_system in self.external_system_counters:
                    self.external_system_counters[external_system] += 1

                logger.info(
                    f"Transformed contact {item.get('id')} to {external_system} format"
                )

            except Exception as e:
                logger.error(
                    f"Schema transformation error for contact {item.get('id')}: {e}"
                )
                self.transformation_errors += 1
                external_data = None

            # Store processed event with transformation result
            processed_event = {
                "received_at": datetime.now().isoformat(),
                "event": event,
                "external_data": external_data,
                "processed": True,
                "transformation_success": external_data is not None,
            }
            self.processed_events.append(processed_event)

            # Simulate processing time
            await asyncio.sleep(0.01)  # 10ms processing time

            logger.info(
                f"Processed {operation} event for contact {item.get('id')} ({contact_type})"
            )
            return True

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            return False
        finally:
            self.is_processing = False

    async def route_event(self, event: Dict[str, Any]) -> bool:
        """Route events based on record type - only process contacts"""
        record_type = event.get("record")

        if record_type == "contacts":
            return await self.consume_event(event)
        else:
            logger.info(f"Ignoring {record_type} event - only processing contacts")
            return True  # Successfully ignored

    async def consume_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Process a batch of events"""
        try:
            results = []
            for event in events:
                result = await self.route_event(event)
                results.append(result)

            success_count = sum(results)
            total_count = len(events)

            logger.info(
                f"Batch processed: {success_count}/{total_count} events successful"
            )
            return success_count == total_count

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return False

    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate event structure"""
        required_fields = ["record", "operation", "item"]

        for field in required_fields:
            if field not in event:
                return False

        if event["record"] != "contacts":
            return False

        if event["operation"] not in ["create", "update", "delete"]:
            return False

        item = event["item"]
        required_item_fields = ["id", "name", "email", "phone", "contact"]

        for field in required_item_fields:
            if field not in item:
                return False

        return True
