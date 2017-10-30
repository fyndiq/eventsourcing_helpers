from typing import Any

import structlog

from eventsourcing_helpers.serializers import from_message_to_dto

logger = structlog.get_logger(__name__)


class EventHandler:
    """
    Application service that calls the correct domain handler for an event.
    """
    handlers: dict = {}

    def __init__(self, message_deserializer=from_message_to_dto) -> None:
        assert self.handlers

        self.message_deserializer = message_deserializer

    def _can_handle_command(self, message: dict) -> bool:
        """
        Checks if the event is something we can handle.

        Args:
            message: Consumed message from the bus.

        Returns:
            bool: Flag to indicate if we can handle the event.
        """
        if not message['class'] in self.handlers:
            logger.debug("Unhandled event", event_class=message['class'])
            return False

        return True

    def _handle_event(self, event: Any) -> None:
        """
        Get and call the correct event handler.

        Args:
            event: Event to be handled.
        """
        handler = self.handlers[event._class]
        handler(event)

    def handle(self, message: dict) -> None:
        """
        Apply correct handler for received event.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        event = self.message_deserializer(message)
        logger.info("Handling event", event_class=event._class)

        self._handle_event(event)
