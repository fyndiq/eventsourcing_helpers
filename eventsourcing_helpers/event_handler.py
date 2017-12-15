from typing import Any

from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.log import get_logger

logger = get_logger(__name__)


class EventHandler(Handler):
    """
    Application service that calls the correct domain handler for an event.
    """

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
