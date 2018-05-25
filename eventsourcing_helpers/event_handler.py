from typing import Any

import structlog

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers import metrics
from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.utils import get_callable_representation

logger = structlog.get_logger(__name__)


class EventHandler(Handler):
    """
    Application service that calls the correct domain handler for an event.
    """

    def _can_handle_command(self, message: Message) -> bool:
        """
        Checks if the event is something we can handle.

        Args:
            message: Consumed message from the bus.

        Returns:
            bool: Flag to indicate if we can handle the event.
        """
        event_class = message.value['class']
        if event_class not in self.handlers:
            logger.debug("Unhandled event", event_class=event_class)
            return False

        return True

    def _handle_event(self, event: Any) -> None:
        """
        Get and call the correct event handler.

        Args:
            event: Event to be handled.
        """
        handler = self.handlers[event._class]

        handler_name = get_callable_representation(handler)
        handler_wrapper = metrics.timed(
            'eventsourcing_helpers.handler.handle',
            tags=[
                'message_type:event',
                f'message_class:{event._class}',
                f'handler:{handler_name}'
            ]
        )(handler)

        handler_wrapper(event)

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
