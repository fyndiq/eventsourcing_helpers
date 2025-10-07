import structlog

from eventsourcing_helpers import metrics
from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.tracing import attrs, get_datadog_service_name, tracer
from eventsourcing_helpers.utils import get_callable_representation

from confluent_kafka_helpers.message import Message

logger = structlog.get_logger(__name__)


class EventHandler(Handler):
    """
    Application service that calls the correct domain handler for an event.
    """

    def _find_handler(self, event_class: str) -> tuple:
        """
        Find the handler associated for a given `event_class`.

        The handler key can be either a string or a class.

        Args:
            event_class: The event class name to find a handler for.

        Returns:
            Found handler function and optional message deserializer class.
        """
        if event_class in self.handlers:
            return self.handlers[event_class], None

        for key in self.handlers.keys():
            try:
                if key.__name__ == event_class:
                    return self.handlers[key], key
            except AttributeError:
                pass

        return None, None

    def _can_handle_command(self, message: Message) -> bool:
        """
        Checks if the event is something we can handle.

        Args:
            message: Consumed message from the bus.

        Returns:
            bool: Flag to indicate if we can handle the event.
        """
        event_class = message.value["class"]
        handler, _ = self._find_handler(event_class)

        if handler is None:
            logger.debug("Unhandled event", event_class=event_class)
            return False

        return True

    def handle(self, message: Message) -> None:
        """
        Apply correct handler for received event.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        event_class = message.value["class"]
        logger.info("Handling event", event_class=event_class)

        handler, deserialize_class = self._find_handler(event_class)
        handler_name = get_callable_representation(handler)

        # this is the first span from the applications perspective and will will act as the "service
        # entry" span
        service_name = get_datadog_service_name()
        with tracer.start_span(
            name="eventsourcing_helpers.handle_event",
            service_name=service_name,
            resource_name=handler_name,
            system=None,
        ) as span:
            with tracer.start_span(
                name="eventsourcing_helpers.deserialize_message",
                service_name=service_name,
                system=None,
            ):
                event = self.message_deserializer(message, deserialize_class=deserialize_class)

            span.set_attribute(
                attrs.MESSAGING_OPERATION_TYPE,
                attrs.MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
            )
            handler_wrapper = metrics.timed(
                "eventsourcing_helpers.handler.handle",
                tags=[
                    "message_type:event",
                    f"message_class:{event_class}",
                    f"handler:{handler_name}",
                ],
            )(handler)
            handler_wrapper(event)
