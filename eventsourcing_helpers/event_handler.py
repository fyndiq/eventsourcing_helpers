from eventsourcing_helpers import logger
from eventsourcing_helpers.serializers import from_message_to_dto


class EventHandler:
    handlers = {}

    def __init__(self):
        assert self.handlers

    def handle(self, message):
        """
        Apply correct method for given event.
        """
        if not message['class'] in self.handlers:
            return

        event_class = message['class']
        log = logger.bind(event_class=event_class)
        log.info("Handling event")

        event = from_message_to_dto(message)
        try:
            handler = self.handlers[event_class]
        except AttributeError:
            log.error("Missing event handler")
        else:
            handler(event)
