from eventsourcing_helpers import logger
from eventsourcing_helpers.message import from_message_to_dto


class EventHandler:
    handlers = {}

    def __init__(self, handlers):
        self.handlers = handlers
        assert self.handlers

    def handle(self, message):
        """
        Apply correct method for given event.
        """

        if not message['class'] in self.handlers:
            return

        log = logger.bind(event=message['class'])
        log.info("Handling event")

        event = from_message_to_dto(message)
        event_name = event.__class__.__name__

        try:
            handler = self.handlers[event_name]

        except AttributeError:
            log.error("Missing event handler")
        else:
            handler(event)
