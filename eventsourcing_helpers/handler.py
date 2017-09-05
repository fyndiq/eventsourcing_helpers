from eventsourcing_helpers import logger
from eventsourcing_helpers.message import from_message_to_dto


class BaseCommandHandler:
    aggregate = None
    aggregate_id_attr = None
    handlers = {}
    repository = None

    def __init__(self, *args, **kwargs):
        # TODO: Send these as parameters, makes the dependency explicit?
        assert self.aggregate
        assert self.aggregate_id_attr
        assert self.handlers
        assert self.repository

    def load_aggregate(self, command):
        """
        Create an empty aggregate and load/apply historical events.
        """
        aggregate_id = getattr(command, self.aggregate_id_attr)
        messages = self.repository.load(aggregate_id)
        historical_events = map(from_message_to_dto, messages)

        aggregate = self.aggregate()
        aggregate.apply_events(historical_events)

        return aggregate

    def handle(self, message):
        """
        Apply correct aggregate method for given command.
        """
        if not message['class'] in self.handlers:
            return

        log = logger.bind(command=message['class'])
        log.info("Handling command")

        command = from_message_to_dto(message)
        command_name = command.__class__.__name__

        aggregate = self.load_aggregate(command)

        try:
            handler = getattr(aggregate, self.handlers[command_name])
        except AttributeError:
            log.error("Missing command handler")
        else:
            handler(command)
            self.repository.save(aggregate)


class BaseEventHandler:
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
