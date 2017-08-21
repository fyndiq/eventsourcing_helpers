from eventsourcing_helpers import logger
from eventsourcing_helpers.message import from_message_to_dto


class BaseCommandHandler:

    aggregate = None
    aggregate_id_attr = None

    handlers = {}

    def __init__(self, repository, *args, **kwargs):
        self._repository = repository(*args, **kwargs)

    def key_filter(self, key, message_key):
        """
        Only load messages if the condition is true.

        Default we are only interested in keys that belong
        to the same aggregate.
        """
        return key == message_key

    def load_aggregate(self, command):
        """
        Create an empty aggregate and load/apply historical events.
        """
        aggregate_id = getattr(command, self.aggregate_id_attr)
        messages = self._repository.load(aggregate_id, self.key_filter)
        historical_events = map(from_message_to_dto, messages)

        aggregate = self.aggregate()
        aggregate.apply_events(historical_events)

        return aggregate

    def handle(self, message):
        """
        Route a command to the correct handler.
        """
        if not message['class'] in self.handlers:
            return

        log = logger.bind(command=message['class'])
        log.info("Routing command")

        command = from_message_to_dto(message)
        command_name = command.__class__.__name__

        aggregate = self.load_aggregate(command)

        try:
            handler = getattr(aggregate, self.handlers[command_name])
        except AttributeError:
            log.error("Missing command handler")
        else:
            handler(command)
            self._repository.save(aggregate)
