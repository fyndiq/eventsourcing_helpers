from eventsourcing_helpers import logger
from eventsourcing_helpers.serializers import from_message_to_dto


class CommandHandler:

    aggregate_root = None
    handlers = {}
    repository = None

    def __init__(self):
        assert self.aggregate_root
        assert self.handlers
        assert self.repository

    def _load_aggregate(self, command):
        """
        Create an empty aggregate and load/apply historical events.
        """
        aggregate_id = command.guid
        messages = self.repository.load(aggregate_id)
        historical_events = map(from_message_to_dto, messages)

        aggregate = self.aggregate_root()
        aggregate.apply_events(historical_events)

        return aggregate

    def handle(self, message):
        """
        Apply correct aggregate method for given command.
        """
        if not message['class'] in self.handlers:
            logger.debug("Unhandled command", command=message['class'])
            return

        log = logger.bind(command=message['class'])
        log.info("Handling command")

        command = from_message_to_dto(message)
        command_name = command.__class__.__name__

        aggregate = self._load_aggregate(command)

        handler = getattr(aggregate, self.handlers[command_name])
        handler(command)
        self.repository.save(aggregate)
