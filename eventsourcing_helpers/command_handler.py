from typing import Any, Callable, List

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository
from eventsourcing_helpers.serializers import from_message_to_dto

logger = structlog.get_logger()


class CommandHandler:
    """
    A domain service that turns a command into events by calling methods on an
    aggregate root, publishes the resulting events, and handles transactions
    and persistence.
    """
    aggregate_root: AggregateRoot = None
    handlers: dict = {}
    repository_config: dict = None

    def __init__(self, message_deserializer: Callable=from_message_to_dto,
                 repository: Any=Repository) -> None:
        assert self.aggregate_root
        assert self.handlers
        assert self.repository_config

        self.message_deserializer = message_deserializer
        self.repository = repository(self.repository_config)

    def _can_handle_command(self, message: dict) -> bool:
        """
        Checks if the command is something we can handle.

        Args:
            message: Consumed message from the bus.

        Returns:
            bool: Flag to indicate if we can handle the command.
        """
        if not message['class'] in self.handlers:
            logger.debug("Unhandled command", command_class=message['class'])
            return False

        return True

    def _get_events(self, guid: str) -> List[Any]:
        """
        Get all events for an aggregate root from the repository.

        Args:
            guid: Get all events with this guid.

        Returns:
            list: List with all events.
        """
        events = self.repository.load(guid)
        events = list(map(self.message_deserializer, events))

        return events

    def _get_aggregate_root(self, guid: str) -> AggregateRoot:
        """
        Get latest state of the aggregate root or return an
        empty instance.

        Args:
            guid: Guid of the aggregate root.

        Returns:
            AggregateRoot: An aggregate root with the latest state.
        """
        events = self._get_events(guid)
        aggregate_root = self.aggregate_root()
        aggregate_root.apply_events(events)

        return aggregate_root

    def _handle_command(self, aggregate_root: AggregateRoot,
                        command: Any) -> None:
        """
        Get and call the correct command handler.

        Args:
            aggregate_root: Entity which implements the command handlers.
            command: Command to be handled.
        """
        command_class = command.__class__.__name__
        handler_name = self.handlers[command_class]
        logger.info("Calling command handler", command_class=command_class,
                    handler_name=handler_name,
                    aggregate_root=aggregate_root.name)

        handler = getattr(aggregate_root, handler_name)
        handler(command)

    def _commit_staged_events(self, aggregate_root: AggregateRoot) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Entity with all staged events.
        """
        self.repository.commit(aggregate_root)

    def handle(self, message: dict) -> None:
        """
        Apply correct handler for received command.

        After the command has been handled the staged
        events are committed to the repository.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        command = self.message_deserializer(message)
        command_class = command.__class__.__name__
        logger.info("Handling command", command_class=command_class)

        aggregate_root = self._get_aggregate_root(command.guid)
        self._handle_command(aggregate_root, command)
        self._commit_staged_events(aggregate_root)
