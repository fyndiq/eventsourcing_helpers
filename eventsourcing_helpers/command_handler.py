from typing import Any, Callable, List

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository
from eventsourcing_helpers.serializers import from_message_to_dto

logger = structlog.get_logger(__name__)


class CommandHandler:
    """
    Command handler.

    Handles a command by calling the correct function or method in a handler
    class.
    """
    handlers: dict = {}

    def __init__(self, message_deserializer: Callable=
                 from_message_to_dto) -> None:  # yapf: disable
        assert self.handlers
        self.message_deserializer = message_deserializer

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

    def _handle_command(self, command: Any, handler_class: Any=None) -> None:
        """
        Get and call the correct command handler.

        The handler can either be a callable or a name of a method in the
        handler class.

        Args:
            command: Command to be handled.
            handler_class: Optional class with handler methods.
        """
        command_class = command._class
        handler = self.handlers[command_class]
        logger.info("Calling command handler", command_class=command_class)
        if not callable(handler):
            handler = getattr(handler_class, handler)
        handler(command)

    def handle(self, message: dict) -> None:
        """
        Apply correct handler for the received command.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        command = self.message_deserializer(message)
        logger.info("Handling command", command_class=command._class)
        self._handle_command(command)


class ESCommandHandler(CommandHandler):
    """
    Event sourced command handler.

    Unlike the command handler we expect an aggregate root to handle all the
    commands.

    The resulting staged events are published to a message bus and persisted in
    an event store using a repository.
    """
    aggregate_root: AggregateRoot = None
    repository_config: dict = None

    def __init__(self, message_deserializer: Callable=from_message_to_dto,
                 repository: Any=Repository, **kwargs) -> None:  # yapf: disable
        super().__init__(message_deserializer)
        assert self.aggregate_root
        assert self.repository_config

        self.repository = repository(self.repository_config, **kwargs)

    def _get_events(self, guid: str) -> List[Any]:
        """
        Get all events for an aggregate root from the repository.

        Args:
            guid: Get all events for this guid.

        Returns:
            list: List with all events.
        """
        events = self.repository.load(guid)
        events = list(map(self.message_deserializer, events))

        return events

    def _get_aggregate_root(self, guid: str) -> AggregateRoot:
        """
        Get latest state of the aggregate root or return an empty instance.

        Args:
            guid: Guid of the aggregate root.

        Returns:
            AggregateRoot: An aggregate root with the latest state.
        """
        events = self._get_events(guid)
        aggregate_root = self.aggregate_root()
        aggregate_root.apply_events(events)

        return aggregate_root

    def _commit_staged_events(self, aggregate_root: AggregateRoot) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Entity with all staged events.
        """
        self.repository.commit(aggregate_root)

    def handle(self, message: dict) -> None:
        """
        Apply correct handler for the received command.

        After the command has been handled the staged events are committed to
        the repository.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        command = self.message_deserializer(message)
        logger.info("Handling command", command_class=command._class)

        aggregate_root = self._get_aggregate_root(command.guid)
        self._handle_command(command, handler_class=aggregate_root)
        self._commit_staged_events(aggregate_root)
