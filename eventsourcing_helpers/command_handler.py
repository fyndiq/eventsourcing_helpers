from typing import Any, Callable, Generator

import structlog

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers import metrics
from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository
from eventsourcing_helpers.serializers import from_message_to_dto
from eventsourcing_helpers.utils import get_callable_representation

logger = structlog.get_logger(__name__)


class CommandHandler(Handler):
    """
    Command handler.

    Handles a command by calling the correct function or method in a handler
    class.
    """

    def _can_handle_command(self, message: Message) -> bool:
        """
        Checks if the command is something we can handle.

        Args:
            message: Consumed message from the bus.

        Returns:
            bool: Flag to indicate if we can handle the command.
        """
        command_class = message.value['class']
        if command_class not in self.handlers:
            logger.debug("Unhandled command", command_class=command_class)
            return False

        return True

    def _handle_command(self, command: Any, handler_inst: Any = None) -> None:
        """
        Get and call the correct command handler.

        The handler can either be a callable or a name of a method in the
        handler instance.

        Args:
            command: Command to be handled.
            handler_inst: Optional handler instance - probably an instance
                of the aggregate root.
        """
        command_class = command._class
        handler = self.handlers[command_class]

        handler_name = get_callable_representation(handler)
        handler_wrapper = metrics.timed(
            'eventsourcing_helpers.handler.handle',
            tags=[
                'message_type:command',
                f'message_class:{command_class}',
                f'handler:{handler_name}'
            ]
        )(handler)

        logger.info("Calling command handler", command_class=command_class)
        if handler_inst:
            handler_wrapper(handler_inst, command)
        else:
            handler_wrapper(command)

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

    def _get_events(self, id: str) -> Generator[Any, None, None]:
        """
        Get all aggregate events from the repository.

        Args:
            id: Aggregate root id.

        Returns:
            list: List with all events.
        """
        with self.repository.load(id) as events:
            for event in events:
                yield self.message_deserializer(event, is_new=False)

    def _get_aggregate_root(self, id: str) -> AggregateRoot:
        """
        Get latest state of the aggregate root.

        Args:
            id: ID of the aggregate root.

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        aggregate_root = self.aggregate_root()
        aggregate_root._apply_events(self._get_events(id))

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

        aggregate_root = self._get_aggregate_root(command.id)
        self._handle_command(command, handler_inst=aggregate_root)
        self._commit_staged_events(aggregate_root)
