from typing import Any, Union

import structlog

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.metrics import statsd
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository
from eventsourcing_helpers.tracing import attrs, get_datadog_service_name, tracer
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
        command_class = message.value["class"]
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

        logger.info("Calling command handler", command_class=command_class)
        if handler_inst:
            handler(handler_inst, command)
        else:
            handler(command)

    def handle(self, message: Message) -> None:
        """
        Apply correct handler for the received command.

        Args:
            message: Consumed message from the bus.
        """
        if not self._can_handle_command(message):
            return

        command_class = message.value["class"]
        logger.info("Handling command", command_class=command_class)
        handler = self.handlers[command_class]
        handler_name = get_callable_representation(handler)

        # this is the first span from the applications perspective and will will act as the "service
        # entry" span
        service_name = get_datadog_service_name()
        with tracer.start_span(
            name="eventsourcing_helpers.handle_command",
            service_name=service_name,
            resource_name=handler_name,
            system=None,
        ) as span:
            with tracer.start_span(
                name="eventsourcing_helpers.deserialize_message",
                service_name=service_name,
                system=None,
            ):
                command = self.message_deserializer(message)

            span.set_attribute(
                attrs.MESSAGING_OPERATION_TYPE,
                attrs.MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
            )
            with statsd.timed(
                "eventsourcing_helpers.handler.handle",
                tags=[
                    "message_type:command",
                    f"message_class:{command_class}",
                    f"handler:{handler_name}",
                ],
            ):
                self._handle_command(command)


class ESCommandHandler(CommandHandler):
    """
    Event sourced command handler.

    Unlike the command handler we expect an aggregate root to handle all the
    commands.

    The resulting staged events are published to a message bus and persisted in
    an event store using a repository.
    """

    aggregate_root: Union[AggregateRoot, None] = None
    repository_config: Union[dict, None] = None

    def __init__(self, *args, repository: Any = Repository, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        assert self.aggregate_root
        assert self.repository_config

        self.repository = repository(
            self.repository_config, self.aggregate_root, **kwargs
        )

    def _get_aggregate_root(self, id: str) -> AggregateRoot:
        """
        Get latest state of the aggregate root.

        Args:
            id: ID of the aggregate root.

        Returns:
            AggregateRoot: Aggregate root instance with the latest state.
        """
        return self.repository.load(id)

    def _commit_staged_events(self, aggregate_root: AggregateRoot) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Aggregate root with staged events.
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

        command_class = command._class
        handler = self.handlers[command_class]
        handler_name = get_callable_representation(handler)
        with statsd.timed(
            "eventsourcing_helpers.handler.handle",
            tags=[
                "message_type:command",
                f"message_class:{command_class}",
                f"handler:{handler_name}",
            ],
        ):
            aggregate_root = self._get_aggregate_root(command.id)
            try:
                self._handle_command(command, handler_inst=aggregate_root)
            except Exception as e:
                self.repository.snapshot.delete(aggregate_root)
                statsd.increment(  # type: ignore
                    "eventsourcing_helpers.snapshot.cache.delete",
                    tags=[f"id={aggregate_root.id}"],
                )
                raise e
            self._commit_staged_events(aggregate_root)
