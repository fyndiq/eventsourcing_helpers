from copy import deepcopy
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from eventsourcing_helpers.command_handler import CommandHandler, ESCommandHandler

module = "eventsourcing_helpers.command_handler"

command_class, id = "FooCommand", "1"
message = Mock(value={"class": command_class, "data": {"id": id}})
events = [1, 2, 3]

command = Mock()
command._class = command_class
command.id = id


class ESCommandHandlerTests:
    def setup_method(self):
        self.aggregate_root = Mock()
        self.aggregate_root.foo_method = Mock()

        config = {"return_value.load.return_value.__enter__.return_value": events}
        self.repository = MagicMock()
        self.repository.configure_mock(**config)

        command_handler = ESCommandHandler
        command_handler.aggregate_root = self.aggregate_root
        command_handler.handlers = {command_class: self.aggregate_root.foo_method}
        command_handler.repository_config = {"empty_config": None}

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = command

        self.handler = command_handler(
            message_deserializer=self.message_deserializer, repository=self.repository
        )

    @patch(f"{module}.ESCommandHandler._commit_staged_events")
    @patch(f"{module}.ESCommandHandler._handle_command")
    @patch(f"{module}.ESCommandHandler._get_aggregate_root")
    @patch(f"{module}.ESCommandHandler._can_handle_command")
    @patch(f"{module}.statsd.timed")
    def test_handle(
        self, mock_metrics_timed, mock_can_handle, mock_get, mock_handle, mock_commit
    ):
        """
        Test that the correct methods are invoked when handling a command.
        """
        mock_metrics_timed.return_value.__enter__.return_value = Mock

        mock_can_handle.return_value = True
        mock_get.return_value = self.aggregate_root

        self.handler.handle(message)

        self.message_deserializer.assert_called_once_with(message)
        mock_can_handle.assert_called_once_with(message)
        mock_get.assert_called_once_with(command.id)
        mock_handle.assert_called_once_with(command, handler_inst=self.aggregate_root)
        mock_commit.assert_called_once_with(self.aggregate_root)

    @patch(f"{module}.ESCommandHandler._commit_staged_events")
    @patch(f"{module}.ESCommandHandler._handle_command")
    @patch(f"{module}.ESCommandHandler._get_aggregate_root")
    @patch(f"{module}.ESCommandHandler._can_handle_command")
    @patch(f"{module}.statsd.timed")
    def test_handle_deletes_snapshot_on_error(
        self, mock_metrics_timed, mock_can_handle, mock_get, mock_handle, mock_commit
    ):
        mock_metrics_timed.return_value.__enter__.return_value = Mock

        mock_can_handle.return_value = True
        mock_get.return_value = self.aggregate_root
        mock_handle.side_effect = TypeError

        with pytest.raises(TypeError):
            self.handler.handle(message)

        self.handler.repository.snapshot.delete.assert_called_once_with(
            self.aggregate_root
        )

    def test_commit_staged_events(self):
        """
        Test that the repository is invoked correctly.
        """
        self.handler._commit_staged_events(self.aggregate_root)
        self.repository.return_value.commit.assert_called_once_with(self.aggregate_root)

    def test_handle_command_by_str(self):
        """
        Test that the correct command handler is invoked using a str.
        """
        self.handler._handle_command(command, handler_inst=self.aggregate_root)
        self.aggregate_root.foo_method.assert_called_once_with(
            self.aggregate_root, command
        )

    def test_handle_command_by_function(self):
        """
        Test that the correct command handler is invoked using a class function.
        """
        self.handler.handlers = {command_class: self.aggregate_root.foo_method}
        self.handler._handle_command(command, handler_inst=self.aggregate_root)
        self.aggregate_root.foo_method.assert_called_once_with(
            self.aggregate_root, command
        )


class CommandHandlerTests:
    def setup_method(self):
        self.mock_handler = MagicMock()
        self.mock_handler.__name__ = "command_handler"

        command_handler = CommandHandler
        command_handler.handlers = {command_class: self.mock_handler}

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = command

        self.handler = command_handler(message_deserializer=self.message_deserializer)

    @patch(f"{module}.CommandHandler._handle_command")
    @patch(f"{module}.CommandHandler._can_handle_command")
    @patch(f"{module}.statsd.timed")
    def test_handle(self, mock_metrics_timed, mock_can_handle, mock_handle):
        """
        Test that the correct methods are invoked when handling a command.
        """
        mock_metrics_timed.return_value.__enter__.return_value = Mock

        mock_can_handle.return_value = True
        self.handler.handle(message)

        self.message_deserializer.assert_called_once_with(message)
        mock_can_handle.assert_called_once_with(message)
        mock_handle.assert_called_once_with(command)

    def test_can_handle_command(self):
        """
        Test that we only handle registered commands.
        """
        can_handle = self.handler._can_handle_command(message)
        assert can_handle is True

        _message = deepcopy(message)
        _message.value["class"] = "BarCommand"
        can_handle = self.handler._can_handle_command(_message)
        assert can_handle is False

    def test_handle_command(self):
        """
        Test that the correct command handler is invoked.
        """
        self.handler._handle_command(command)
        self.mock_handler.assert_called_once_with(command)

    @patch(f"{module}.tracer.start_span")
    @patch(f"{module}.CommandHandler._can_handle_command")
    def test_handle_adds_tracing(self, mock_can_handle, mock_start_span):
        mock_start_span.return_value.__enter__.return_value = Mock()

        self.handler.handle(message)

        mock_start_span.assert_has_calls(
            [
                call(
                    name="eventsourcing_helpers.handle_command",
                    service_name="unknown_service",
                    resource_name="command_handler",
                    system=None,
                ),
                call().__enter__(),
                call(
                    name="eventsourcing_helpers.deserialize_message",
                    service_name="unknown_service",
                    system=None,
                ),
                call().__enter__(),
                call().__exit__(None, None, None),
                call().__enter__().set_attribute("messaging.operation.type", "process"),
                call().__exit__(None, None, None),
            ]
        )
