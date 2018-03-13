from copy import deepcopy
from unittest.mock import MagicMock, Mock, patch

from eventsourcing_helpers.command_handler import (
    CommandHandler, ESCommandHandler)

module = 'eventsourcing_helpers.command_handler'

command_class, id = 'FooCommand', '1'
message = Mock(value={'class': command_class, 'data': {'id': id}})
events = [1, 2, 3]

command = Mock()
command._class = command_class
command.id = id


class ESCommandHandlerTests:
    def setup_method(self):
        self.aggregate_root = Mock()
        self.aggregate_root.foo_method = Mock()

        config = {
            'return_value.load.return_value.__enter__.return_value': events
        }
        self.repository = MagicMock()
        self.repository.configure_mock(**config)

        command_handler = ESCommandHandler
        command_handler.aggregate_root = self.aggregate_root
        command_handler.handlers = {command_class: 'foo_method'}
        command_handler.repository_config = {'empty_config': None}

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = command

        self.handler = command_handler(
            message_deserializer=self.message_deserializer,
            repository=self.repository
        )

    @patch(f'{module}.ESCommandHandler._commit_staged_events')
    @patch(f'{module}.ESCommandHandler._handle_command')
    @patch(f'{module}.ESCommandHandler._get_aggregate_root')
    @patch(f'{module}.ESCommandHandler._can_handle_command')
    def test_handle(self, mock_can_handle, mock_get, mock_handle, mock_commit):
        """
        Test that the correct methods are invoked when handling a command.
        """
        mock_can_handle.return_value = True
        mock_get.return_value = self.aggregate_root

        self.handler.handle(message)

        self.message_deserializer.assert_called_once_with(message)
        mock_can_handle.assert_called_once_with(message)
        mock_get.assert_called_once_with(command.id)
        mock_handle.assert_called_once_with(
            command, handler_inst=self.aggregate_root
        )
        mock_commit.assert_called_once_with(self.aggregate_root)

    def test_commit_staged_events(self):
        """
        Test that the repository is invoked correctly.
        """
        self.handler._commit_staged_events(self.aggregate_root)
        self.repository.return_value.commit.assert_called_once_with(
            self.aggregate_root
        )

    def test_handle_command_by_str(self):
        """
        Test that the correct command handler is invoked using a str.
        """
        self.handler._handle_command(command, handler_inst=self.aggregate_root)
        self.aggregate_root.foo_method.called_once_with(command)

    def test_handle_command_by_function(self):
        """
        Test that the correct command handler is invoked using a class function.
        """
        self.handler.handlers = {command_class: self.aggregate_root.foo_method}
        self.handler._handle_command(command, handler_inst=self.aggregate_root)
        self.aggregate_root.foo_method.called_once_with(command)

    @patch(f'{module}.ESCommandHandler._get_events')
    def test_get_aggregate_root(self, mock_get_events):
        """
        Test that we get the correct aggregate root and that the correct
        methods are invoked.
        """
        mock_get_events.return_value = events
        aggregate_root = self.handler._get_aggregate_root(command.id)

        self.aggregate_root.return_value == aggregate_root
        mock_get_events.assert_called_once_with(command.id)
        self.aggregate_root._apply_events.called_once_with(events)

    def test_get_events(self):
        """
        Test that we get the correct events and that the correct methods
        are invoked.
        """
        self.message_deserializer.side_effect = lambda m, **kwargs: m
        _events = self.handler._get_events(command.id)

        assert events == list(_events)
        self.repository.return_value.load.assert_called_once_with(command.id)
        assert self.message_deserializer.call_count == len(events)

        self.message_deserializer.side_effect = None


class CommandHandlerTests:
    def setup_method(self):
        self.mock_handler = Mock()

        command_handler = CommandHandler
        command_handler.handlers = {command_class: self.mock_handler}

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = command

        self.handler = command_handler(
            message_deserializer=self.message_deserializer
        )

    @patch(f'{module}.CommandHandler._handle_command')
    @patch(f'{module}.CommandHandler._can_handle_command')
    def test_handle(self, mock_can_handle, mock_handle):
        """
        Test that the correct methods are invoked when handling a command.
        """
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
        _message.value['class'] = 'BarCommand'
        can_handle = self.handler._can_handle_command(_message)
        assert can_handle is False

    def test_handle_command(self):
        """
        Test that the correct command handler is invoked.
        """
        self.handler._handle_command(command)
        self.mock_handler.called_once_with(command)
