from unittest.mock import Mock, patch

from eventsourcing_helpers.command_handler import CommandHandler

module = 'eventsourcing_helpers.command_handler'


class CommandHandlerTests:

    def setup_method(self):
        command_class, guid = 'FooCommand', '1'

        self.message = {'class': command_class, 'data': {'guid': guid}}
        self.events = [1, 2, 3]
        self.aggregate_root = Mock()

        self.command = Mock()
        self.command._class = command_class
        self.command.guid = guid

        config = {'return_value.load.return_value': self.events}
        self.repository = Mock()
        self.repository.configure_mock(**config)

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = self.command

        handler = CommandHandler
        handler.aggregate_root = self.aggregate_root
        handler.handlers = {command_class: 'foo_method'}
        handler.repository_config = {'empty_config': None}

        self.handler = handler(
            message_deserializer=self.message_deserializer,
            repository=self.repository
        )

    @patch(f'{module}.CommandHandler._commit_staged_events')
    @patch(f'{module}.CommandHandler._handle_command')
    @patch(f'{module}.CommandHandler._get_aggregate_root')
    @patch(f'{module}.CommandHandler._can_handle_command')
    def test_handle(self, mock_can_handle, mock_get, mock_handle, mock_commit):
        """
        Test that correct methods are invoked when handling a command.
        """
        mock_can_handle.return_value = True
        mock_get.return_value = self.aggregate_root

        self.handler.handle(self.message)

        self.message_deserializer.assert_called_once_with(self.message)
        mock_can_handle.assert_called_once_with(self.message)
        mock_get.assert_called_once_with(self.command.guid)
        mock_handle.assert_called_once_with(
            self.aggregate_root, self.command
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

    def test_handle_command(self):
        """
        Test that the correct command handler is invoked.
        """
        self.handler._handle_command(
            self.aggregate_root, self.command
        )
        self.aggregate_root.foo_method.called_once_with(self.command)

    @patch(f'{module}.CommandHandler._get_events')
    def test_get_aggregate_root(self, mock_get_events):
        """
        Test that we get the correct aggregate root and that the correct
        methods are invoked.
        """
        mock_get_events.return_value = self.events
        aggregate_root = self.handler._get_aggregate_root(self.command.guid)

        self.aggregate_root.return_value == aggregate_root
        mock_get_events.assert_called_once_with(self.command.guid)
        self.aggregate_root.apply_events.called_once_with(self.events)

    def test_get_events(self):
        """
        Test that we get the correct events and that the correct methods
        are invoked.
        """
        self.message_deserializer.side_effect = lambda m: m
        events = self.handler._get_events(self.command.guid)

        assert self.events == events
        self.repository.return_value.load.assert_called_once_with(
            self.command.guid
        )
        assert self.message_deserializer.call_count == len(self.events)

    def test_can_handle_command(self):
        """
        Test that we only handle registered commands.
        """
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is True

        self.message['class'] = 'BarCommand'
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is False
