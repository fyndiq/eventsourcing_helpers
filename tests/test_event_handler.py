from unittest.mock import Mock, patch

from eventsourcing_helpers.event_handler import EventHandler

module = 'eventsourcing_helpers.event_handler'


class EventHandlerTests:

    def setup_method(self):
        self.event_class, self.guid = 'FooEvent', 1
        self.message = {'class': self.event_class, 'data': {'guid': self.guid}}

        self.event = Mock()
        self.event.__class__.__name__ = self.event_class
        self.event.guid = self.guid

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = self.event

        self.event_handler = Mock()
        self.handler = EventHandler
        self.handler.handlers = {self.event_class: self.event_handler}
        self.handler = self.handler(self.message_deserializer)

    @patch(f'{module}.EventHandler._handle_event')
    @patch(f'{module}.EventHandler._can_handle_command')
    def test_handle(self, mock_can_handle, mock_handle):
        """
        Test that correct methods are invoked when handling an event.
        """
        self.handler.handle(self.message)

        mock_can_handle.assert_called_once_with(self.message)
        mock_handle.assert_called_once_with(self.event)

    def test_handle_event(self):
        """
        Test that the correct event handler is invoked.
        """
        self.handler._handle_event(self.event)
        self.event_handler.called_once_with(self.event)

    def test_can_handle_command(self):
        """
        Test that we only handle registered events.
        """
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is True

        self.message['class'] = 'BarEvent'
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is False
