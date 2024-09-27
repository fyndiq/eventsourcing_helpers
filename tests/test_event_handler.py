from unittest.mock import MagicMock, Mock, call, patch

from eventsourcing_helpers.event_handler import EventHandler

module = "eventsourcing_helpers.event_handler"


class EventHandlerTests:
    def setup_method(self):
        self.event_class, self.id = "FooEvent", 1
        self.message = Mock(value={"class": self.event_class, "data": {"id": self.id}})
        self.event = Mock()
        self.event._class = self.event_class
        self.event.id = self.id

        self.message_deserializer = Mock()
        self.message_deserializer.return_value = self.event

        self.event_handler = MagicMock()
        self.event_handler.__name__ = "event_handler"
        self.handler = EventHandler
        self.handler.handlers = {self.event_class: self.event_handler}
        self.handler = self.handler(self.message_deserializer)

    @patch(f"{module}.EventHandler._can_handle_command")
    def test_handle(self, mock_can_handle):
        """
        Test that correct methods are invoked when handling an event.
        """
        self.handler.handle(self.message)

        mock_can_handle.assert_called_once_with(self.message)
        self.event_handler.assert_called_once_with(self.event)

    def test_can_handle_command(self):
        """
        Test that we only handle registered events.
        """
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is True

        self.message.value["class"] = "BarEvent"
        can_handle = self.handler._can_handle_command(self.message)
        assert can_handle is False

    @patch(f"{module}.tracer.start_span")
    @patch(f"{module}.EventHandler._can_handle_command")
    def test_handle_adds_tracing(self, mock_can_handle, mock_start_span):
        mock_start_span.return_value.__enter__.return_value = Mock()

        self.handler.handle(self.message)

        mock_start_span.assert_has_calls(
            [
                call(
                    name="eventsourcing_helpers.handle_event",
                    service_name="unknown_service",
                    resource_name="event_handler",
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
