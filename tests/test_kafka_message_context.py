from unittest.mock import Mock, patch

from eventsourcing_helpers.messagebus.backends.kafka import message_handler_context
from eventsourcing_helpers.messagebus.backends.kafka.context import get_message_handler_span_links

from confluent_kafka_helpers.context import get_propagated_headers


class MessageHandlerContextTests:
    @patch("eventsourcing_helpers.messagebus.backends.kafka.context.tracer")
    def test_sets_filtered_headers_and_current_message_links(self, tracer):
        message = Mock()
        message._meta.headers = {"x-request-id": "abc", "other": "ignored"}
        tracer.extract_headers.return_value = "context"
        tracer.extract_links.return_value = ["link"]

        with message_handler_context(message, propagate_header_keys=["x-request-id"]):
            assert get_propagated_headers() == {"x-request-id": "abc"}
            assert get_message_handler_span_links() == ["link"]

        assert get_propagated_headers() == {}
        assert get_message_handler_span_links() is None
        tracer.extract_headers.assert_called_once_with(headers=message._meta.headers)
        tracer.extract_links.assert_called_once_with(context="context")
