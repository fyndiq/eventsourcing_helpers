from unittest.mock import Mock

from eventsourcing_helpers.consumer import Consumer


class ConsumerTests:
    def test_consume_batches_delegates_to_messagebus(self):
        messagebus = Mock()
        handler = Mock()

        Consumer(messagebus=messagebus, handler=handler).consume_batches()

        messagebus.consume_batches.assert_called_once_with(handler=handler.handle)
