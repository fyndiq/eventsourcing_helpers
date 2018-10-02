from functools import partial
from unittest.mock import MagicMock, Mock

from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend


class AvroConsumerMock:
    def __init__(self, config):
        message = Mock()
        message._meta = Mock()
        message._meta.offset = 1
        self.messages = iter([message, message])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.messages)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        pass

    is_auto_commit = False
    commit = Mock()


class KafkaBackendTests:

    def setup_method(self):
        self.consumer = AvroConsumerMock
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {'producer': {'foo': 'bar'}, 'consumer': {'group.id': 'consumer.group.1'}}
        self.get_producer_config = Mock(return_value=self.config['producer'])
        self.get_consumer_config = Mock(return_value=self.config['consumer'])
        self.id, self.event = 1, {'event': 'value'}
        self.backend = partial(
            KafkaAvroBackend, self.config, self.producer, self.consumer,
            self.value_serializer, self.get_producer_config, self.get_consumer_config
        )

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        self.backend()

        self.producer.assert_called_once_with(self.config['producer'], value_serializer=self.value_serializer)

    def test_consume(self):
        backend = self.backend()
        handler = Mock()
        backend.consume(handler=handler)
        assert handler.call_count == 1
