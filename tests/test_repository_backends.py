from functools import partial
from unittest.mock import Mock

from eventsourcing_helpers.repository.backends.kafka import KafkaAvroBackend


class KafkaBackendTests:

    def setup_method(self):
        self.consumer = Mock()
        self.consumer.is_auto_commit = True
        self.loader = Mock()
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {'producer': {'foo': 'bar'}, 'loader': {'foo': 'bar'}}
        self.guid, self.events = 1, [1, 2, 3]
        self.backend = partial(
            KafkaAvroBackend, self.config, self.consumer, self.producer,
            self.loader, self.value_serializer
        )

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        self.backend()

        self.loader.assert_called_once_with({'foo': 'bar'})
        self.producer.assert_called_once_with({
            'foo': 'bar'
        }, value_serializer=self.value_serializer)

    def test_commit(self):
        """
        Test that the produce method are invoked correctly.
        """
        backend = self.backend()
        backend.commit(self.guid, self.events)

        expected = [({'key': self.guid, 'value': e}, ) for e in self.events]
        assert self.producer.return_value.produce.call_args_list == expected
        assert self.producer.return_value.produce.call_count == len(self.events)
        assert self.consumer.commit.called is False

    def test_commit_consumer_offset(self):
        """
        Test that the consumer offset is committed if
        auto commit is disabled.
        """
        self.consumer.is_auto_commit = False

        backend = self.backend()
        backend.commit(self.guid, self.events)
        self.consumer.commit.assert_called_once()

    def test_load(self):
        """
        Test that the load method are invoked correctly.
        """
        backend = self.backend()
        backend.load(self.guid)
        self.loader.return_value.load.assert_called_once_with(self.guid)
