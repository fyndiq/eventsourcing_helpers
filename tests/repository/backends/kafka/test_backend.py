from functools import partial
from unittest.mock import Mock, MagicMock, patch

from eventsourcing_helpers.repository.backends.kafka import KafkaAvroBackend


events = [1, 2, 3]


class KafkaBackendTests:

    def setup_method(self):
        self.loader = Mock()
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {'producer': {'foo': 'bar'}, 'loader': {'foo': 'bar'}}
        self.get_producer_config = Mock(return_value=self.config['producer'])
        self.get_loader_config = Mock(return_value=self.config['loader'])
        self.id, self.events = 1, [1, 2, 3]
        self.backend = partial(
            KafkaAvroBackend, self.config, self.producer, self.loader,
            self.value_serializer, self.get_producer_config,
            self.get_loader_config
        )

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        self.backend()

        self.loader.assert_called_once_with(self.config['loader'])
        self.producer.assert_called_once_with({
            'foo': 'bar'
        }, value_serializer=self.value_serializer)

    def test_commit(self):
        """
        Test that the produce method are invoked correctly.
        """
        backend = self.backend()
        backend.commit(self.id, self.events)

        expected = [({'key': self.id, 'value': e}, ) for e in self.events]
        assert self.producer.return_value.produce.call_args_list == expected
        assert self.producer.return_value.produce.call_count == len(self.events)

    def test_load(self):
        """
        Test that the load method are invoked correctly.
        """
        backend = self.backend()
        backend.load(self.id)
        self.loader.return_value.load.assert_called_once_with(self.id)

    def test_get_events(self):
        backend = self.backend()
        message_deserializer = Mock()
        message_deserializer.side_effect = lambda m, **kwargs: m

        config = {
            'return_value.__enter__.return_value': events
        }
        load_mock = MagicMock()
        load_mock.configure_mock(**config)

        with patch('eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend.load', load_mock):  # noqa
            _events = backend.get_events(id, message_deserializer)
            assert events == list(_events)
            load_mock.assert_called_once_with(id)
            assert message_deserializer.call_count == len(events)
