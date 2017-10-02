from functools import partial
from unittest.mock import Mock

from eventsourcing_helpers.repository.backends.kafka import KafkaAvroBackend


class KafkaBackendTests:

    def setup_method(self):
        self.loader = Mock()
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {'backend_config': {'producer': None, 'loader': None}}
        self.guid, self.events = 1, [1, 2, 3]
        self.backend = partial(
            KafkaAvroBackend, self.config, self.producer, self.loader,
            self.value_serializer
        )

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        self.backend()

        expected_config = self.config['backend_config']['loader']
        self.loader.assert_called_once_with(expected_config)

        expected_config = self.config['backend_config']['producer']
        self.producer.assert_called_once_with(
            expected_config, value_serializer=self.value_serializer
        )

    def test_commit(self):
        """
        Test that the produce method are invoked correctly.
        """
        backend = self.backend()
        backend.commit(self.guid, self.events)

        expected = [({'key': self.guid, 'value': e}, ) for e in self.events]
        assert self.producer.return_value.produce.call_args_list == expected
        assert self.producer.return_value.produce.call_count == len(self.events)

    def test_load(self):
        """
        Test that the load method are invoked correctly.
        """
        backend = self.backend()
        backend.load(self.guid)
        self.loader.return_value.load.assert_called_once_with(self.guid)
