from unittest.mock import Mock

from eventsourcing_helpers.repository.backends.kafka import KafkaAvroBackend


class KafkaBackendTests:

    def setup_method(self):
        self.loader = Mock()
        self.producer = Mock()
        self.config = {'producer': None, 'loader': None}
        self.guid, self.events = 1, [1, 2, 3]

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        KafkaAvroBackend(self.config, loader=self.loader, producer=self.producer)
        self.loader.assert_called_once_with(self.config['loader'])
        self.producer.assert_called_once_with(self.config['producer'])

    def test_commit(self):
        """
        Test that the produce method are invoked correctly.
        """
        backend = KafkaAvroBackend(
            self.config, loader=self.loader, producer=self.producer
        )
        backend.commit(self.guid, self.events)

        expected = [({'key': self.guid, 'value': e}, ) for e in self.events]
        assert self.producer.return_value.produce.call_args_list == expected
        assert self.producer.return_value.produce.call_count == len(self.events)

    def test_load(self):
        """
        Test that the load method are invoked correctly.
        """
        backend = KafkaAvroBackend(
            self.config, loader=self.loader, producer=self.producer
        )
        backend.load(self.guid)
        self.loader.return_value.load.assert_called_once_with(self.guid)
