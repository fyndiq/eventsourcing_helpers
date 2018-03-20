from unittest.mock import Mock, patch, MagicMock

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository, import_backend
from eventsourcing_helpers.repository.backends import RepositoryBackend
from eventsourcing_helpers.repository.backends.kafka import KafkaAvroBackend

backend_cls = Mock(spec=RepositoryBackend)

BACKENDS = {'foo': backend_cls}
BACKENDS_PACKAGE = 'eventsourcing_helpers.repository.backends'

command_class, id = 'FooCommand', '1'
message = Mock(value={'class': command_class, 'data': {'id': id}})
events = [1, 2, 3]


class RepositoryTests:

    def setup_method(self):
        self.id, self.events = 1, [1, 2, 3]
        backend_cls.return_value.load.return_value = self.events
        backend_cls.reset_mock()

        self.importer = Mock(return_value=backend_cls)
        self.backend = f'{BACKENDS_PACKAGE}.backend_cls'
        self.config = {
            'backend_config': {'foo': 'bar'},
            'backend': self.backend
        }
        self.aggregate_root = Mock(spec=AggregateRoot)
        self.aggregate_root.id = self.id
        self.aggregate_root._events = self.events

        self.repository = Repository

    def test_init(self):
        """
        Test that we import the correct backend when initializing
        the repository.
        """
        self.repository(self.config, self.importer)
        self.importer.assert_called_once_with(self.backend)

        expected_config = self.config['backend_config']
        self.importer.return_value.assert_called_once_with(expected_config)

    def test_commit(self):
        """
        Test that the backend's commit method is invoked correctly.
        """
        repository = self.repository(self.config, self.importer)
        repository.commit(self.aggregate_root)

        expected_args = {'id': self.id, 'events': self.events}
        backend_cls.return_value.commit.assert_called_once_with(**expected_args)

    def test_load(self):
        """
        Test that the backend's load method is invoked correctly.
        """
        repository = self.repository(self.config, self.importer)
        events = repository.load(self.aggregate_root.id)

        backend_cls.return_value.load.assert_called_once_with(self.id)
        assert events == self.events

    def test_get_events(self):
        message_deserializer = Mock()
        message_deserializer.side_effect = lambda m, **kwargs: m

        config = {
            'return_value.__enter__.return_value': events
        }
        load_mock = MagicMock()
        load_mock.configure_mock(**config)
        repository = self.repository(self.config, self.importer)

        with patch('eventsourcing_helpers.repository.Repository.load', load_mock):  # noqa
            _events = repository._get_events(id, message_deserializer)
            assert events == list(_events)
            load_mock.assert_called_once_with(id)
            assert message_deserializer.call_count == len(events)

    @patch('eventsourcing_helpers.repository.Repository._get_events')
    def test_get_aggregate_root(self, mock_get_events):
        """
        Test that we get the correct aggregate root and that the correct
        methods are invoked.
        """
        repository = self.repository(self.config, self.importer)
        mock_get_events.return_value = events
        aggregate_root_class = Mock
        message_deserializer = Mock()

        aggregate_root = repository.get_aggregate_root(
            id, aggregate_root_class, message_deserializer)

        mock_get_events.assert_called_once_with(id, message_deserializer)
        aggregate_root._apply_events.called_once_with(events)


class ImporterTests:

    def test_import_backend(self):
        """
        Test that we import the correct backend class.
        """
        backend_cls = import_backend(f'{BACKENDS_PACKAGE}.kafka.KafkaAvroBackend')
        assert backend_cls == KafkaAvroBackend
