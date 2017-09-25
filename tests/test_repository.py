from mock import Mock, patch

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository, import_backend
from eventsourcing_helpers.repository.backends.base import RepositoryBackend
from eventsourcing_helpers.repository.backends.kafka import KafkaBackend

backend_cls = Mock(spec=RepositoryBackend)
BACKENDS = {'foo': backend_cls}


@patch('eventsourcing_helpers.repository.BACKENDS', BACKENDS)
class RepositoryTests:

    def setup_method(self):
        self.guid, self.events = 1, [1, 2, 3]
        backend_cls.return_value.load.return_value = self.events
        backend_cls.reset_mock()

        self.importer = Mock(return_value=backend_cls)
        self.backend = 'foo'
        self.config = {'foo': 'bar', 'backend': self.backend}

        self.aggregate_root = Mock(spec=AggregateRoot)
        self.aggregate_root.guid = self.guid
        self.aggregate_root._events = self.events

        self.repository = Repository

    def test_init(self):
        """
        Test that we import the correct backend when initializing
        the repository.
        """
        self.repository(self.config, self.importer)
        self.importer.assert_called_once_with(BACKENDS[self.backend])
        self.importer.return_value.assert_called_once_with(self.config)

    def test_commit(self):
        """
        Test that the backend's commit method is invoked correctly.
        """
        repository = self.repository(self.config, self.importer)
        repository.commit(self.aggregate_root)

        expected_args = {'guid': self.guid, 'events': self.events}
        backend_cls.return_value.commit.assert_called_once_with(**expected_args)

    def test_load(self):
        """
        Test that the backend's load method is invoked correctly.
        """
        repository = self.repository(self.config, self.importer)
        events = repository.load(self.aggregate_root.guid)

        backend_cls.return_value.load.assert_called_once_with(self.guid)
        assert events == self.events


class ImporterTests:

    def test_import_backend(self):
        """
        Test that we import the correct backend class.
        """
        backend_cls = import_backend('kafka.KafkaBackend')
        assert backend_cls == KafkaBackend
