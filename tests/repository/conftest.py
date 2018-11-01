from unittest.mock import MagicMock

import pytest

from eventsourcing_helpers.repository import Snapshot
from eventsourcing_helpers.repository.backends import RepositoryBackend


@pytest.fixture(scope='function')
def snapshot_mock():
    def snapshot(return_value, **kwargs):
        mock = MagicMock(spec=Snapshot, **kwargs)
        attrs = {'return_value.load.return_value': return_value}
        mock.configure_mock(**attrs)
        return mock

    return snapshot


@pytest.fixture(scope='function')
def repository_backend_mock():
    def repository_backend(return_value, **kwargs):
        mock = MagicMock(spec=RepositoryBackend, **kwargs)
        attrs = {
            'return_value.load.return_value.__enter__.return_value': return_value,
            'return_value.get_events.return_value': return_value
        }
        mock.configure_mock(**attrs)
        return mock

    return repository_backend
