from unittest.mock import MagicMock

import pytest

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


@pytest.fixture(scope='function')
def snapshot_backend_mock():
    def snapshot_backend(return_value, **kwargs):
        mock = MagicMock(spec=SnapshotBackend, **kwargs)
        attrs = {
            'return_value.load.return_value': return_value,
        }
        mock.configure_mock(**attrs)
        return mock

    return snapshot_backend
