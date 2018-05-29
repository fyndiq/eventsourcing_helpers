from unittest.mock import MagicMock

import pytest

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


@pytest.fixture(scope='function')
def snapshot_backend_mock():
    def snapshot_backend(**kwargs):
        mock = MagicMock(spec=SnapshotBackend, **kwargs)
        attrs = {}
        mock.configure_mock(**attrs)
        return mock

    return snapshot_backend
