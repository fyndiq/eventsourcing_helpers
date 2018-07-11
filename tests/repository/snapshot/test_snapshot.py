from functools import partial
from unittest.mock import Mock

import pytest

from eventsourcing_helpers.repository.snapshot import Snapshot


class SnapshotTests:
    config = {'backend_config': {}}
    snapshot_data_mock = 'data'

    @pytest.fixture(autouse=True)
    def setup_method(
        self, aggregate_root_cls_mock, snapshot_backend_mock,
        importer_mock
    ):
        self.aggregate_root_cls = aggregate_root_cls_mock()
        self.backend = snapshot_backend_mock(
            return_value=self.snapshot_data_mock
        )
        self.importer = importer_mock(return_value=self.backend)
        self.deserializer = Mock()
        self.serializer = Mock()
        self.hash_function = Mock()

        self.snapshot = partial(
            Snapshot, config=self.config,
            importer=self.importer,
            serializer=self.serializer,
            deserializer=self.deserializer,
            hash_function=self.hash_function
        )

    def test_save(self):
        snapshot = self.snapshot()
        aggregate_root = self.aggregate_root_cls()
        aggregate_root.id = 1
        snapshot.save(aggregate_root)

        self.serializer.call_count == 1
        self.hash_function.call_count == 1
        self.backend.save.call_count == 1

    def test_load(self):
        snapshot = self.snapshot()
        aggregate_root = self.aggregate_root_cls()
        test_id = 1
        snapshot.load(test_id, aggregate_root)
        test_hash = snapshot.hash_function(
            aggregate_root.get_representation())

        self.backend().load.assert_called_once_with(test_id)
        self.deserializer.assert_called_once_with(
            self.snapshot_data_mock, test_hash)

    def test_delete(self):
        snapshot = self.snapshot()
        aggregate_root = self.aggregate_root_cls()
        aggregate_root.id = 1
        snapshot.delete(aggregate_root)

        self.backend().delete.assert_called_once_with(aggregate_root.id)
