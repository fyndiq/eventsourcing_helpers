import pytest
from mongomock import MongoClient

from eventsourcing_helpers.repository.snapshot.backends.mongo import (
    MongoSnapshotBackend
)


class MongoSnapshotBackendTests():
    config = {'MONGO_URI': 'mongodb://localhost:27017'}

    @pytest.fixture(autouse=True)
    def setup_method(
        self, aggregate_root_cls_mock, snapshot_backend_mock,
        importer_mock
    ):
        self.mongo_snapshot_backend = MongoSnapshotBackend(
            self.config,
            MongoClient
        )

    def test_mongo_save_saves_data(self):
        id = 'a'
        data = {'b': 1}
        import ipdb
        ipdb.set_trace()

        self.mongo_snapshot_backend.save(
            id=id, data=data
        )

        stored_data = self.mongo_snapshot_backend.client.snapshots.snapshots.find_one({'_id': id})
        expected_data = {'_id': id}
        expected_data.update(data)
        assert stored_data == expected_data
