from unittest.mock import Mock

import pytest
from mongomock import MongoClient

from eventsourcing_helpers.repository.snapshot.backends.mongo import (
    MongoSnapshotBackend
)


class MongoSnapshotBackendTests():
    config = {'host': 'mongodb://localhost:27017'}

    @pytest.fixture(autouse=True)
    def setup_method(
        self, aggregate_root_cls_mock, snapshot_backend_mock, importer_mock
    ):
        self.backend = MongoSnapshotBackend(self.config, MongoClient)

    def test_mongo_save_saves_data(self):
        id = 'a'
        data = {'b': 1}

        self.backend.save(id=id, data=data)

        stored_data = self.backend.client.snapshots.snapshots.find_one(
            {
                '_id': id
            }
        )
        expected_data = {'_id': id}
        expected_data.update(data)
        assert stored_data == expected_data

    def test_mongo_load_loads_correct_data(self):
        id = 'a'
        query = {'_id': id}
        data = {'b': 1}

        self.backend.client.snapshots.snapshots.find_one_and_replace(
            query, data, upsert=True
        )
        stored_data = self.backend.load(id)

        expected_data = {'_id': id}
        expected_data.update(data)
        assert stored_data == expected_data

    @pytest.mark.parametrize(
        'config, expected_call', [
            (
                {
                    'host': 'a'
                }, {
                    'host': 'a',
                    'connectTimeoutMS': 2000,
                    'serverSelectionTimeoutMS': 1000,
                    'waitQueueTimeoutMS': 1000,
                    'socketTimeoutMS': 1000
                }
            ), (
                {
                    'host': 'a',
                    'connectTimeoutMS': 5000
                }, {
                    'host': 'a',
                    'connectTimeoutMS': 5000,
                    'serverSelectionTimeoutMS': 1000,
                    'waitQueueTimeoutMS': 1000,
                    'socketTimeoutMS': 1000
                }
            )
        ]
    )
    def test_mongo_init_uses_default_config(self, config, expected_call):
        mongo_client_mock = Mock()
        self.backend = MongoSnapshotBackend(config, mongo_client_mock)

        mongo_client_mock.assert_called_once_with(**expected_call)
