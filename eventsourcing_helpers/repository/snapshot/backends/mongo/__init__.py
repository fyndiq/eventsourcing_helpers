from pymongo import MongoClient

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend
from eventsourcing_helpers.repository.snapshot.backends.mongo.serializer import (  # noqa
    serialize_data, deserialize_data)


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(
        self, config: dict
    ) -> None:
        assert 'MONGO_URI' in config, 'You must specify MONGO_URI!'
        mongo_uri = config.get('MONGO_URI')
        self.client = MongoClient(mongo_uri)
        self.db = self.client.snapshots

    def save_snapshot(self, aggregate_id: str, pickled_data: str,
                      aggregate_version: str, aggregate_hash: int) -> None:
        data_to_save = serialize_data(
            pickled_data, aggregate_version, aggregate_hash)

        query = {'_id': aggregate_id}
        self.db.snapshots.find_one_and_replace(
            query, data_to_save, upsert=True)

    def get_from_snapshot(self, aggregate_id: str) -> (str, str, int):
        """
        Get the aggregate with the specific aggregate_id from the snapshot
        storage.
        Return a tuple with (aggregate_root, aggregate_version, aggregate_hash)
        """
        query = {'_id': aggregate_id}

        snapshot_data = self.db.snapshots.find_one(query)
        if snapshot_data is None:
            return (None, None, 0)
        else:
            return deserialize_data(snapshot_data)
