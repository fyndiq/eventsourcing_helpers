from pymongo import MongoClient

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(
        self, config: dict
    ) -> None:
        assert 'MONGO_URI' in config, 'You must specify MONGO_URI!'
        mongo_uri = config.get('MONGO_URI')
        self.client = MongoClient(mongo_uri)
        self.db = self.client.snapshots

    def save_snapshot(self, aggregate_id: str, data: dict) -> None:
        query = {'_id': aggregate_id}
        self.db.snapshots.find_one_and_replace(
            query, data, upsert=True)

    def get_from_snapshot(self, aggregate_id: str) -> (str, str, int):
        """
        Get the aggregate with the specific aggregate_id from the snapshot
        storage.
        Return a tuple with (aggregate_root, aggregate_version, aggregate_hash)
        """
        query = {'_id': aggregate_id}

        snapshot_data = self.db.snapshots.find_one(query)
        return snapshot_data
