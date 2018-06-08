from pymongo import MongoClient

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(self, config: dict, mongo_client_class=MongoClient) -> None:

        assert 'MONGO_URI' in config, 'You must specify MONGO_URI!'
        mongo_uri = config.get('MONGO_URI')
        self.client = mongo_client_class(mongo_uri)
        self.db = self.client.snapshots

    def save(self, id: str, data: dict) -> None:
        """
        Saves the data to the snapshot storage

        Args:
            id (str): The id to save the data for
            data (dict): data to be saved
        Returns:
            None
        """
        query = {'_id': id}
        self.db.snapshots.find_one_and_replace(query, data, upsert=True)

    def load(self, id: str) -> dict:
        """
        Get the aggregate with the specific id from the snapshot storage

        Args:
            id (str): The id to retrieve the data for
        Returns:
            dict: The stored snapshot data
        """
        query = {'_id': id}

        snapshot_data = self.db.snapshots.find_one(query)
        return snapshot_data
