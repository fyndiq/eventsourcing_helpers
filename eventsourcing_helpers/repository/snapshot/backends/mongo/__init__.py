from typing import Dict, Union

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend

DEFAULT_CONFIG = {
    'connectTimeoutMS': 2000,
    'serverSelectionTimeoutMS': 1000,
    'waitQueueTimeoutMS': 1000,
    'socketTimeoutMS': 1000,
}


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(self, config: dict, mongo_client_class=MongoClient) -> None:
        assert 'host' in config, 'You must specify host!'
        mongo_config = {**DEFAULT_CONFIG, **config}
        self.client = mongo_client_class(**mongo_config)
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

    def load(self, id: str) -> Union[Dict, None]:
        """
        Get the aggregate with the specific id from the snapshot storage

        Args:
            id (str): The id to retrieve the data for
        Returns:
            dict: The stored snapshot data
        """
        query = {'_id': id}
        try:
            snapshot_data = self.db.snapshots.find_one(query)
            return snapshot_data
        except PyMongoError:
            return None

    def delete(self, id: str) -> None:
        """
        Deletes the data of the snapshot with specified id.

        Args:
            id (str): The id to save the data for
        Returns:
            None
        """
        query = {'_id': id}
        self.db.snapshots.delete_many(query)
