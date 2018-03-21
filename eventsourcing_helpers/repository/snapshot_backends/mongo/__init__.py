from typing import Callable

import jsonpickle

from pymongo import MongoClient

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot_backends import SnapshotBackend
from eventsourcing_helpers.repository.snapshot_backends.mongo.serializer import snapshot_serializer  # noqa


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(
        self, config: dict,
        encoder: Callable = jsonpickle.encode,
        decoder: Callable = jsonpickle.decode,
    ) -> None:

        self.encoder = encoder
        self.decoder = decoder

        assert 'MONGO_URI' in config, 'You must specify MONGO_URI!'

        # Get Mongo URI etc

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        encoded_aggregate = self.encoder.encode(aggregate)
        data_to_save = snapshot_serializer(
            encoded_aggregate, aggregate.version, aggregate._hash)
        pass

    def _get_from_snapshot(self, aggregate_id: str, aggregate: AggregateRoot) -> AggregateRoot:  # noqa
        """
        Get the aggregate with the specific aggregate_id from the snapshot
        storage. Return it if it has the same saved hash as the current hash
        of the aggregate. Otherwise return None
        """
        return None
