from typing import Callable

import structlog

import jsonpickle

from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot.config import get_snapshot_config  # noqa
from eventsourcing_helpers.repository.snapshot.serializer import (
    serialize_data, deserialize_data)

BACKENDS = {
    'null': 'eventsourcing_helpers.repository.snapshot.backends.null.NullSnapshotBackend',   # noqa
}

logger = structlog.get_logger(__name__)


class Snapshot:
    """
    Interface to communicate with a snapshot backend.

    The interface provides methods for saving and loading a snapshot
    """
    DEFAULT_BACKEND = 'null'

    def __init__(self, config: dict,
                 importer: Callable=import_backend,
                 encode_method=jsonpickle.encode,
                 decode_method=jsonpickle.decode,
                 **kwargs) -> None:  # yapf: disable
        snapshot_config = get_snapshot_config(config)
        snapshot_backend_path = snapshot_config.get(
            'backend', BACKENDS[self.DEFAULT_BACKEND])
        snapshot_backend_config = snapshot_config.get('backend_config', '')

        logger.info("Using snapshot backend", backend=snapshot_backend_path,
                    config=snapshot_backend_config)
        snapshot_backend_class = importer(snapshot_backend_path)
        self.snapshot_backend = snapshot_backend_class(
            snapshot_backend_config, **kwargs)

        self.decode_method = decode_method
        self.encode_method = encode_method

    def save_aggregate_as_snapshot(self, aggregate: AggregateRoot) -> None:
        pickled_data = self.encode_method(aggregate)
        data_to_save = serialize_data(
            pickled_data, aggregate._version, aggregate.get_schema_hash())

        self.snapshot_backend.save_snapshot(aggregate.id, data_to_save)

    def load_aggregate_from_snapshot(self, aggregate_id: str,
                                     current_aggregate_hash: int,
                                     ) -> AggregateRoot:
        """
        Loads an aggregate from the snapshot storage.
        If the saved aggregate_hash in the database does not match the
        current_aggregate_hash it means that the model has changed since the
        snapshot was saved. If that is the case the snapshot should not be used
        and None should be returned

        Args:
            aggregate_id (str): The id of the aggregate we wish to load
            current_aggregate_hash (int): The hash of the latest model of the
                                          aggregate we are trying to load

        Returns:
            AggregateRoot: The aggregate that was loaded (or None)
        """

        snapshot_data = self.snapshot_backend.get_from_snapshot(aggregate_id)
        (pickled_data, _, snapshot_aggregate_hash) = deserialize_data(
            snapshot_data)

        if pickled_data and current_aggregate_hash == snapshot_aggregate_hash:
            aggregate = self.decode_method(pickled_data)
        else:
            aggregate = None

        return aggregate
