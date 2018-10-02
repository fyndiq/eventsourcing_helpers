import hashlib
from typing import Callable

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot.config import get_snapshot_config
from eventsourcing_helpers.repository.snapshot.serializers import (
    from_aggregate_root_to_snapshot, from_snapshot_to_aggregate_root
)
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'null': 'eventsourcing_helpers.repository.snapshot.backends.null.NullSnapshotBackend'
}

logger = structlog.get_logger(__name__)


def get_hash(seed: str) -> str:
    """
    Returns a hash of the given seed
    """
    md5_hash = hashlib.md5(seed.encode())
    string_hash = md5_hash.hexdigest()
    return string_hash


class Snapshot:
    """
    Interface to communicate with a snapshot backend.

    The interface provides methods for saving and loading a snapshot
    """
    DEFAULT_BACKEND = 'null'

    def __init__(
        self, config: dict, importer: Callable = import_backend,
        serializer: Callable = from_aggregate_root_to_snapshot,
        deserializer: Callable = from_snapshot_to_aggregate_root,
        hash_function: Callable = get_hash,
        **kwargs
    ) -> None:
        config = get_snapshot_config(config)
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        backend_config = config.get('backend_config')

        logger.info(
            "Using snapshot backend", backend=backend_path,
            config=backend_config
        )
        backend_class = importer(backend_path)
        self.backend = backend_class(backend_config, **kwargs)

        self.serializer = serializer
        self.deserializer = deserializer
        self.hash_function = hash_function

    def save(self, aggregate_root: AggregateRoot) -> None:
        """
        Saves an aggregate to the snapshot storage

        Args:
            aggregate_root: The aggregate to be saved
        Returns:
            None
        """
        current_hash = self.hash_function(
            aggregate_root.__class__().get_representation())

        snapshot = self.serializer(aggregate_root, current_hash)
        self.backend.save(aggregate_root.id, snapshot)

    def load(self, id: str, aggregate_root: AggregateRoot) -> AggregateRoot:
        """
        Loads an aggregate root from the snapshot storage.

        If the saved aggregate root hash in the database does not match the
        current hash it means that the model has changed since the snapshot was
        saved.

        If that is the case the snapshot should not be used.

        Args:
            id: ID of the aggregate root.
            Args:
            aggregate_root: The aggregate type of the object
                                            to load

        Returns:
            AggregateRoot: Aggregate root instance with the latest state.
        """
        snapshot = self.backend.load(id)
        current_hash = self.hash_function(
            aggregate_root.__class__().get_representation())
        aggregate_root = self.deserializer(snapshot, current_hash)

        return aggregate_root

    def delete(self, aggregate_root: AggregateRoot) -> None:
        """
        Deletes the snapshot of the aggregate root.

        Args:
            aggregate_root: The aggregate to be deleted

        Returns:
            None
        """
        self.backend.delete(aggregate_root.id)
