from typing import Callable, Generator, Any

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.repository.snapshots.config import get_snapshot_config  # noqa


BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend',  # noqa
}

SNAPSHOT_BACKENDS = {
    'mongo': 'eventsourcing_helpers.repository.snapshots.backends.mongo.MongoSnapshotBackend',   # noqa
}

logger = structlog.get_logger(__name__)


class Repository:
    """
    Generic interface to communicate with a repository backend.

    The repository acts as a mediator between the domain and the
    data mapping layer.

    More concrete it provides a way to store and retrieve events that
    belongs to an aggregate root - from/to some kind of storage.
    """
    DEFAULT_BACKEND = 'kafka_avro'
    DEFAULT_SNAPSHOT_BACKEND = 'mongo'

    def __init__(self, config: dict, importer: Callable=import_backend,
                 **kwargs) -> None:  # yapf: disable

        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.info(
            "Using repository backend", backend=backend_path,
            config=backend_config
        )
        backend_class = importer(backend_path)
        self.backend = backend_class(backend_config, **kwargs)

        snapshot_config = get_snapshot_config(config)
        snapshot_backend_path = snapshot_config.get(
            'backend', SNAPSHOT_BACKENDS[self.DEFAULT_SNAPSHOT_BACKEND])
        assert 'backend_config' in snapshot_config, "You must pass a (snapshot) backend config"  # noqa
        snapshot_backend_config = snapshot_config.get('backend_config')

        logger.info("Using snapshot backend", backend=snapshot_backend_path,
                    config=snapshot_backend_config)
        snapshot_backend_class = importer(snapshot_backend_path)
        self.snapshot_backend = snapshot_backend_class(
            snapshot_backend_config, **kwargs)

    def commit(self, aggregate_root: AggregateRoot, **kwargs) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Aggregate root to commit.
        """
        assert isinstance(aggregate_root, AggregateRoot)
        id, events = aggregate_root.id, aggregate_root._events

        if events:
            assert id, "The id must be set on the aggregate root"
            logger.info("Committing staged events to repository")
            self.backend.commit(id=id, events=events, **kwargs)
            aggregate_root._clear_staged_events()

    def load(self, id: str, **kwargs) -> list:
        """
        Load events from the repository.

        Args:
            id: Aggregate root id to load.

        Returns:
            list: Loaded events.
        """
        events = self.backend.load(id, **kwargs)

        return events

    def get_aggregate_root(
        self, id: str, aggregate_root_class: AggregateRoot,
        message_deserializer: Callable
    ) -> AggregateRoot:
        """
        Get latest state of the aggregate root.

        Args:
            id (str): ID of the aggregate root.
            aggregate_root_class (AggregateRoot): The class of the aggregate
                                                  root
            message_deserializer (Callable): the deserializer to use for the
                                             events

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        aggregate_root = aggregate_root_class()
        events = self._get_events(id, message_deserializer)
        aggregate_root._apply_events(events)

        return aggregate_root

    def _get_events(self, id: str, message_deserializer: Callable
                    ) -> Generator[Any, None, None]:
        """
        Get all aggregate events from the repository.

        Args:
            id: Aggregate root id.
            message_deserializer (Callable): the deserializer to use for the
                                             events

        Returns:
            list: List with all events.
        """
        with self.load(id) as events:
            for event in events:
                yield message_deserializer(event, is_new=False)
