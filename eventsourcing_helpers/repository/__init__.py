from typing import Any, Callable, Generator

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot import Snapshot
from eventsourcing_helpers.serializers import from_message_to_dto
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend',  # noqa
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

    def __init__(
        self, config: dict, importer: Callable = import_backend,
        message_deserializer: Callable = from_message_to_dto, **kwargs
    ) -> None:
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.info(
            "Using repository backend", backend=backend_path,
            config=backend_config
        )
        backend_class = importer(backend_path)

        self.message_deserializer = message_deserializer
        self.backend = backend_class(backend_config, **kwargs)
        self.snapshot = Snapshot(config, **kwargs)

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
            self.snapshot.save_aggregate_as_snapshot(aggregate_root)

    def load(self, id: str, aggregate_root_cls: AggregateRoot) -> AggregateRoot:
        """
        Load aggregate by ID accordingly:

        1. First try to get it from the snapshot.
        2. If there was nothing in the snapshot, go to 4
        3. Else use the aggregate received from the snapshot
        4. Otherwise read the Aggregate root from the event history

        Args:
            id: ID of the aggregate root.
            aggregate_root_cls: The class of the aggregate root.

        Returns:
            AggregateRoot: Aggregate root instance with the latest state.
        """
        aggregate_root = self._load_from_snapshot_storage(
            id, aggregate_root_cls
        )
        if aggregate_root is None:
            aggregate_root = self._load_from_event_storage(
                id, aggregate_root_cls
            )
            logger.debug("Aggregate was read from event history")
        else:
            logger.debug("Aggregate was read from snapshot")

        return aggregate_root

    def _load_from_snapshot_storage(
        self, id: str, aggregate_root_cls: AggregateRoot
    ) -> AggregateRoot:
        """
        Load the aggregate from a snapshot.

        Args:
            id: ID of the aggregate root.
            aggregate_root_cls: The class of the aggregate root.

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        schema_hash = aggregate_root_cls().get_schema_hash()
        aggregate_root = self.snapshot.load_aggregate_from_snapshot(
            id, schema_hash
        )
        return aggregate_root

    def _load_from_event_storage(
        self, id: str, aggregate_root_cls: AggregateRoot
    ) -> AggregateRoot:
        """
        Load the aggregate from the event storage.

        Args:
            id: ID of the aggregate root.
            aggregate_root_cls: The class of the aggregate root.

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        aggregate_root = aggregate_root_cls()
        events = self._get_events(id)
        aggregate_root._apply_events(events)

        return aggregate_root

    def _get_events(self, id: str) -> Generator[Any, None, None]:
        """
        Get all aggregate events from the repository.

        Args:
            id: Aggregate root id.

        Returns:
            list: List with all events.
        """
        with self.backend.load(id) as events:  # type:ignore
            for event in events:
                yield self.message_deserializer(event, is_new=False)
