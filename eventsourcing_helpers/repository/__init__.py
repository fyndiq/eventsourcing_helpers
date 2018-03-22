from typing import Callable, Generator, Any

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.repository.snapshot import Snapshot


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

    def _read_aggregate_from_snapshot(self, id: str,
                                      aggregate_root_class: AggregateRoot
                                      ) -> AggregateRoot:
        """
        Get latest state of the aggregate root by the state from the snapshot
        storage.

        Args:
            id (str): ID of the aggregate root.
            aggregate_root_class (AggregateRoot): The class of the aggregate
                                                  root

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        aggregate = self.snapshot.load_aggregate_from_snapshot(
            id, hash(aggregate_root_class))
        return aggregate

    def _read_aggregate_from_event_history(
        self, id: str, aggregate_root_class: AggregateRoot,
        message_deserializer: Callable
    ) -> AggregateRoot:
        """
        Get latest state of the aggregate root by reading all events and
        applying them.

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

    def get_aggregate_root(
        self, id: str, aggregate_root_class: AggregateRoot,
        message_deserializer: Callable
    ) -> AggregateRoot:
        """
        Get latest state of the aggregate root.

        1. First try to get it from the snapshot.
        2. If there was nothing in the snapshot, go to 4
        3. Else use the aggregate received from the snapshot
        4. Otherwise read the Aggregate root from the event history

        Args:
            id (str): ID of the aggregate root.
            aggregate_root_class (AggregateRoot): The class of the aggregate
                                                  root
            message_deserializer (Callable): the deserializer to use for the
                                             events

        Returns:
            AggregateRoot: Aggregate root with the latest state.
        """
        aggregate = self._read_aggregate_from_snapshot(
            id, aggregate_root_class)

        if aggregate is None:
            aggregate = self._read_aggregate_from_event_history(
                id, aggregate_root_class, message_deserializer)

        return aggregate

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
