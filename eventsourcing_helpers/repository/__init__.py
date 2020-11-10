from typing import Callable

import structlog
from confluent_kafka import KafkaException

from eventsourcing_helpers.metrics import statsd
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot import Snapshot
from eventsourcing_helpers.serializers import from_message_to_dto
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend',
}

logger = structlog.get_logger(__name__)


class Repository:
    """
    Interface to communicate with a repository backend.

    The repository acts as a mediator between the domain and the data mapping
    layer.

    More concrete it provides a way to store and retrieve events that belongs
    to an aggregate root from/to some kind of storage.

    It also handles snapshots by saving/loading the latest state of an
    aggregate root.
    """
    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(
        self, config: dict, aggregate_root_cls: AggregateRoot,
        ignore_missing_apply_methods: bool = False, importer: Callable = import_backend,
        message_deserializer: Callable = from_message_to_dto, snapshot=Snapshot, **kwargs
    ) -> None:
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.info(
            "Using repository backend", backend=backend_path, config=backend_config,
            ignore_missing_apply_methods=ignore_missing_apply_methods
        )
        backend_class = importer(backend_path)

        self.aggregate_root_cls = aggregate_root_cls
        self.message_deserializer = message_deserializer
        self.snapshot = snapshot(config, **kwargs)
        self.backend = backend_class(backend_config, **kwargs)

        self.ignore_missing_apply_methods = ignore_missing_apply_methods

    def commit(self, aggregate_root: AggregateRoot, **kwargs) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Aggregate root with staged events to commit.
        """
        assert isinstance(aggregate_root, AggregateRoot)
        id, events = aggregate_root.id, aggregate_root._events

        if events:
            assert id, "The id must be set on the aggregate root"
            logger.info("Committing staged events to repository")
            self.snapshot.save(aggregate_root)
            try:
                self.backend.commit(id=id, events=events, **kwargs)
            except KafkaException as e:
                logger.info("Kafka commit failed, rolling back snapshot!")
                statsd.increment(  # type: ignore
                    'eventsourcing_helpers.snapshot.cache.delete',
                    tags=[f'id={id}']
                )
                self.snapshot.delete(aggregate_root)
                raise e

            aggregate_root._clear_staged_events()

    def load(self, id: str, max_offset: int = None) -> AggregateRoot:
        """
        Load an aggregate root accordingly:
         - First try to load it from the snapshot storage.
         - If there are no snapshot load it from the event storage.

        Args:
            id: ID of the aggregate root.
            max_offset: Stop loading events at this position. If set to None
                all events will be loaded.

        Returns:
            AggregateRoot: Aggregate root instance with the latest state.
        """
        aggregate_root = self._load_from_snapshot_storage(id)
        if aggregate_root is None:
            statsd.increment('eventsourcing_helpers.snapshot.cache.misses')
            aggregate_root = self._load_from_event_storage(id, max_offset)
            logger.debug("Aggregate was loaded from event storage")
        else:
            statsd.increment('eventsourcing_helpers.snapshot.cache.hits')  # type: ignore
            logger.debug("Aggregate was loaded from snapshot storage")

        return aggregate_root

    def _load_from_snapshot_storage(self, id: str) -> AggregateRoot:
        aggregate_root = self.snapshot.load(id, self.aggregate_root_cls())

        return aggregate_root

    def _load_from_event_storage(self, id: str, max_offset: int) -> AggregateRoot:
        aggregate_root = self.aggregate_root_cls()
        events = self.backend.get_events(id, max_offset=max_offset)
        events = (self.message_deserializer(event, is_new=False) for event in events)
        aggregate_root._apply_events(
            events, ignore_missing_apply_methods=self.ignore_missing_apply_methods
        )
        return aggregate_root
