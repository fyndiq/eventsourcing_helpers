from typing import Callable

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend',   # noqa
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

        logger.info("Using repository backend", backend=backend_path,
                    config=backend_config)
        backend_class = importer(backend_path)
        self.backend = backend_class(backend_config, **kwargs)

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
