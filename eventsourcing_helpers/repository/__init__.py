from typing import Callable

import structlog

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'kafka.KafkaAvroBackend',
}
BACKENDS_PACKAGE = 'eventsourcing_helpers.repository.backends'

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
        backend = config.get('backend', self.DEFAULT_BACKEND)
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.debug("Using repository backend", backend=backend,
                     config=backend_config)
        backend_class = importer(BACKENDS_PACKAGE, BACKENDS[backend])
        self.backend = backend_class(backend_config, **kwargs)

    def commit(self, aggregate_root: AggregateRoot, **kwargs) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Aggregate root to commit.
        """
        assert isinstance(aggregate_root, AggregateRoot)
        guid, events = aggregate_root.guid, aggregate_root._events

        if events:
            assert guid, "The guid must be set on the aggregate root"
            logger.info("Committing staged events to repository")
            self.backend.commit(guid=guid, events=events, **kwargs)
            aggregate_root.clear_staged_events()

    def load(self, guid: str, **kwargs) -> list:
        """
        Load events from the repository.

        Args:
            guid: Aggregate root guid to load.

        Returns:
            list: Loaded events.
        """
        events = self.backend.load(guid, **kwargs)

        return events
