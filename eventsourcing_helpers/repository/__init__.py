from typing import Callable

from eventsourcing_helpers import logger
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'kafka.KafkaAvroBackend',
}
BACKENDS_PACKAGE = 'eventsourcing_helpers.repository.backends'


class Repository:
    """
    The repository acts as a mediator between the domain and the
    data mapping layer.

    More concrete it provides a way to store and retrieve events that
    belongs to an aggregate root - from/to some kind of storage.
    """
    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(self, config: dict, importer: Callable=import_backend) -> None:
        backend = config.pop('backend', self.DEFAULT_BACKEND)
        self.backend = importer(BACKENDS_PACKAGE, BACKENDS[backend])(config)
        logger.debug("Using repository backend", backend=backend)

    def commit(self, aggregate_root: AggregateRoot, **kwargs) -> None:
        """
        Commit staged events to the repository.

        Args:
            aggregate_root: Aggregate root to commit.
        """
        assert isinstance(aggregate_root, AggregateRoot)
        guid, events = aggregate_root.guid, aggregate_root._events

        if events:
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
