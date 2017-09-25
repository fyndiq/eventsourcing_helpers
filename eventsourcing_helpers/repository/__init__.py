from importlib import import_module
from typing import Callable

from eventsourcing_helpers import logger
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.backends.base import RepositoryBackend

BACKENDS = {
    'kafka': 'kafka.KafkaBackend',
}


def import_backend(location: str) -> RepositoryBackend:
    """
    Dynamically load a repository backend.

    Args:
        location: Location of the backend class in the
            backends package. Format: 'module.class'.

    Returns:
        RepositoryBackend: The backend class.

    Example:
        >>> load_backend('kafka.KafkaBackend')
        <class 'eventsourcing_helpers.repository.backends.kafka.KafkaBackend'>
    """
    module_name, class_name = location.rsplit('.', 1)

    package = 'eventsourcing_helpers.repository.backends'
    module = import_module(f'{package}.{module_name}')
    backend_cls = getattr(module, class_name)

    return backend_cls


class Repository:
    """
    The repository acts as a mediator between the domain and the
    data mapping layer.

    More concrete it provides a way to store and retrieve events that
    belongs to an aggregate root - from/to some kind of storage.
    """
    DEFAULT_BACKEND = 'kafka'

    def __init__(self, config: dict, importer: Callable=import_backend) -> None:
        backend = config.pop('backend', self.DEFAULT_BACKEND)
        self.backend = importer(BACKENDS[backend])(config)
        logger.debug(f"Using {backend} as repository backend")

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
