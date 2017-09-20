from importlib import import_module

from eventsourcing_helpers import logger
from eventsourcing_helpers.models import AggregateRoot

BACKENDS = {
    'kafka': 'KafkaBackend',
}


def import_backend(backend):
    module = import_module('eventsourcing_helpers.repository.backends')
    backend = getattr(module, backend)
    return backend


class Repository:

    DEFAULT_BACKEND = 'kafka'

    def __init__(self, config):
        backend = config.pop('backend', self.DEFAULT_BACKEND)
        self.backend = import_backend(BACKENDS[backend])(config)
        logger.debug(f"Using {backend} as repository backend")

    def commit(self, aggregate, *args, **kwargs):
        assert isinstance(aggregate, AggregateRoot)
        logger.info("Committing staged events to repository")
        self.backend.commit(aggregate, *args, **kwargs)
        aggregate.clear_staged_events()
        return

    def load(self, *args, **kwargs):
        logger.info("Loading events from repository")
        return self.backend.load(*args, **kwargs)
