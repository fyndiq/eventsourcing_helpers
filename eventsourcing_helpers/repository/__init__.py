from eventsourcing_helpers import logger
from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.backends import KafkaBackend

BACKENDS = {
    'kafka': KafkaBackend,
}


class Repository:

    DEFAULT_BACKEND = 'kafka'

    def __init__(self, config):
        backend = config.pop('backend', self.DEFAULT_BACKEND)
        self.backend = BACKENDS[backend](config)

    def save(self, aggregate, *args, **kwargs):
        assert isinstance(aggregate, AggregateRoot)
        logger.info("Committing events to repository")
        self.backend.save(aggregate, *args, **kwargs)
        aggregate.clear_staged_events()
        return

    def load(self, *args, **kwargs):
        logger.info("Loading events from repository")
        return self.backend.load(*args, **kwargs)
