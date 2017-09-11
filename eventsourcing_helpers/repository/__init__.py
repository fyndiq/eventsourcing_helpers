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
        return self.backend.save(aggregate, *args, **kwargs)

    def load(self, *args, **kwargs):
        return self.backend.load(*args, **kwargs)
