from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.backends import KafkaBackend


class Repository:

    def __init__(self, *args, backend=KafkaBackend, **kwargs):
        self.backend = backend(*args, **kwargs)

    def save(self, aggregate, *args, **kwargs):
        assert isinstance(aggregate, AggregateRoot)
        return self.backend.save(aggregate, *args, **kwargs)

    def load(self, *args, **kwargs):
        return self.backend.load(*args, **kwargs)
