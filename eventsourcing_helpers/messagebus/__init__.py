from typing import Callable

from eventsourcing_helpers import logger
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'kafka.KafkaAvroBackend',
}
BACKENDS_PACKAGE = 'eventsourcing_helpers.messagebus.backends'


class MessageBus:

    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(self, config: dict, importer: Callable=import_backend) -> None:
        backend = config.pop('backend', self.DEFAULT_BACKEND)
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.pop('backend_config')

        logger.debug("Using message bus backend", backend=backend,
                     config=backend_config)
        backend_class = importer(BACKENDS_PACKAGE, BACKENDS[backend])
        self.backend = backend_class(backend_config)

    def produce(self, key, value, **kwargs):
        self.backend.produce(key, value, **kwargs)

    def get_consumer(self):
        return self.backend.get_consumer()
