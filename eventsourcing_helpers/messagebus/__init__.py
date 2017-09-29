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
        self.backend = importer(BACKENDS_PACKAGE, BACKENDS[backend])(config)
        logger.debug("Using message bus backend", backend=backend)

    def produce(self, key, value, **kwargs):
        self.backend.produce(key, value, **kwargs)

    def get_consumer(self):
        return self.backend.get_consumer()
