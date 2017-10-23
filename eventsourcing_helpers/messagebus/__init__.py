from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'kafka.KafkaAvroBackend',
}
BACKENDS_PACKAGE = 'eventsourcing_helpers.messagebus.backends'

logger = structlog.get_logger(__name__)


class MessageBus:
    """
    Generic interface to communicate with a message bus backend.

    The interface provides methods for producing and consuming
    messages (commands, events).
    """
    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(self, config: dict, importer: Callable=import_backend) -> None:
        backend = config.get('backend', self.DEFAULT_BACKEND)
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.debug("Using message bus backend", backend=backend,
                     config=backend_config)
        backend_class = importer(BACKENDS_PACKAGE, BACKENDS[backend])
        self.backend = backend_class(backend_config)

    def produce(self, value, key=None, **kwargs):
        """
        Produce a message to the message bus.

        Args:
            key: Key to produce message with.
            value: Value to produce message with.
        """
        self.backend.produce(key=key, value=value, **kwargs)

    def get_consumer(self):
        """
        Get a message consumer.

        Returns:
            Callable: Message consumer.
        """
        return self.backend.get_consumer()
