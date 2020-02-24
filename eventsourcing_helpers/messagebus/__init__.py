from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.messagebus.backends.kafka.KafkaAvroBackend',
    'mock': 'eventsourcing_helpers.messagebus.backends.mock.MockBackend'
}

logger = structlog.get_logger(__name__)


class MessageBus:
    """
    Interface to communicate with a message bus backend.

    The interface provides methods for producing and consuming messages
    (commands, events).
    """
    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(self, config: dict, importer: Callable = import_backend, **kwargs) -> None:
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.debug("Using message bus backend", backend=backend_path, config=backend_config)
        backend_class = importer(backend_path)
        self.backend = backend_class(config=backend_config, **kwargs)

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

        Use this if you want more control of the message loop.

        Returns:
            Callable: Message consumer.
        """
        return self.backend.get_consumer()

    def consume(self, handler: Callable):
        """
        Consume and handle messages indefinitely.
        """
        self.backend.consume(handler)
