from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'kafka_avro': 'eventsourcing_helpers.messagebus.backends.kafka.KafkaAvroBackend'   # noqa
}

logger = structlog.get_logger(__name__)


class MessageBus:
    """
    Interface to communicate with a message bus backend.

    The interface provides methods for producing and consuming
    messages (commands, events).
    """
    DEFAULT_BACKEND = 'kafka_avro'

    def __init__(self, config: dict, handler: Callable=None,
                 importer: Callable=import_backend) -> None:  # yapf: disable
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.debug("Using message bus backend", backend=backend_path,
                     config=backend_config, handler=handler.__class__.__name__)
        backend_class = importer(backend_path)
        self.backend = backend_class(backend_config, handler=handler)

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

    def consume(self):
        """
        Consume and handle messages indefinitely.
        """
        self.backend.consume()
