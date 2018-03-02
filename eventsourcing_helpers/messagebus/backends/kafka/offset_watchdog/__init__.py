"""
In certain cases, messages from Kafka are consumed more than once.
To get as possible to "exaclty once" semantics, we implement our
own Offset Watchdog that provides the best effort of controlling
"double reads" by storing the last offset for the particular
(topic, partition, consumer group) tuple and skipping messages
with offset lower than the stored one.

Certain backend implementations are available:
 * memory (default) - stores offsets in a map, thus works only
   for the current process and not restart-safe;
 * redis - stores offsets in a Redis instance specified via REDIS_URL;
 * null - bypasses all the checks (for using in tests, debugging, etc.)
"""

from typing import Callable

import structlog

from confluent_kafka_helpers.message import Message
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import OffsetWatchdogBackend
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'null': 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends.NullOffsetWatchdogBackend',  # noqa
    'memory': 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends.InMemoryOffsetWatchdogBackend',  # noqa
    'redis': 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.redis_backend.RedisOffsetWatchdogBackend'  # noqa
}

logger = structlog.get_logger(__name__)


class OffsetWatchdog:
    """
    Offset watchdog facade.
    Loads and configures the real storage backend on initialization.
    """
    DEFAULT_BACKEND = 'memory'

    def __init__(self,
                 consumer_id: str,
                 config: dict,
                 importer: Callable = import_backend) -> None:  # yapf: disable
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        # Backend config is optional for some backends
        backend_config = config.get('backend_config', {})

        logger.debug("Using message bus backend", backend=backend_path, config=backend_config)
        backend_class = importer(backend_path)
        self.backend: OffsetWatchdogBackend = backend_class(consumer_id=consumer_id, config=backend_config)

    def seen(self, message: Message) -> bool:
        """Checks if the `message` has been seen before"""
        return self.backend.seen(message)

    def set_seen(self, message: Message):
        """Marks the message as already seen"""
        self.backend.set_seen(message)
