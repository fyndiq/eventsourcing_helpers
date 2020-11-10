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

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import (
    OffsetWatchdogBackend
)
from eventsourcing_helpers.metrics import base_metric, statsd
from eventsourcing_helpers.utils import import_backend

BACKENDS_PATH = 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends'
BACKENDS = {
    'null': f'{BACKENDS_PATH}.null.NullOffsetWatchdogBackend',
    'memory': f'{BACKENDS_PATH}.memory.InMemoryOffsetWatchdogBackend',
    'redis': f'{BACKENDS_PATH}.redis.RedisOffsetWatchdogBackend'
}

logger = structlog.get_logger(__name__)


class OffsetWatchdog:
    """
    Offset watchdog facade.

    Loads and configures the real storage backend on initialization.
    """
    DEFAULT_BACKEND = 'memory'

    def __init__(self, config: dict, importer: Callable = import_backend) -> None:
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        backend_config = config.get('backend_config')
        self.config = backend_config

        logger.debug("Using offset watchdog backend", backend=backend_path, config=self.config)
        backend_class = importer(backend_path)
        self.backend: OffsetWatchdogBackend = backend_class(config=self.config)

    def seen(self, message: Message) -> bool:
        """Checks if the `message` has been seen before"""
        seen = self.backend.seen(message)
        if seen:
            logger.warning("Message already seen previously", message_meta=message._meta)
            statsd.increment(  # type: ignore
                f'{base_metric}.messagebus.kafka.offset_watchdog.seen.count', tags=[
                    f'partition:{message._meta.partition}', f'topic:{message._meta.topic}',
                    f'consumer_group:{self.config["group.id"]}'  # type: ignore
                ]
            )
        return seen

    def set_seen(self, message: Message):
        """Marks the message as already seen"""
        self.backend.set_seen(message)
