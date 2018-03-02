from typing import Callable

import structlog

from confluent_kafka_helpers.message import Message
from eventsourcing_helpers.utils import import_backend

BACKENDS = {
    'null': 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.NullOffsetWatchdogBackend',  # noqa
    'memory': 'eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.InMemoryOffsetWatchdogBackend'  # noqa
}

logger = structlog.get_logger(__name__)


class OffsetWatchdog:
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
        return self.backend.seen(message)

    def set_seen(self, message: Message):
        self.backend.set_seen(message)


class OffsetWatchdogBackend:
    def __init__(self, consumer_id: str, config: dict) -> None:
        self._consumer_id = consumer_id

    def _key(self, message: Message):
        return f'{message._meta.partition}-{message._meta.topic}-{self._consumer_id}'

    def seen(self, message: Message) -> bool:
        raise NotImplementedError

    def set_seen(self, message: Message):
        raise NotImplementedError


class NullOffsetWatchdogBackend(OffsetWatchdogBackend):
    def seen(self, message: Message) -> bool:
        return False

    def set_seen(self, message: Message):
        pass


class InMemoryOffsetWatchdogBackend(OffsetWatchdogBackend):
    def __init__(self, consumer_id: str, config: dict) -> None:
        super().__init__(consumer_id=consumer_id, config=config)
        self._offset_map: dict = {}

    def seen(self, message: Message) -> bool:
        last_offset = self._offset_map.get(self._key(message), -1)
        return message._meta.offset <= last_offset

    def set_seen(self, message: Message):
        self._offset_map[self._key(message)] = message._meta.offset
