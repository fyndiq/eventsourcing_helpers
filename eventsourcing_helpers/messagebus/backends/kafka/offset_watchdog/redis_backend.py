import structlog

import redis

from confluent_kafka_helpers.message import Message
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog import OffsetWatchdogBackend

logger = structlog.get_logger(__name__)

SOCKET_TIMEOUT = 1
SOCKET_CONNECT_TIMEOUT = 0.5


class RedisOffsetWatchdogBackend(OffsetWatchdogBackend):
    """
    Redis offset watchdog backend.
    Stores the last offsets in a Redis database.
    """

    def __init__(self, consumer_id: str, config: dict) -> None:
        super().__init__(consumer_id=consumer_id, config=config)
        self.redis = redis.StrictRedis.from_url(url=config['REDIS_URI'], decode_responses=True,
                                                socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
                                                socket_timeout=SOCKET_TIMEOUT)

    def seen(self, message: Message) -> bool:
        last_offset = self.redis.get(self._key(message), -1)
        return message._meta.offset <= last_offset

    def set_seen(self, message: Message):
        self.redis.set(self._key(message), message._meta.offset)
