import structlog
from redis import StrictRedis
from redis.sentinel import Sentinel

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import (
    OffsetWatchdogBackend
)

logger = structlog.get_logger(__name__)

__all__ = ['RedisOffsetWatchdogBackend']


class RedisOffsetWatchdogBackend(OffsetWatchdogBackend):
    """
    Redis offset watchdog backend.

    Stores the last offsets in a Redis database.
    """
    DEFAULT_CONFIG = {
        'socket_connect_timeout': 1.0,
        'socket_timeout': 1.0,
        'retry_on_timeout': True,
        'decode_responses': True
    }

    def __init__(self, config: dict) -> None:
        super().__init__(config=config)
        self._redis = None
        self._redis_sentinel = None
        config = {**self.DEFAULT_CONFIG, **config}
        config.pop('group.id', None)

        if 'url' in config:
            assert 'sentinels' not in config
            self._redis = StrictRedis.from_url(**config)
        else:
            assert 'sentinels' in config
            assert 'service_name' in config

            sentinels = [tuple(h.split(':')) for h in config.pop('sentinels').split(',')]
            self._redis_sentinel = Sentinel(sentinels=sentinels, **config)
            self._redis_sentinel_service_name = config.pop('service_name')

    @property
    def redis(self) -> StrictRedis:
        if self._redis:
            return self._redis
        return self._redis_sentinel.master_for(self._redis_sentinel_service_name)  # type: ignore

    def seen(self, message: Message) -> bool:
        last_offset = self.redis.get(self._key(message))
        if last_offset is None:
            return False

        # https://github.com/edenhill/librdkafka/issues/1720
        # until this bug is fixed at least make sure we log when it happens.
        offset = message._meta.offset
        offset_diff = offset - int(last_offset)
        if offset_diff != 1:
            logger.warning("Offset deviation detected", offset_diff=offset_diff)

        return offset <= int(last_offset)

    def set_seen(self, message: Message):
        self.redis.set(self._key(message), message._meta.offset)
