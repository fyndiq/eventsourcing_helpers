from redis import StrictRedis
from redis.sentinel import Sentinel

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import OffsetWatchdogBackend

__all__ = ['RedisOffsetWatchdogBackend']

SOCKET_TIMEOUT = 0.1
SOCKET_CONNECT_TIMEOUT = 0.1


class RedisOffsetWatchdogBackend(OffsetWatchdogBackend):
    """
    Redis offset watchdog backend.
    Stores the last offsets in a Redis database.
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config=config)
        self._redis = self._redis_sentinel = None
        if 'REDIS_URI' in config:
            assert 'REDIS_SENTINELS' not in config
            self._redis = StrictRedis.from_url(url=config['REDIS_URI'],
                                               decode_responses=True,
                                               socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
                                               socket_timeout=SOCKET_TIMEOUT)

        else:
            assert 'REDIS_SENTINELS' in config
            assert 'REDIS_SENTINEL_SERVICE_NAME' in config
            self._redis_sentinel = Sentinel(config['REDIS_SENTINELS'],
                                            socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
                                            socket_timeout=SOCKET_TIMEOUT,
                                            retry_on_timeout=True,
                                            decode_responses=True,
                                            db=config['REDIS_DATABASE'])
            self._redis_sentinel_service_name = config['REDIS_SENTINEL_SERVICE_NAME']

    @property
    def redis(self) -> StrictRedis:
        if self._redis:
            return self._redis
        return self._redis_sentinel.master_for(self._redis_sentinel_service_name)

    def seen(self, message: Message) -> bool:
        last_offset = self.redis.get(self._key(message))
        if last_offset is None:
            return False
        return message._meta.offset <= int(last_offset)

    def set_seen(self, message: Message):
        self.redis.set(self._key(message), message._meta.offset)
