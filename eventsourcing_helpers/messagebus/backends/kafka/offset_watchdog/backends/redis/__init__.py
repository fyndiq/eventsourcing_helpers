import structlog
from redis import StrictRedis
from redis.sentinel import Sentinel

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import (  # noqa
    OffsetWatchdogBackend
)

logger = structlog.get_logger(__name__)

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
        socket_connect_timeout = config.get(
            'socket_connect_timeout', SOCKET_CONNECT_TIMEOUT
        )
        socket_timeout = config.get('socket_timeout', SOCKET_TIMEOUT)
        if 'redis_uri' in config:
            assert 'redis_sentinels' not in config
            self._redis = StrictRedis.from_url(
                url=config['redis_uri'],
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=socket_timeout, retry_on_timeout=True,
                decode_responses=True
            )

        else:
            assert 'redis_sentinels' in config
            assert 'redis_sentinel_service_name' in config

            sentinels = [
                tuple(h.split(':'))
                for h in config['redis_sentinels'].split(',')
            ]

            self._redis_sentinel = Sentinel(
                sentinels, db=config['redis_database'],
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=socket_timeout, retry_on_timeout=True,
                decode_responses=True
            )
            self._redis_sentinel_service_name = config[
                'redis_sentinel_service_name'
            ]

    @property
    def redis(self) -> StrictRedis:
        if self._redis:
            return self._redis
        return self._redis_sentinel.master_for(
            self._redis_sentinel_service_name
        )

    def seen(self, message: Message) -> bool:
        last_offset = self.redis.get(self._key(message))
        if last_offset is None:
            return False

        # https://github.com/edenhill/librdkafka/issues/1720
        # until this bug is fixed at least make sure we log when it happens.
        offset = message._meta.offset
        offset_diff = offset - int(last_offset)
        if offset_diff != 1:
            logger.warning(
                "Offset deviation detected", offset_diff=offset_diff
            )

        return offset <= int(last_offset)

    def set_seen(self, message: Message):
        self.redis.set(self._key(message), message._meta.offset)
