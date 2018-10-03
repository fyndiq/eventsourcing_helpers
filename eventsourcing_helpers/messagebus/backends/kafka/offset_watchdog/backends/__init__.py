import structlog

from confluent_kafka_helpers.message import Message

logger = structlog.get_logger(__name__)


class OffsetWatchdogBackend:
    """
    Abstract base class for the offset watchdog backends
    """

    def __init__(self, config: dict) -> None:
        self.config = config

    def _key(self, message: Message):
        """Returns the key for storing offset in the backend"""
        return f'{message._meta.partition}-{message._meta.topic}-{self.config["group.id"]}'

    def seen(self, message: Message) -> bool:
        """Checks if the `message` has been seen before"""
        raise NotImplementedError

    def set_seen(self, message: Message):
        """Marks the message as already seen"""
        raise NotImplementedError
