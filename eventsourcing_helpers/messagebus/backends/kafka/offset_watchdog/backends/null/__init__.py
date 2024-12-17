from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import (
    OffsetWatchdogBackend,
)

from confluent_kafka_helpers.message import Message


class NullOffsetWatchdogBackend(OffsetWatchdogBackend):
    """
    Null (bypass) backend. Does nothing and treats all messages as never seen.
    """

    def seen(self, message: Message) -> bool:
        return False

    def set_seen(self, message: Message):
        pass
