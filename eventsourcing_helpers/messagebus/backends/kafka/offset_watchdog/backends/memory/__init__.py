from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends import (
    OffsetWatchdogBackend
)


class InMemoryOffsetWatchdogBackend(OffsetWatchdogBackend):
    """
    In-memory offset watchdog backend.
    Stores the last offsets in a instance's dictionary
    """
    def __init__(self, config: dict) -> None:
        super().__init__(config=config)
        self._offset_map: dict = {}

    def seen(self, message: Message) -> bool:
        last_offset = self._offset_map.get(self._key(message), -1)
        return message._meta.offset <= last_offset

    def set_seen(self, message: Message):
        self._offset_map[self._key(message)] = message._meta.offset
