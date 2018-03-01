from eventsourcing_helpers.message import Message


class OffsetWatchdog:
    def __init__(self, consumer_id: str):
        self._consumer_id = consumer_id

    def _key(self, message: Message):
        return f'{message._meta.partition}-{message._meta.topic}-{self._consumer_id}'

    def seen(self, message: Message) -> bool:
        raise NotImplementedError

    def set_seen(self, message: Message):
        raise NotImplementedError


class InMemoryOffsetWatchdog(OffsetWatchdog):
    def __init__(self, consumer_id: str):
        super().__init__(consumer_id=consumer_id)
        self._offset_map = {}

    def seen(self, message: Message) -> bool:
        last_offset = self._offset_map.get(self._key(message), -1)
        return message._meta.offset <= last_offset

    def set_seen(self, message: Message):
        self._offset_map[self._key(message)] = message._meta.offset
