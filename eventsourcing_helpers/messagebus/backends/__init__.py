from typing import Type

from eventsourcing_helpers.message import Message


class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, message: Type[Message], key: str = None, **kwargs) -> None:
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()
