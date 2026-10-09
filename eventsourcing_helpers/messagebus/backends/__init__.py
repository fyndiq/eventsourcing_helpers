from typing import Callable

from eventsourcing_helpers.handler import Handler


def is_message_handler(handler: Callable) -> bool:
    """Return True for bound EventHandler/CommandHandler-style handlers."""
    return isinstance(getattr(handler, "__self__", None), Handler)


class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()

    def consume_batches(self, handler: Callable) -> None:
        raise NotImplementedError()
