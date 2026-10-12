from typing import Callable


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
