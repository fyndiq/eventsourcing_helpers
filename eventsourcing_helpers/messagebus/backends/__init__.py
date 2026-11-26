import math
from typing import Callable


def validate_delivery_timeout(timeout):
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("delivery_timeout must be finite and positive")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("delivery_timeout must be finite and positive")


class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()

    def produce_many(self, records, *, timeout=30.0) -> None:
        raise NotImplementedError()

    def get_batch_consumer(self, batch_size=100, timeout=0.1):
        raise NotImplementedError()

    def consume_batches(
        self, handler: Callable, *, batch_size=100, timeout=0.1, delivery_timeout=30.0
    ) -> None:
        raise NotImplementedError()
