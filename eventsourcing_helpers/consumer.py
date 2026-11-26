from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.messagebus import MessageBus


class Consumer:
    """
    Helper to consume and handle messages from the bus.
    """

    def __init__(self, messagebus: MessageBus, handler: Handler) -> None:
        self._messagebus = messagebus
        self._handler = handler

    def consume(self) -> None:
        self._messagebus.consume(handler=self._handler.handle)

    def consume_batch(self, batch_size=100, timeout=0.1, delivery_timeout=30.0) -> None:
        self._messagebus.consume_batch(
            handler=self._handler.handle_batch,
            batch_size=batch_size,
            timeout=timeout,
            delivery_timeout=delivery_timeout,
        )
