from typing import Callable, Hashable

from eventsourcing_helpers.concurrent_batch import (
    ConcurrentBatchConfig,
    ConcurrentBatchHandler,
    ShutdownObserver,
    kafka_message_key,
)
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

    def consume_batches(self) -> None:
        self._messagebus.consume_batches(handler=self._handler.handle)


class BatchConsumer:
    """
    Consume fixed batches while handling individual messages concurrently.

    Messages with the same key are handled sequentially. Different keys may run
    at the same time, bounded by ``ConcurrentBatchConfig.max_workers``.
    """

    def __init__(
        self,
        messagebus: MessageBus,
        handler: Callable,
        *,
        key: Callable[[object], Hashable] = kafka_message_key,
        config: ConcurrentBatchConfig | None = None,
        shutdown_observer: ShutdownObserver | None = None,
    ) -> None:
        self._messagebus = messagebus
        self._handler = ConcurrentBatchHandler(
            handler,
            key=key,
            config=config,
            shutdown_observer=shutdown_observer,
        )

    def consume(self) -> None:
        try:
            self._messagebus.consume_batches(handler=self._handler)
        finally:
            self._handler.close()
