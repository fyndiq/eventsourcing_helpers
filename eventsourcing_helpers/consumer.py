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
