from typing import Callable

from eventsourcing_helpers.serializers import from_message_to_dto


class Handler:
    handlers: dict = {}

    def __init__(self, message_deserializer: Callable=
                 from_message_to_dto) -> None:  # yapf: disable
        assert self.handlers
        self.message_deserializer = message_deserializer

    def handle(self, message: dict) -> None:
        """
        Apply correct handler for the received message.

        Args:
            message: Consumed message from the bus.
        """
        raise NotImplementedError(
            "You need to implement handle method in your subclass"
        )
