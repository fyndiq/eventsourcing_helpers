class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, key: str, value: dict, topic: str=None) -> None:
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()
