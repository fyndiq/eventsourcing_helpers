class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()

    def get_batch_consumer(self, batch_size=100, timeout=0.1):
        raise NotImplementedError()

    def produce_many(self, records) -> None:
        raise NotImplementedError()

    def consume_batch(self, handler, batch_size=100, timeout=0.1, delivery_timeout=30.0) -> None:
        raise NotImplementedError()
