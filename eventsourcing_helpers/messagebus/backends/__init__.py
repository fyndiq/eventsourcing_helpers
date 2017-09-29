class MessageBusBackend:
    """
    Message bus interface.
    """

    def produce(self, *args, **kwargs):
        raise NotImplementedError()

    def get_consumer(self):
        raise NotImplementedError()
