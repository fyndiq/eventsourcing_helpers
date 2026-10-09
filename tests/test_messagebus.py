from unittest.mock import Mock

from eventsourcing_helpers.messagebus import MessageBus


class BackendMock:
    def __init__(self, config):
        self.config = config
        self.consume_batches = Mock()


class MessageBusTests:
    def test_consume_batches_delegates_to_backend(self):
        importer = Mock(return_value=BackendMock)
        config = {"backend": "backend.path", "backend_config": {"consumer": {}}}
        handler = Mock()

        messagebus = MessageBus(config=config, importer=importer)
        messagebus.consume_batches(handler=handler)

        messagebus.backend.consume_batches.assert_called_once_with(handler)
