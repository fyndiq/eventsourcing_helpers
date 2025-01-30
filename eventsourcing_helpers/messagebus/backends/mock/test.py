import pytest

pytest.register_assert_rewrite("eventsourcing_helpers.messagebus.backends.mock.backend")


@pytest.fixture
def consumer():
    """Returns an `eventsourcing_helpers.Consumer` instance.

    You'll most probably want to return the applications consumer instance here.
    """
    raise RuntimeError("You must override the `consumer` fixture")


@pytest.fixture
def messagebus(consumer):
    """Returns the mocked messagebus backend instance.

    Basically just a fixture to make the interface in the tests a bit nicer.
    """
    return consumer._messagebus.backend


@pytest.fixture
def consume_messages(consumer, messagebus):
    """Add messages to the mocked messagebus consumer and consume them."""

    def consume(messages):
        messagebus.consumer.add_messages(messages)
        consumer.consume()

    return consume
