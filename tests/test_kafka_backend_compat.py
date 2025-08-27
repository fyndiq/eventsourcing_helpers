from unittest.mock import Mock

import pytest

from eventsourcing_helpers.messagebus.backends.mock.backend_compat import MockBackend


class MockBackendTests:
    def setup_method(self):
        self.backend = MockBackend(config={})

    @pytest.mark.parametrize(
        "headers, expected_headers",
        [
            ([("d", b"e")], {"d": "e"}),
            (None, {}),
        ],
    )
    def test_consumer_assert_one_message_added_with(self, headers, expected_headers):
        self.backend.consumer.add_message(
            message_class="a", data={"b": "c"}, headers=headers
        )
        expected_message = dict(
            message_class="a", data={"b": "c"}, headers=expected_headers
        )
        self.backend.consumer.assert_one_message_added_with(**expected_message)

    def test_consume_messages_should_call_handler(self):
        self.backend.consumer.add_message(message_class="a", data={"b": "c"})
        self.backend.consumer.add_message(message_class="d", data={"e": "f"})
        handler = Mock()
        self.backend.consume(handler=handler)
        assert handler.call_count == 2

    @pytest.mark.parametrize("headers", [{"d": "e"}, None])
    def test_produced_assert_one_message_produced_with(self, headers):
        self.backend.produce(value="b", key="a", headers=headers)
        self.backend.producer.assert_one_message_produced_with(
            **dict(value="b", key="a", headers=headers)
        )

    @pytest.mark.parametrize("headers", [{"d": "e"}, None])
    def test_producer_assert_multiple_messages_produced_with(self, headers):
        self.backend.produce(key="a", value="b", headers=headers, topic="foo.bar")
        self.backend.produce(key="b", value="c", headers=headers, topic="bar.foo")

        expected_messages = [
            {"key": "a", "value": "b", "headers": headers, "topic": "foo.bar"},
            {"key": "b", "value": "c", "headers": headers, "topic": "bar.foo"},
        ]
        self.backend.producer.assert_multiple_messages_produced_with(
            messages=expected_messages
        )

    @pytest.mark.parametrize("headers", [{"d": "e"}, None])
    def test_producer_assert_multiple_messages_produced_with_invalid_length(
        self, headers
    ):
        self.backend.produce(key="a", value="b", headers=headers, topic="foo.bar")
        self.backend.produce(key="b", value="c", headers=headers, topic="bar.foo")

        expected_messages = [
            {"key": "b", "value": "c", "headers": headers, "topic": "bar.foo"},
            {"key": "a", "value": "b", "headers": headers, "topic": "foo.bar"},
            {"key": "d", "value": "e", "headers": headers, "topic": None},
        ]
        with pytest.raises(AssertionError):
            self.backend.producer.assert_multiple_messages_produced_with(
                messages=expected_messages
            )

    @pytest.mark.parametrize("headers", [{"d": "e"}, None])
    def test_producer_assert_multiple_messages_produced_with_invalid_message(
        self, headers
    ):
        self.backend.produce(key="a", value="b", headers=headers, topic="foo.bar")
        self.backend.produce(key="b", value="c", headers=headers, topic="bar.foo")

        expected_messages = [
            {"key": "a", "value": "b", "headers": headers, "topic": "foo.bar"},
            {"key": "a", "value": "c", "headers": headers, "topic": "bar.foo"},
        ]
        with pytest.raises(AssertionError):
            self.backend.producer.assert_multiple_messages_produced_with(
                messages=expected_messages
            )

    def test_producer_assert_message_produced_with(self):
        self.backend.produce(value="b", key="a")
        self.backend.produce(value="c", key="b")
        self.backend.producer.assert_message_produced_with(**dict(value="b", key="a"))
        self.backend.producer.assert_message_produced_with(**dict(value="c", key="b"))

    def test_producer_assert_no_messages_produced(self):
        self.backend.producer.assert_no_messages_produced()

        self.backend.produce(value="b", key="a")
        with pytest.raises(AssertionError):
            self.backend.producer.assert_no_messages_produced()

    def test_producer_assert_one_message_produced(self):
        self.backend.produce(value="b", key="a")
        self.backend.producer.assert_one_message_produced()

        self.backend.produce(value="b", key="a")
        self.backend.produce(value="b", key="a")
        with pytest.raises(AssertionError):
            self.backend.producer.assert_one_message_produced()
