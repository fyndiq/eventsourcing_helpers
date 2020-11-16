from unittest.mock import Mock

import pytest

from eventsourcing_helpers.messagebus.backends.mock import MockBackend


class MockBackendTests:
    def setup_method(self):
        self.backend = MockBackend(config={})

    @pytest.mark.parametrize(
        'headers, expected_headers',
        [
            ([('d', b'e')], {'d': 'e'}),
            (None, {}),
        ],
    )
    def test_add_one_consumer_message_should_be_added_to_internal_queue(
        self, headers, expected_headers
    ):
        self.backend.consumer.add_message(message_class='a', data={'b': 'c'}, headers=headers)
        expected_message = dict(message_class='a', data={'b': 'c'}, headers=expected_headers)
        self.backend.consumer.assert_one_message_added_with(**expected_message)

    def test_consume_messages_should_call_handler(self):
        self.backend.consumer.add_message(message_class='a', data={'b': 'c'})
        self.backend.consumer.add_message(message_class='d', data={'e': 'f'})
        handler = Mock()
        self.backend.consume(handler=handler)
        assert handler.call_count == 2

    @pytest.mark.parametrize('headers', [{'d': 'e'}, None])
    def test_produced_message_should_be_added_to_internal_queue(self, headers):
        self.backend.produce(value='b', key='a', headers=headers)
        self.backend.producer.assert_one_message_produced_with(
            **dict(value='b', key='a', headers=headers)
        )

    def test_producer_assert_message_produced_with(self):
        self.backend.produce(value='b', key='a')
        self.backend.produce(value='c', key='b')
        self.backend.producer.assert_message_produced_with(**dict(value='b', key='a'))
        self.backend.producer.assert_message_produced_with(**dict(value='c', key='b'))
