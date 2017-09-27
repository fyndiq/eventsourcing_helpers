from typing import NamedTuple
from unittest.mock import patch

from eventsourcing_helpers.message import message_factory
from eventsourcing_helpers.serializers import (
    from_message_to_dto, to_message_from_dto)


class SerializerTests:

    @patch('eventsourcing_helpers.serializers.message_factory')
    def test_from_message_to_dto(self, mock_factory):
        """
        Test that the message factory is invoked correctly when
        deserializing a message.
        """
        message = {'class': 'FooClass', 'data': {'foo': 'bar'}}
        from_message_to_dto(message)

        assert mock_factory.call_args[0][0].__name__ == 'FooClass'
        assert mock_factory.call_args[0][0]._fields == ('foo', )

    def test_to_message_from_dto(self):
        """
        Test that we can serialize a DTO to a message.
        """
        fields = [('guid', None)]
        FooEvent = message_factory(NamedTuple('FooEvent', fields))
        dto = FooEvent(guid=1)
        message = to_message_from_dto(dto)

        assert message['class'] == 'FooEvent'
        assert message['data']['guid'] == 1
