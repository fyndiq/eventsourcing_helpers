from collections import namedtuple

from eventsourcing_helpers.serializers import (
    from_message_to_dto, to_message_from_dto)


class SerializerTests:

    def test_from_message_to_dto(self):
        """
        Test that we can serialize a message to a DTO.
        """
        message = {'class': 'FooClass', 'data': {'foo': 'bar'}}
        dto = from_message_to_dto(message)

        assert dto.__class__.__name__ == 'FooClass'
        assert dto.foo == 'bar'

    def test_to_message_from_dto(self):
        """
        Test that we can deserialize a DTO to a message.
        """
        FooEvent = namedtuple('FooEvent', 'guid')
        dto = FooEvent(guid=1)
        message = to_message_from_dto(dto)

        assert message['class'] == 'FooEvent'
        assert message['data']['guid'] == 1
