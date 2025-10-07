from typing import Any

from eventsourcing_helpers.message import Message, message_factory

from confluent_kafka_helpers.message import Message as ConfluentKafkaMessage

try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple


def from_message_to_dto(
    message: ConfluentKafkaMessage, is_new: bool = True, deserialize_class: type | None = None
) -> Message | Any:
    """
    Deserialize a `confluent_kafka_helpers.message.Message` to a data transfer
    object (DTO).

    If no `deserialize_class` is provided a default wrapped `namedtuple` class
    will be used.

    Args:
        message: Message to deserialize.
        is_new: Flag that indicates if the message is new or loaded
            from the repository.
        deserialize_class (optional): Class to use for deserializing the
        message into a DTO.

    Returns:
        object: DTO instance hydrated with message data.

    Example:
        >>> Message({"class": "OrderCompleted", "data": {"order_id": "UA123"}})
        >>> from_message_to_dto(message)
        OrderCompleted(order_id="UA123")
    """
    data, class_name, meta = (
        message.value["data"],
        message.value["class"],
        message._meta,
    )
    if deserialize_class:
        dto = deserialize_class(Meta=meta, **data)
    else:
        message_cls = namedtuple(class_name, data.keys() | {"Meta"})
        dto = message_factory(message_cls, is_new=is_new)(Meta=meta, **data)

    return dto


def to_message_from_dto(dto: Message) -> dict:
    """
    Serialize a data transfer object (DTO) to a message.

    The message includes two keys `class` and `data`. The `class` will be the
    type of the DTO and the `data` will be a dict with all attributes.

    Args:
        dto: DTO instance.

    Returns:
        dict: Serialized message.

    Example:
        >>> OrderCompleted = namedtuple('OrderCompleted', ['order_id', 'date'])
        >>> dto = OrderCompleted('UA123', '2017-09-08')
        >>> to_message_from_dto(dto)
        {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
    """
    message = {"class": dto._class, "data": dto.to_dict()}

    return message
