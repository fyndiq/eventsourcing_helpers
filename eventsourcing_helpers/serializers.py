from collections import namedtuple
from typing import Any


def from_message_to_dto(message: dict) -> Any:
    """
    Deserialize a message to a data transfer object (DTO).

    The message is expected to include a 'class' and 'data' attribute. The
    class defines the type of the object and data the attributes.

    Args:
        message: Message to deserialize.

    Returns:
        namedtuple: DTO of type 'class' hydrated with 'data'.

    Example:
        >>> message = {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
        >>> from_message_to_dto(message)
        OrderCompleted(order_id='UA123', date='2017-09-08')
    """
    data = message['data']
    obj = namedtuple(message['class'], data.keys())
    dto = obj(**data)

    return dto


def to_message_from_dto(dto: Any) -> dict:
    """
    Serialize a data transfer object (DTO) to a message.

    The message will include two attributes 'class' and 'data. The class will
    be the type of the instance and the data will be a dict of all attributes.

    Args:
        dto: A namedtuple instance.

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
    data = dict(dto._asdict())
    message = {'class': dto.__class__.__name__, 'data': data}

    return message
