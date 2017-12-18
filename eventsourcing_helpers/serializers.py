from cnamedtuple import namedtuple

from eventsourcing_helpers.message import Message, message_factory
from confluent_kafka_helpers.message import Message as ConfluentMessage


def from_message_to_dto(message: ConfluentMessage) -> Message:
    """
    Deserialize a message to a data transfer object (DTO).

    The message is just a dict expected to include a 'class' and 'data'
    attribute. The class defines the type of the DTO and data the attributes.

    Args:
        message: Message to deserialize.

    Returns:
        Message: DTO instance of type 'class' hydrated with 'data'.

    Example:
        >>> message = {
        ...    'class': 'OrderCompleted',
        ...    'data': {
        ...        'order_id': 'UA123',
        ...        'date': '2017-09-08'
        ...    }
        ... }
        >>> from_message_to_dto(message)
        OrderCompleted(order_id='UA123', date='2017-09-08')
    """
    data, class_name = message.value['data'], message.value['class']
    data['meta_data'] = message._meta

    cls = namedtuple(class_name, data.keys())
    dto = message_factory(cls)(**data)

    return dto


def to_message_from_dto(dto: Message) -> dict:
    """
    Serialize a data transfer object (DTO) to a message.

    The message includes two keys 'class' and 'data. The class will be the
    type of the DTO and the data will be a dict with all its attributes.

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
    message = {'class': dto._class, 'data': dto.to_dict()}

    return message
