from collections import namedtuple


def from_message_to_dto(message):
    """Serialize a message to a Data Transfer Object (DTO).
    
    Args:
        message (dict): A dict containing the `class` and `data` keys, the `data` should be dict.
    
    Returns:
        A namedtuple instance represented by message `class` name and the `data` values.
        
    Example:
        >>> message = {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
        >>> dto = from_message_to_dto(message)
        >>> dto
        OrderCompleted(order_id='UA123', date='2017-09-08')
        >>> assert dto.order_id == message['data']['order_id']
        >>> assert dto.date == message['data']['date']
        
    """
    obj = namedtuple(message['class'], message['data'].keys())
    dto = obj(**message['data'])

    return dto


def to_message_from_dto(dto):
    """
    Serialize a Data Transfer Object (DTO) to a message.

    Args:
        dto (obj): A namedtuple instance
    
    Returns:
        A dict with the keys `class` and `data`, the values are extracted from the dto object.

    Example:
        >>> from collections import namedtuple
        >>> OrderCompleted = namedtuple('OrderCompleted', ['order_id', 'date'])
        >>> dto = OrderCompleted('UA123', '2017-09-08')
        >>> message = to_message_from_dto(dto)
        >>> message 
        {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
        >>> assert message['class'] == 'OrderCompleted'
        >>> assert message['data']['order_id'] == dto.order_id
        >>> assert message['data']['date'] == dto.date
    """
    message = {
        'class': dto.__class__.__name__,
        'data': dict(dto._asdict())
    }
    return message
