from collections import namedtuple


def from_message_to_dto(message):
    """
    Serialize a message to a Data Transfer Object (DTO).

        >> message = {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
        >> from_message_to_dto(message)
        OrderCompleted(order_id='UA123', date='2017-09-08')
    """
    obj = namedtuple(message['class'], message['data'].keys())
    dto = obj(**message['data'])

    return dto


def to_message_from_dto(dto):
    """
    Serialize a Data Transfer Object (DTO) to a message.

        >> dto = OrderCompleted(order_id='UA123', date='2017-09-08')
        >> to_message_from_dto(dto)
        >> {
            'class': 'OrderCompleted',
            'data': {
                'order_id': 'UA123',
                'date': '2017-09-08'
            }
        }
    """
    message = {
        'class': dto.__class__.__name__,
        'data': dict(dto._asdict())
    }
    return message
