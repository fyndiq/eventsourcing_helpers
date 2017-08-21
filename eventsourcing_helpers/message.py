from collections import namedtuple


def from_message_to_dto(message):
    """
    Transform a message to a DTO.
    """
    obj = namedtuple(message['class'], message['data'].keys())
    dto = obj(**message['data'])

    return dto


def to_message_from_dto(dto):
    """
    Transform a DTO to a message.
    """
    message = {
        'class': dto.__class__.__name__,
        'data': dict(dto._asdict())
    }
    return message
