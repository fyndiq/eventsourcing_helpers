import copy
import time
from typing import Union
from unittest.mock import MagicMock

from confluent_kafka_helpers.message import Message


def create_message(message_class: str, data: dict, headers: Union[None, dict]) -> Message:
    kafka_message = MagicMock()
    kafka_message.configure_mock(
        **{
            'value.return_value': {
                'class': message_class,
                'data': copy.deepcopy(data)
            },
            'timestamp.return_value': (0, time.time()),
            'headers.return_value': headers
        }
    )
    message = Message(kafka_message=kafka_message)
    return message
