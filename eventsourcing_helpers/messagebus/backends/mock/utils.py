import copy
import time
from unittest.mock import MagicMock

from confluent_kafka_helpers.message import Message


def create_message(message_class: str, data: dict) -> Message:
    kafka_message = MagicMock()
    kafka_message.configure_mock(
        **{
            'value.return_value': {
                'class': message_class,
                'data': copy.deepcopy(data)
            },
            'timestamp.return_value': (0, time.time()),
        }
    )
    message = Message(kafka_message=kafka_message)
    return message
