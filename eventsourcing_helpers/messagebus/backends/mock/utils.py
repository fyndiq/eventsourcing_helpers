import copy
import time
from unittest.mock import MagicMock


def create_kafka_message(message_class: str, data: dict) -> MagicMock:
    message = MagicMock()
    message.configure_mock(
        **{
            'value.return_value': {
                'class': message_class,
                'data': copy.deepcopy(data)
            },
            'timestamp.return_value': (0, time.time()),
        }
    )
    return message
