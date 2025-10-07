import copy
import time
from typing import Optional, Union
from unittest.mock import MagicMock

from confluent_kafka_helpers.message import Message


def create_message(
    key: str, value: dict, topic: Optional[str], headers: Optional[Union[dict, list]]
) -> Message:
    if isinstance(headers, dict):
        headers = list(headers.items())
    kafka_message = MagicMock()
    kafka_message.configure_mock(
        **{
            "key.return_value": key,
            "value.return_value": copy.deepcopy(value),
            "topic.return_value": topic,
            "headers.return_value": headers,
            "timestamp.return_value": (0, time.time()),
        }
    )
    message = Message(kafka_message=kafka_message)
    return message
