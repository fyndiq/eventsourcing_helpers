import copy
import time
from collections import deque
from itertools import islice
from typing import Optional, Union
from unittest.mock import MagicMock

from confluent_kafka_helpers.consumer import get_propagate_header_keys, validate_batch_options
from confluent_kafka_helpers.context import get_propagated_headers
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


def output_headers(kwargs):
    """Mirror Kafka source/header selection without adding empty headers to legacy calls."""
    kwargs = dict(kwargs)
    source = kwargs.pop("source", None)
    propagated = source.propagated_headers if source is not None else get_propagated_headers()
    if propagated:
        kwargs["headers"] = {**propagated, **(kwargs.get("headers") or {})}
    return kwargs


class MockBatchConsumer:
    def __init__(self, messages: deque, config: dict, batch_size=100, timeout=0.1):
        validate_batch_options(batch_size, timeout)
        for setting in ("poll_timeout", "non_blocking", "batch_max_size", "batch_max_wait"):
            if setting in config:
                raise ValueError(f"Use batch_size and timeout; batch consumer rejects {setting}")
        for setting in ("enable.auto.commit", "enable.auto.offset.store"):
            if str(config.get(setting, False)).lower() not in {"false", "0"}:
                raise ValueError(f"Batch consumption requires {setting}=false")
        self.messages = messages
        self.batch_size = batch_size
        self.keys = get_propagate_header_keys(config.get("headers.propagate"))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        snapshot = list(self.messages)
        for start in range(0, len(snapshot), self.batch_size):
            batch = list(islice(snapshot, start, start + self.batch_size))
            for message in batch:
                message.propagated_headers = {
                    key: value for key, value in message._meta.headers.items() if key in self.keys
                }
            yield batch

    def commit_batch(self, messages):
        pending = list(islice(self.messages, len(messages)))
        if len(pending) != len(messages) or any(a is not b for a, b in zip(pending, messages)):
            raise ValueError("Mock batch commits must acknowledge the next complete batch")
        for _ in messages:
            self.messages.popleft()
