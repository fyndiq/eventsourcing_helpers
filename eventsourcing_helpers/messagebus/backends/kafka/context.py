import contextlib
import contextvars
from collections.abc import Iterable, Iterator
from typing import Optional

from opentelemetry.trace import Link

from eventsourcing_helpers.tracing import tracer

from confluent_kafka_helpers.context import clear_propagated_headers, set_propagated_headers
from confluent_kafka_helpers.message import Message

_message_handler_span_links: contextvars.ContextVar[Optional[list[Link]]] = contextvars.ContextVar(
    "message_handler_span_links", default=None
)


def get_message_handler_span_links() -> Optional[list[Link]]:
    return _message_handler_span_links.get()


@contextlib.contextmanager
def message_handler_context(
    message: Message, propagate_header_keys: Iterable[str] = ()
) -> Iterator[None]:
    """
    Set per-message propagated headers and span links while handling a batch message.

    Batch consumers receive one Kafka consume span for the whole batch. Use this
    context around per-message work inside a batch handler so downstream code sees
    the same propagated headers as single-message consumption, and handler spans
    link back to the message's trace context.
    """
    headers = message._meta.headers or {}
    propagate_header_keys = set(propagate_header_keys)
    propagated_headers = {
        key: value for key, value in headers.items() if key in propagate_header_keys
    }
    links = tracer.extract_links(context=tracer.extract_headers(headers=headers))
    token = _message_handler_span_links.set(links)
    set_propagated_headers(propagated_headers)

    try:
        yield
    finally:
        clear_propagated_headers()
        _message_handler_span_links.reset(token)
