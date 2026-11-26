# Migration guide

## Opt in to Kafka batches

The batch API is available in `eventsourcing-helpers==2.5.0rc4`. It requires
`confluent-kafka-helpers>=1.8.0rc3`. Choose either the single-message API or the
batch API for each worker. For batch workers, set both `enable.auto.commit`
and `enable.auto.offset.store` to false in the Kafka consumer configuration.

Call `Consumer.consume_batch(batch_size=100, timeout=0.1, delivery_timeout=30.0)`
to run your existing handler. By default, `Handler.handle_batch(messages)`
calls `handle(message)` in order with each input's own headers and trace
context. Override it to process records together. Converted DTOs retain their
input metadata. You can also pass a callable directly to
`MessageBus.consume_batch(handler, batch_size=100, timeout=0.1, delivery_timeout=30.0)`.

`batch_size` is a maximum of 1 to 1,000,000 records. The Kafka fetch can return
fewer records or none after `timeout` seconds. An empty fetch does not invoke
the handler. `delivery_timeout` must be finite and positive and bounds the
delivery wait. The managed loop commits input offsets after the handler
returns and Kafka confirms delivery of its output, including calls to
`produce()` made inside the handler. Offsets advance once per successful
batch, even when the handler processes one message at a time. Handler and
delivery errors leave the batch uncommitted, so records already handled in
that batch can replay. Outside the managed loop, `produce_many()` follows the
producer's configured flush policy.

This example uses the mock backend to publish an output for each input and
check that its business header follows it. With Kafka, use the configured
Kafka backend and the schema-appropriate values.

```python
from eventsourcing_helpers.messagebus import MessageBus

bus = MessageBus({
    "backend": "eventsourcing_helpers.messagebus.backends.mock.backend.MockBackend",
    "backend_config": {},
})
bus.backend.consumer.add_message(
    key="article-1",
    value={"class": "PauseArticle", "data": {"id": "article-1"}},
    headers={"origin_action_type": "pause"},
)


def handle(messages):
    bus.produce_many([
        {
            "key": source._meta.key,
            "value": {"class": "ArticlePaused", "data": source.value["data"]},
            "source": source,
        }
        for source in messages
    ])


bus.consume_batch(handle, batch_size=100, timeout=0.1, delivery_timeout=30.0)
assert bus.backend.producer.messages[0]["headers"]["origin_action_type"] == "pause"
```

On Kafka, `source` supplies the propagated business headers and trace context
for its output. Explicit business headers take precedence. When calling an
existing per-message handler, scope its ordinary `produce()` calls to that
input with `message_context(message)`. Omitting `source` or passing
`source=None` uses the active context. Outside a per-message scope, output
uses the batch span and inherits no input record's business headers.

```python
from confluent_kafka_helpers.context import message_context


def handle_batch(messages):
    for message in messages:
        with message_context(message):
            existing_handler(message)
```

### Manage offsets directly

`MessageBus.get_batch_consumer(batch_size=100, timeout=0.1)` returns a
factory. Assign it with `consumer_factory = bus.get_batch_consumer()`, then
iterate over the consumer inside `with consumer_factory() as consumer:`.
A manual loop owns output delivery confirmation and must
call `consumer.commit_batch(batch)` after the records are handled and their
output is delivered. The managed delivery barrier applies to
`MessageBus.consume_batch()`.

### Ordering and replay

Process records in the supplied order to preserve Kafka partition order.
`produce_many()` queues its records in the supplied order. Kafka does not
provide global ordering across partitions. A crash after output delivery
and before the input offset commit can replay effects. Keep business writes
and emitted events safe to replay in the application. Kafka producer
idempotence applies to producer retries and does not deduplicate application
replays.

## From 1.x -> 2.x

### 1. Update `PydanticMixin` import

If you are using the Pydantic mixin you have to change this import:

From:

	eventsourcing_helpers.message.PydanticMixin

To:

	eventsourcing_helpers.message.pydantic.PydanticMixin

### 2. Update mock backend

If you are using the "old" mock backend and add/assert messages like this:

```python
def test_create_article_stock(app, messagebus):
    messagebus.consumer.add_message(
        message_class="UpdateArticleStock", data={"id": "<article-id>"}
    )
    app.consume()

    expected_event = events.ArticleStockUpdated(
      id="<article-id>"
    )
    messagebus.producer.assert_message_produced_with(
        key="<article-id>", value=expected_event
    )
```

You will have to update the backend used in tests from:

    eventsourcing_helpers.messagebus.backends.mock.MockBackend

To:

    eventsourcing_helpers.messagebus.backends.mock.backend_compat.MockBackend
