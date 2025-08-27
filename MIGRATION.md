# Migration guide

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
