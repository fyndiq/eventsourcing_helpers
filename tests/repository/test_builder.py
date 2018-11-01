from unittest.mock import MagicMock

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.builder import AggregateBuilder


class TestAggregate(AggregateRoot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = None

    def apply_first_event(self, event):
        self.state = "first_event"

    def apply_second_event(self, event):
        self.state = "second_event"

    def apply_third_event(self, event):
        self.state = "third_event"


class AggregateBuilderTests:
    def setup_method(self):
        events = [
            MagicMock(id="123", _class="FirstEvent", _meta=MagicMock(offset=0)),
            MagicMock(id="123", _class="SecondEvent", _meta=MagicMock(offset=1)),
            MagicMock(id="123", _class="ThirdEvent", _meta=MagicMock(offset=2))
        ]
        loader = MagicMock()
        loader.configure_mock(**{
            'return_value.load.return_value.__enter__.return_value': events
        })
        config = {
            'backend_config': {'loader': {'foo': 'bar'}}
        }
        self.factory = AggregateBuilder(
            config=config, aggregate_root_cls=TestAggregate,
            message_deserializer=lambda e, **kwargs: e, loader=loader
        )

    def test_rebuild_returns_the_aggregate_to_the_final_state(self):
        aggregate = self.factory.rebuild(id="123")
        assert aggregate.state == "third_event"

    def test_rebuild_returns_the_aggregate_in_the_right_state_when_max_offset_is_given(self):
        aggregate = self.factory.rebuild(id="123", max_offset=0)
        assert aggregate.state == "first_event"

        aggregate = self.factory.rebuild(id="123", max_offset=1)
        assert aggregate.state == "second_event"

        aggregate = self.factory.rebuild(id="123", max_offset=2)
        assert aggregate.state == "third_event"
