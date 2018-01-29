from unittest.mock import MagicMock

from eventsourcing_helpers.builders import ESEntityBuilder
from eventsourcing_helpers.models import Entity


class TestEntity(Entity):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = None

    def apply_first_event(self, event):
        self.state = "first_event"

    def apply_second_event(self, event):
        self.state = "second_event"

    def apply_third_event(self, event):
        self.state = "third_event"


class ESEntityBuilderTests:

    def setup_method(self):
        events = [
            MagicMock(id="123", _class="FirstEvent", _meta=MagicMock(offset=0)),  # noqa
            MagicMock(id="123", _class="SecondEvent", _meta=MagicMock(offset=1)), # noqa
            MagicMock(id="123", _class="ThirdEvent", _meta=MagicMock(offset=2))
        ]

        config = {
            'return_value.load.return_value.__enter__.return_value': events
        }
        repository = MagicMock()
        repository.configure_mock(**config)

        self.factory = ESEntityBuilder(
            repository_config={},
            message_deserializer=lambda e: e,
            repository=repository,
        )

    def test_get_events_stop_with_max_offset(self):
        events_max_offset1 = list(self.factory._get_events(id="123", max_offset=1))  # noqa
        events_max_offset2 = list(self.factory._get_events(id="123", max_offset=2))  # noqa

        assert len(events_max_offset1) == 2
        assert len(events_max_offset2) == 3

    def test_rebuild_returns_the_entity_to_the_final_state(self):
        entity = self.factory.rebuild(TestEntity, id="123")
        assert entity.state == "third_event"

    def test_rebuild_returns_the_entity_in_the_right_state_when_max_offset_is_given(self):  # noqa
        entity = self.factory.rebuild(TestEntity, id="123", max_offset=0)
        assert entity.state == "first_event"

        entity = self.factory.rebuild(TestEntity, id="123", max_offset=1)
        assert entity.state == "second_event"

        entity = self.factory.rebuild(TestEntity, id="123", max_offset=2)
        assert entity.state == "third_event"
