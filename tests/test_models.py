from unittest.mock import Mock, patch

import pytest

from eventsourcing_helpers.models import AggregateRoot, Entity, EntityDict


class Foo(AggregateRoot):
    pass


class Bar(Entity):
    pass


class LargerTestAggregate(AggregateRoot):
    def __init__(self):
        super().__init__()
        self.title = None
        self.description = None
        self.status = None
        self.tags = None
        self.properties = None


class NestedAggregate(AggregateRoot):
    def __init__(self):
        super().__init__()
        self.nested_entity = EntityDict()
        self.nested_entity.nested_entity_id = 'a'


class DoubleNestedAggregate(AggregateRoot):
    def __init__(self):
        super().__init__()
        self.nested_entity = EntityDict()
        self.nested_entity.nested_entity_id = 'a'
        self.nested_entity.second_nested_entity = EntityDict()
        self.nested_entity.second_nested_entity.second_nested_entity_id = 'a'


class AggregateRootTests:

    def test_subclass(self):
        """
        Test that the aggregate root is just a subclass of `Entity`.
        """
        aggregate_root = Foo()

        assert isinstance(aggregate_root, AggregateRoot)
        assert isinstance(aggregate_root, Entity)


class EntityTests:

    def setup_method(self):
        self.aggregate_root = Foo()
        self.entity = Bar()
        class FooEvent:
            pass
        self.event = Mock(spec=FooEvent)
        self.event._class = 'FooEvent'
        self.event.id = 1

    @patch('eventsourcing_helpers.models.Entity._get_apply_method_name')
    @patch('eventsourcing_helpers.models.Entity._apply_event')
    def test_apply_event(self, mock_event, mock_apply):
        """
        Test that correct methods are invoked when applying an event.
        """
        mock_apply.return_value = 'apply_foo_event'
        is_new = False

        with patch.object(Entity, '_get_entity', wraps=self.aggregate_root._get_entity) as mock_get_entity:
            self.aggregate_root.apply_event(self.event, is_new)

        mock_apply.assert_called_once_with(self.event.__class__.__name__)
        mock_get_entity.assert_called_once_with(self.event.id)
        mock_event.assert_called_once_with(
            self.event, self.aggregate_root, 'apply_foo_event', is_new
        )

    @patch('eventsourcing_helpers.models.Entity.apply_event')
    def test_apply_events(self, mock_apply):
        """
        Test that the apply method is run multiple times.
        """
        num_events = 4
        events = [None] * num_events

        self.aggregate_root._apply_events(events)
        assert mock_apply.call_count == num_events

    def test_clear_events(self):
        """
        Test that the committed events are cleared from ALL
        `Entity` instances.
        """
        self.aggregate_root._events.append(self.event)
        self.entity._events.append(self.event)

        assert len(self.aggregate_root._events) == 2
        self.aggregate_root._clear_staged_events()

        assert len(self.aggregate_root._events) == 0
        assert len(self.entity._events) == 0

    def test_create_id(self):
        """
        Test that the returned id is a string and with correct length.
        """
        id = self.entity.create_id()

        assert isinstance(id, str)
        assert len(id) == 36

    def test_get_apply_method_name(self):
        """
        Test that we get the correct apply method name for an event.
        """
        apply_method_name = self.entity._get_apply_method_name(self.event._class)
        assert apply_method_name == 'apply_foo_event'

    @patch('eventsourcing_helpers.models.Entity._get_all_entities')
    def test_get_entity(self, mock_entities):
        """
        Test that the correct entity is returned.
        """
        self.aggregate_root.id = 1
        self.entity.id = 2

        entities = [self.aggregate_root, self.entity]
        mock_entities.return_value = entities
        entity = self.aggregate_root._get_entity(self.entity.id)
        assert entity == self.entity

        entities.pop()
        mock_entities.return_value = entities
        entity = self.aggregate_root._get_entity(self.entity.id)
        assert entity == self.aggregate_root

    @patch('eventsourcing_helpers.models.Entity._get_child_entities')
    def test_get_all_entities(self, mock_entities):
        """
        Test that we get all child entities including the current instance.
        """
        mock_entities.return_value = [self.entity]
        entities = self.aggregate_root._get_all_entities()

        assert list(entities) == [self.aggregate_root, self.entity]

    @patch('eventsourcing_helpers.models.Entity._get_all_entities')
    def test_get_child_entities(self, mock_entities):
        """
        Test that we get all child entities for the current instance.
        """
        self.aggregate_root.__dict__.update({'Bar': self.entity})
        mock_entities.return_value = [self.entity]
        entities = self.aggregate_root._get_child_entities()

        assert list(entities) == [self.entity]

    def test_stage_event(self):
        """
        Test that an event is staged correctly.
        """
        self.entity._stage_event(self.event, is_new=False)
        assert len(self.entity._events) == 0

        self.entity._stage_event(self.event, is_new=True)
        assert len(self.entity._events) == 1
        assert self.event in self.entity._events

    def test_get_apply_method(self):
        """
        Test that the correct method is returned.
        """
        mock_method = Mock()
        self.aggregate_root.apply_foo_event = mock_method
        method = self.aggregate_root._get_apply_method(
            self.aggregate_root, 'apply_foo_event'
        )
        assert method == mock_method

    @patch('eventsourcing_helpers.models.Entity._get_apply_method')
    @patch('eventsourcing_helpers.models.Entity._stage_event')
    def test_apply_event_aggregate_root(self, mock_stage, mock_apply):
        """
        Test that an event is correctly applied on the aggregate root.
        """
        method_name, is_new = 'apply_foo_event', True

        self.aggregate_root._apply_event(
            self.event, self.aggregate_root, method_name, is_new
        )
        mock_apply.assert_called_once_with(self.aggregate_root, method_name)
        mock_stage.assert_called_once_with(self.event, is_new)

    @pytest.mark.parametrize('entity, data', [
        (Foo(), ['Foo', 'id', '_version']),
        (LargerTestAggregate(), [
            'LargerTestAggregate', 'id', '_version', 'title', 'description',
            'status', 'tags', 'properties'
        ]),
    ])
    def test_get_representation_includes_name_and_fields(self, entity, data):
        representation = entity.get_representation()
        for field in data:
            assert field in representation

    @pytest.mark.parametrize('entity, data', [
        (NestedAggregate(), ['NestedAggregate',
                             'id', '_version',
                             'nested_entity', 'nested_entity_id',
                             'eventsourcing_helpers.models.EntityDict']),
        (DoubleNestedAggregate(), [
            'DoubleNestedAggregate', 'id', '_version',
            'nested_entity', 'nested_entity_id',
            'second_nested_entity', 'second_nested_entity_id',
            'eventsourcing_helpers.models.EntityDict']),
    ])
    def test_get_representation_handles_nested_entities(
        self, entity, data
    ):
        representation = entity.get_representation()
        for field in data:
            assert field in representation


class EntityDictTests:

    def setup_method(self):
        self.entity = Bar()
        self.entity_dict = EntityDict({1: self.entity})

    def test_values(self):
        """
        Test that the value must be of instance `Entity`.
        """
        with pytest.raises(AssertionError):
            self.entity_dict[1] = {1: 'Foo'}

        self.entity_dict[2] = Foo()

    @patch('eventsourcing_helpers.models.EntityDict._get_child_entities')
    def test_get_all_entities(self, mock_entities):
        """
        Test that correct methods are invoked.
        """
        self.entity_dict._get_all_entities()
        mock_entities.assert_called_once()

    @patch('eventsourcing_helpers.models.Entity._get_all_entities')
    def test_get_child_entities(self, mock_entities):
        """
        Test that we get all entities in the current EntityDict.
        """
        mock_entities.return_value = [self.entity]
        entities = self.entity_dict._get_child_entities()

        assert list(entities) == [self.entity]
        mock_entities.assert_called_once()
