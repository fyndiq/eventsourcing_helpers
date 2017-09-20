from collections import namedtuple

import pytest
from mock import Mock, patch

from eventsourcing_helpers.models import AggregateRoot, Entity, EntityDict

FooEvent = namedtuple('FooEvent', 'guid')


class Foo(AggregateRoot):
    pass


class Bar(Entity):
    pass


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
        self.event = FooEvent(guid=1)

    @patch('eventsourcing_helpers.models.Entity._get_apply_method_name')
    @patch('eventsourcing_helpers.models.Entity._get_entity')
    @patch('eventsourcing_helpers.models.Entity._apply_event')
    def test_apply_event(self, mock_event, mock_entity, mock_apply):
        """
        Test that correct methods are invoked when applying an event.
        """
        mock_apply.return_value = 'apply_foo_event'
        is_new = False

        self.aggregate_root.apply_event(self.event)

        mock_apply.called_once_with(self.event.__class__.__name__)
        mock_entity.called_once_with(self.event.guid)
        mock_event.called_once_with( self.event, self.aggregate_root,
                                    'apply_foo_event', is_new)

    @patch('eventsourcing_helpers.models.Entity.apply_event')
    def test_apply_events(self, mock_apply):
        """
        Test that the apply method is run multiple times.
        """
        num_events = 4
        events = [FooEvent(guid=i) for i in range(0, num_events)]

        self.aggregate_root.apply_events(events)
        assert mock_apply.call_count == num_events

    def test_clear_events(self):
        """
        Test that the committed events are cleared from ALL
        `Entity` instances.
        """
        self.aggregate_root._events.append(self.event)
        self.entity._events.append(self.event)

        assert len(self.aggregate_root._events) == 2
        self.aggregate_root.clear_staged_events()

        assert len(self.aggregate_root._events) == 0
        assert len(self.entity._events) == 0

    def test_create_guid(self):
        """
        Test that the returned guid is a string and with correct length.
        """
        guid = self.entity.create_guid()

        assert isinstance(guid, str)
        assert len(guid) == 36

    def test_get_apply_method_name(self):
        """
        Test that we get the correct apply method name for an event.
        """
        apply_method_name = self.entity._get_apply_method_name(
            self.event.__class__.__name__
        )
        assert apply_method_name == 'apply_foo_event'

    @patch('eventsourcing_helpers.models.Entity._get_all_entities')
    def test_get_entity(self, mock_entities):
        """
        Test that the correct entity is returned.
        """
        self.aggregate_root.guid = 1
        self.entity.guid = 2

        entities = [self.aggregate_root, self.entity]
        mock_entities.return_value = entities
        entity = self.aggregate_root._get_entity(self.entity.guid)
        assert entity == self.entity

        entities.pop()
        mock_entities.return_value = entities
        entity = self.aggregate_root._get_entity(self.entity.guid)
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
