import re
import uuid
from itertools import chain

from eventsourcing_helpers import logger

word_regexp = re.compile('[A-Z][^A-Z]*')


class Entity:

    _events = []

    def __init__(self):
        self.guid = None
        self._version = 0

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def _get_child_entities(self):
        entities = [
            e._get_all_entities() for e in self.__dict__.values()
            if isinstance(e, (Entity, EntityDict))
        ]
        return chain.from_iterable(entities)

    def _get_all_entities(self):
        return chain([self], self._get_child_entities())

    def _get_entity(self, guid):
        entities = self._get_all_entities()
        return next((e for e in entities if e.guid == guid), self)

    def create_guid(self):
        return str(uuid.uuid4())

    def apply_events(self, events):
        logger.info("Apply events from repository")
        for event in events:
            self.apply(event, is_new=False)

    def clear_commited_events(self):
        logger.info("Clearing committed events")
        # clear events from all `Entity` instances
        Entity._events = []

    def apply(self, event, is_new=True):
        event_class = event.__class__.__name__

        words = re.findall(word_regexp, event_class)
        words = map(str.lower, words)
        apply_method_name = 'apply_' + '_'.join(words)

        entity = self._get_entity(event.guid)
        entity_class = entity.__class__.__name__
        log = logger.bind(
            guid=event.guid, event_class=event_class, entity_class=entity_class
        )

        try:
            apply_method = getattr(entity, apply_method_name)
        except AttributeError:
            log.error("Missing event apply method", method=apply_method_name)
        else:
            log.info("Applying event", is_new=is_new)
            apply_method(event)
            if is_new:
                self._events.append(event)


class EntityDict(dict):

    def __repr__(self):
        return f"{self.__class__.__name__}({self.values()})"

    def __setitem__(self, key, value):
        assert isinstance(value, Entity)
        super().__setitem__(key, value)

    def _get_child_entities(self):
        entities = [
            e._get_all_entities() for e in self.values()
            if isinstance(e, (Entity, EntityDict))
        ]
        return chain.from_iterable(entities)

    def _get_all_entities(self):
        return self._get_child_entities()


class AggregateRoot(Entity):
    pass
