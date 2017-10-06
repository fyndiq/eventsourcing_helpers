import re
import uuid
from itertools import chain
from typing import Any, Callable, Iterator, List

import structlog

word_regexp = re.compile('[A-Z][^A-Z]*')
logger = structlog.get_logger(__name__)


class Entity:
    """
    A rich domain model that exposes attributes and behaviour
    with an identity and a lifecycle.

    An entity may only hold references to local models.
    """
    # a shared list between all entities with staged events
    # that later will be committed to the repository.
    _events: List[Any] = []

    def __init__(self) -> None:
        self.guid: bool = None
        self._version: int = 0

    def __call__(self, *args, **kwargs):
        # fixes https://github.com/python/mypy/issues/2113
        super().__call__(*args, **kwargs)

    def __repr__(self) -> str:
        attrs = {k: v for k, v in self.__dict__.items() if v is not None}
        return f"{self.name}({attrs})"

    @property
    def name(self):
        return self.__class__.__name__

    def _get_apply_method(self, entity: Any, method_name: str) -> Callable:
        """
        Get apply method on an entity.

        Args:
            entity: Entity to look on.
            method_name: Apply method name to look for.

        Returns:
            method: Callback to apply method.
        """
        return getattr(entity, method_name)

    def _apply_event(self, event: Any, entity: 'Entity', method_name,
                     is_new) -> None:
        """
        Apply an event on one entity.

        Args:
            event: Event to be applied.
            entity: Entity to apply the event on.
            method_name: Apply method on the entity.
            is_new: Flag to indicate if the event should be staged.
        """
        log = logger.bind(
            guid=event.guid, event_class=event.__class__.__name__,
            entity_class=entity.name
        )
        try:
            apply_method = self._get_apply_method(entity, method_name)
            # TODO: apply the event in the aggregate root if it's defined.
        except AttributeError:
            log.error("Missing event apply method", method=method_name)
        else:
            log.info("Applying event", is_new=is_new)
            apply_method(event)
            self._stage_event(event, is_new)

    def _stage_event(self, event: Any, is_new: bool) -> None:
        """
        Stage an event.

        All staged events in the list will later be committed to
        the repository.

        Args:
            event: Event to be staged.
            is_new: Flag that indicates if the event should be staged.
        """
        if is_new:
            event_name = event.__class__.__name__
            logger.info("Staging event", event_name=event_name)
            self._events.append(event)

    def _get_child_entities(self) -> Iterator['Entity']:
        """
        Get all child entities for the current instance
        including all instances from all EntityDict's.

        Returns:
            iterable: A list with all child entities.
        """
        entities = [
            e._get_all_entities() for e in self.__dict__.values()
            if isinstance(e, (Entity, EntityDict))
        ]
        return chain.from_iterable(entities)

    def _get_all_entities(self) -> Iterator['Entity']:
        """
        Get the current instance and all child entities.

        Returns:
            iterable: A list with all entities.
        """
        return chain([self], self._get_child_entities())

    def _get_entity(self, guid: str) -> 'Entity':
        """
        Find and return an entity instance with the given guid.

        If no entity are found we return the current instance.
        This is a normal situation when an child entity has not yet
        been created by the parent.

        TODO: This will probably not work if we have more then two
        levels of entities. We should always return the child entities
        parent, not aggregate root.

        Args:
            guid: The guid of the entity.

        Returns:
            Entity: Found entity or current instance.
        """
        entities = self._get_all_entities()
        return next((e for e in entities if e.guid == guid), self)

    def _get_apply_method_name(self, event_name: str) -> str:
        """
        Gets the apply method name for a given event.

        Args:
            event_name: Name of the event. Probably the class
                name of the event.

        Returns:
            str: Name of the apply method.

        Example:
            >>> event = FooEvent(guid=1)
            >>> Entity._get_apply_method_name(event.__class__.__name__)
            'apply_foo_event'
        """
        words = re.findall(word_regexp, event_name)
        lowered_words = list(map(str.lower, words))
        apply_method_name = 'apply_' + '_'.join(lowered_words)

        return apply_method_name

    def create_guid(self) -> str:
        """
        Returns an unique guid as a string.
        """
        return str(uuid.uuid4())

    def clear_staged_events(self) -> None:
        """
        Clear staged events from ALL Entity instances.
        """
        logger.info("Clearing staged events")
        Entity._events = []

    def apply_events(self, events: List[Any]) -> None:
        """
        Apply multiple events.

        Args:
            events: A list of events.
        """
        logger.info("Apply events from repository")
        for event in events:
            self.apply_event(event, is_new=False)

    def apply_event(self, event: Any, is_new: bool=True) -> None:
        """
        Applies an event by calling the correct entity apply method.

        The apply methods performs all state changes in the aggregate.

        Args:
            event: Event to be applied.
            is_new: Flag that indicates if the event should be staged
                (haven't yet been committed).
        """
        event_name = event.__class__.__name__
        apply_method_name = self._get_apply_method_name(event_name)
        entity = self._get_entity(event.guid)

        self._apply_event(event, entity, apply_method_name, is_new)


class EntityDict(dict):
    """
    A collection of domain entities implemented as a dict to allow
    fast lookup by a key.
    """
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.values()})"

    def __setitem__(self, key: str, value: Entity) -> None:
        assert isinstance(value, Entity)
        super().__setitem__(key, value)

    def _get_child_entities(self) -> Iterator[Entity]:
        """
        Get all child entities.

        Returns:
            iterable: A list of all child entities.
        """
        entities = [
            e._get_all_entities() for e in self.values()
            if isinstance(e, (Entity, EntityDict))
        ]
        return chain.from_iterable(entities)

    def _get_all_entities(self) -> Iterator[Entity]:
        """
        Get all entities.

        This method only exist so the call in `_get_child_entities`
        methods (Entity, EntityDict) does not break.

        Returns:
            iterable: A list of all child entities.
        """
        return self._get_child_entities()


class AggregateRoot(Entity):
    """
    The aggregate root represents a single entity and acts as a gateway for all
    modifications within the aggregate.

    The aggregate root may or may not contain an object graph which represents
    a logical cohesive group of domain models (entities, value objects).

    The aggregate root is meant to be used as a facade which means that
    all commands must go through it. This gives us the flexibility to run
    business logic in multiple models in the same command.
    """
    pass
