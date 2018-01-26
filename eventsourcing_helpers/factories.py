from typing import Any, Callable, Generator

from eventsourcing_helpers.models import Entity
from eventsourcing_helpers.repository import Repository
from eventsourcing_helpers.serializers import from_message_to_dto


class ESEntityFactory:
    """
        This class is responsible for loading the messages and
        return the aggregate in the requested state.

        >> factory = ESEntityFactory(repository_config=config)
        >> factory.rebuild(entity_class=Order, id="123131323132")
        Order(id="123131323132", state="completed", ...)
    """
    def __init__(self, repository_config: dict,
                 message_deserializer: Callable=from_message_to_dto,
                 repository: Any=Repository) -> None:  # yapf: disable

        self.message_deserializer = message_deserializer
        self.repository = repository(repository_config)

    def rebuild(self, entity_class: Entity, id: str, max_offset=None) -> Entity:  # noqa
        """
        Rebuild the entity state based on the events on the repository.

        Args:
            entity_class: Class used to apply the events
            id: entity id used to load the events

        Returns:
            entity: rebuilt entity
        """
        entity = entity_class()
        entity._apply_events(self._get_events(id, max_offset=max_offset))
        return entity

    def _get_events(self, id: str, max_offset: int=None) -> Generator[Any, None, None]:  # noqa
        """
        Get events for the given id and stop in the max offset.
        """
        with self.repository.load(id) as events:
            for event in events:
                if max_offset is not None and event._meta.offset > max_offset:
                    break
                yield self.message_deserializer(event)
