from typing import Type

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import Repository


class AggregateBuilder:
    def __init__(
        self, config, aggregate_root_cls: AggregateRoot, repository: Type[Repository] = Repository,
        **kwargs
    ) -> None:
        self.repository = repository(
            config, aggregate_root_cls=aggregate_root_cls, ignore_missing_apply_methods=True,
            **kwargs
        )

    def rebuild(self, id: str, max_offset: int = None) -> AggregateRoot:
        """
        Rebuild aggregate from the repository.

        Max offset is used to make sure we load the aggregate up until the point
        where the event was created.

        Args:
            id: ID of the aggregate root to load.
            max_offset: Stop loading events at this position.
        """
        aggregate = self.repository.load(id, max_offset=max_offset)

        return aggregate
