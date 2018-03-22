from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


class NullSnapshotBackend(SnapshotBackend):
    """
    Provide a dummy snapshot backend that does not do anything
    """

    def __init__(
        self, config: dict,
    ) -> None:
        pass

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        pass

    def _get_from_snapshot(self, aggregate_id: str) -> (str, str, int):  # noqa
        return None
