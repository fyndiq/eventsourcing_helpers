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

    def save_snapshot(self, aggregate_id: str, pickled_data: str,
                      aggregate_version: str, aggregate_hash: int) -> None:
        pass

    def get_from_snapshot(self, aggregate_id: str) -> (str, str, int):
        return (None, None, 0)
