from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


class NullSnapshotBackend(SnapshotBackend):
    """
    Provide a dummy snapshot backend that does not do anything
    """
    def __init__(self, *args, **kwargs) -> None:
        pass

    def save(self, id: str, aggregate_root: AggregateRoot) -> None:
        pass

    def load(self, id: str) -> None:
        return None
