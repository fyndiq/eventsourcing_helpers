from eventsourcing_helpers.repository.snapshot.backends import SnapshotBackend


class NullSnapshotBackend(SnapshotBackend):
    """
    Provide a dummy snapshot backend that does not do anything
    """
    def __init__(self, *args, **kwargs) -> None:
        pass

    def save(self, id: str, data: dict) -> None:
        pass

    def load(self, id: str) -> None:
        return None

    def delete(self, id: str) -> None:
        pass
