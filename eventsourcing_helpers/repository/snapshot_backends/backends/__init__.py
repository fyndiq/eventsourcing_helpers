from eventsourcing_helpers.models import AggregateRoot


class SnapshotBackend:
    """
    Repository interface.
    """

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        raise NotImplementedError()

    def _get_from_snapshot(self, aggregate_id: str) -> AggregateRoot:
        raise NotADirectoryError()
