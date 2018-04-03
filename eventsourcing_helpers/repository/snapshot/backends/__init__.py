import structlog

from eventsourcing_helpers.models import AggregateRoot

logger = structlog.get_logger(__name__)


class SnapshotBackend:
    def save(self, id: str, aggregate_root: AggregateRoot) -> None:
        raise NotImplementedError()

    def load(self, id: str) -> dict:
        raise NotImplementedError()
