import structlog

logger = structlog.get_logger(__name__)


class SnapshotBackend:
    def save(self, id: str, data: dict) -> None:
        raise NotImplementedError()

    def load(self, id: str) -> dict:
        raise NotImplementedError()

    def delete(self, id: str) -> None:
        raise NotImplementedError()
