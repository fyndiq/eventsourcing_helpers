from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend


logger = structlog.get_logger(__name__)


class SnapshotBackend:

    def __init__(self, config: dict, importer: Callable=import_backend,
                 **kwargs) -> None:
        raise NotImplementedError()

    def save_snapshot(self, aggregate_id: str, pickled_data: str,
                      aggregate_version: str, aggregate_hash: int) -> None:
        raise NotImplementedError()

    def get_from_snapshot(self, aggregate_id: str) -> (str, str, int):
        raise NotImplementedError()
