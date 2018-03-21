from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.models import AggregateRoot


logger = structlog.get_logger(__name__)


class Snapshot:

    def __init__(self, config: dict, importer: Callable=import_backend,
                 **kwargs) -> None:
        raise NotImplementedError()

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        raise NotImplementedError()

    def _get_from_snapshot(self, aggregate_id: str) -> AggregateRoot:
        raise NotImplementedError()
