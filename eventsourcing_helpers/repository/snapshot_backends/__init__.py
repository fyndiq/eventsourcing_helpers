from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.models import AggregateRoot


logger = structlog.get_logger(__name__)


class Snapshot:

    def __init__(self, config: dict, importer: Callable=import_backend,
                 **kwargs) -> None:
        pass

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        logger.debug(
            'Saving aggregate as snapshot',
            id=aggregate.id
        )
        self.snapshot_backend._save_snapshot(aggregate)

    def _get_from_snapshot(self, aggregate_id: str) -> AggregateRoot:
        logger.debug(
            'Trying to load aggregate from snapshot',
            id=aggregate_id
        )
        return self.snapshot_backend._get_from_snapshot(aggregate_id)
