from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.models import AggregateRoot


BACKENDS = {
    'mongo': 'eventsourcing_helpers.repository.snapshots.backends.mongo.MongoSnapshotBackend',   # noqa
}


logger = structlog.get_logger(__name__)


class Snapshot:

    DEFAULT_BACKEND = 'mongo'

    def __init__(self, config: dict, importer: Callable=import_backend,
                 **kwargs) -> None:
        backend_path = config.get('backend', BACKENDS[self.DEFAULT_BACKEND])
        assert 'backend_config' in config, "You must pass a backend config"
        backend_config = config.get('backend_config')

        logger.info("Using snapshot backend", backend=backend_path,
                    config=backend_config)
        backend_class = importer(backend_path)
        self.backend = backend_class(backend_config, **kwargs)

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        logger.debug(
            'Saving aggregate as snapshot',
            id=aggregate.id
        )
        self.backend._save_snapshot(aggregate)

    def _get_from_snapshot(self, aggregate_id: str) -> AggregateRoot:
        logger.debug(
            'Trying to load aggregate from snapshot',
            id=aggregate_id
        )
        return self.backend._get_from_snapshot(aggregate_id)
