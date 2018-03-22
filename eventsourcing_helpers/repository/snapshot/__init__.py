from typing import Callable

import structlog

from eventsourcing_helpers.utils import import_backend
from eventsourcing_helpers.repository.snapshot.config import get_snapshot_config  # noqa

BACKENDS = {
    'null': 'eventsourcing_helpers.repository.snapshot.backends.null.NullSnapshotBackend',   # noqa
}

logger = structlog.get_logger(__name__)


class Snapshot:
    """
    Interface to communicate with a snapshot backend.

    The interface provides methods for saving and loading a snapshot
    """
    DEFAULT_BACKEND = 'null'

    def __init__(self, config: dict,
                 importer: Callable=import_backend,
                 **kwargs) -> None:  # yapf: disable
        import ipdb; ipdb.set_trace()
        snapshot_config = get_snapshot_config(config)
        snapshot_backend_path = snapshot_config.get(
            'backend', BACKENDS[self.DEFAULT_BACKEND])
        snapshot_backend_config = snapshot_config.get('backend_config', '')

        logger.info("Using snapshot backend", backend=snapshot_backend_path,
                    config=snapshot_backend_config)
        snapshot_backend_class = importer(snapshot_backend_path)
        self.snapshot_backend = snapshot_backend_class(
            snapshot_backend_config, **kwargs)


