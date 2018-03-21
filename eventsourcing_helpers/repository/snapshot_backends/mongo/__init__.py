from typing import Callable

import jsonpickle

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository.snapshot_backends import SnapshotBackend


class MongoSnapshotBackend(SnapshotBackend):
    def __init__(
        self, config: dict,
        encoder: Callable = jsonpickle.encode,
        decoder: Callable = jsonpickle.decode,
    ) -> None:

        self.encoder = encoder
        self.decoder = decoder

        # Get Mongo URI etc

    def _save_snapshot(self, aggregate: AggregateRoot) -> None:
        pass

    def _get_from_snapshot(self, aggregate_id: str, aggregate: AggregateRoot) -> AggregateRoot:  # noqa
        return None
