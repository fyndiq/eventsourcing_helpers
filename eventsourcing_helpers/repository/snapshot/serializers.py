from typing import Callable

import jsonpickle

from eventsourcing_helpers.models import AggregateRoot


def from_aggregate_root_to_snapshot(
    aggregate_root: AggregateRoot,
    current_hash: int,
    encoder: Callable = jsonpickle.encode
) -> dict:
    snapshot = {
        'data': encoder(aggregate_root),
        'version': aggregate_root._version,
        'hash': current_hash
    }
    return snapshot


def from_snapshot_to_aggregate_root(
    snapshot: dict, current_hash: int, decoder: Callable = jsonpickle.decode
) -> AggregateRoot:
    if not snapshot:
        return None

    data, hash = snapshot['data'], snapshot['hash']
    if data and current_hash == hash:
        return decoder(data)
    else:
        return None
