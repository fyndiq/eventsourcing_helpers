from typing import Callable, Union

import jsonpickle

from eventsourcing_helpers.models import AggregateRoot


def from_aggregate_root_to_snapshot(
    aggregate_root: AggregateRoot,
    current_hash: int,
    encoder: Callable = jsonpickle.encode
) -> dict:
    """
    Serializes an aggregate root into a format suitable for snapshot storage

    Args:
        aggregate_root: The aggregate root to be saved
        current_hash: The hash of the aggregate_root schema
        encoder (optional): The function that encodes the aggregate
            root into data for storage

    Returns:
        dict: The data blob to be stored in the snapshot storage

    Example:
    >>> from_aggregate_root_to_snapshot(aggregate, 123456)
    {
        'data': '{"py/object": "eventsourcing_helpers.models.AggregateRoot",
        "_version": 0, "id": null}', 'version': 0, 'hash': 123456
    }

    """
    snapshot = {
        'data': encoder(aggregate_root),
        'version': aggregate_root._version,
        'hash': current_hash
    }
    return snapshot


def from_snapshot_to_aggregate_root(
    snapshot: dict, current_hash: int, decoder: Callable = jsonpickle.decode
) -> Union[AggregateRoot, None]:
    """Converts the snapshot data into an AggregateRoot (or child)

    Args:
        snapshot: the snapshot data
        current_hash: The current hash of the schema of the object to be
            created (AggregateRoot or child)
        decoder (optional): The decoder of the snapshot data

    Returns:
        AggregateRoot: The restored object

    Example:
    >>> from_snapshot_to_aggregate_root(snapshot, 123456)
    AggregateRoot({'_version': 0})
    """

    if not snapshot:
        return None

    data, hash = snapshot['data'], snapshot['hash']
    if data and current_hash == hash:
        return decoder(data)
    else:
        return None
