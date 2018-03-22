from eventsourcing_helpers.models import AggregateRoot


def serialize_data(data, version: str, aggregate_hash: int) -> dict:
    snapshot_data = {}

    snapshot_data['data'] = data
    snapshot_data['version'] = version
    snapshot_data['hash'] = aggregate_hash
    return snapshot_data


def deserialize_data(data: dict) -> (AggregateRoot, str, int):
    return (data['data'], data['version'], data['hash'])
