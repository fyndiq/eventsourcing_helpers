def snapshot_serializer(data, version: str, aggregate_hash: str) -> dict:
    snapshot_data = {}

    snapshot_data['data'] = data
    snapshot_data['version'] = version
    snapshot_data['hash'] = aggregate_hash
    return snapshot_data
