from copy import deepcopy

from collections import defaultdict


def get_snapshot_config(config) -> dict:
    config = deepcopy(config)

    snapshot_config = defaultdict(
        dict, config.get('snapshot', {})
    )
    return snapshot_config
