from collections import defaultdict
from copy import deepcopy


def get_producer_config(config):
    config = deepcopy(config)
    producer_config = config.get('producer')
    if not producer_config or not producer_config.get('enabled', True):
        return None

    producer_config.update({
        'bootstrap.servers': config.get('bootstrap.servers'),
        'schema.registry.url': config.get('schema.registry.url')
    })
    return producer_config


def get_loader_config(config):
    config = deepcopy(config)
    loader_config = config.get('loader')
    if not loader_config or not loader_config.get('enabled', True):
        return None

    loader_config = defaultdict(dict, loader_config)
    loader_config.update({
        'bootstrap.servers': config.get('bootstrap.servers'),
        'schema.registry.url': config.get('schema.registry.url')
    })
    loader_config['consumer'].update({
        'bootstrap.servers': config.get('bootstrap.servers'),
        'schema.registry.url': config.get('schema.registry.url')
    })
    return loader_config
