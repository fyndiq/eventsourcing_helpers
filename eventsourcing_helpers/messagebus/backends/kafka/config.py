from collections import defaultdict
from copy import deepcopy

TOP_LEVEL_PRODUCER_CONFIG_KEYS = (
    'bootstrap.servers',
    'schema.registry.url',
    'security.protocol',
    'sasl.mechanisms',
    'sasl.username',
    'sasl.password',
    'ssl.ca.location',
)

TOP_LEVEL_CONSUMER_CONFIG_KEYS = (
    'bootstrap.servers',
    'schema.registry.url',
    'security.protocol',
    'sasl.mechanisms',
    'sasl.username',
    'sasl.password',
    'ssl.ca.location',
)


def get_producer_config(config):
    config = deepcopy(config)
    producer_config = config.get('producer', None)
    if not producer_config or not producer_config.get('enabled', True):
        return None

    for key in TOP_LEVEL_PRODUCER_CONFIG_KEYS:
        if key in config:
            producer_config.update({f'{key}': config[key]})
    return producer_config


def get_consumer_config(config):
    config = deepcopy(config)
    consumer_config = config.get('consumer')
    if not consumer_config or not consumer_config.get('enabled', True):
        return None

    consumer_config.pop('offset_watchdog', None)
    for key in TOP_LEVEL_CONSUMER_CONFIG_KEYS:
        if key in config:
            consumer_config.update({f'{key}': config[key]})
    return consumer_config


def get_offset_watchdog_config(config):
    config = deepcopy(config)
    consumer_config = config.get('consumer', {})
    if not consumer_config:
        return None

    offset_wd_config = defaultdict(dict, consumer_config.get('offset_watchdog', {}))
    offset_wd_config['backend_config'].update({'group.id': consumer_config['group.id']})
    return offset_wd_config
