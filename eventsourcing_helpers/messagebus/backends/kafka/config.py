from collections import defaultdict
from copy import deepcopy


def get_producer_config(config):
    config = deepcopy(config)
    producer_config = config.get('producer', None)
    if not producer_config:
        return None

    producer_config.update(
        {
            'bootstrap.servers': config['bootstrap.servers'],
            'schema.registry.url': config['schema.registry.url']
        }
    )
    return producer_config


def get_consumer_config(config):
    config = deepcopy(config)
    consumer_config = config.get('consumer')
    if not consumer_config:
        return None

    consumer_config.pop('offset_watchdog', None)
    consumer_config.update(
        {
            'bootstrap.servers': config['bootstrap.servers'],
            'schema.registry.url': config['schema.registry.url']
        }
    )
    return consumer_config


def get_offset_watchdog_config(config):
    config = deepcopy(config)
    consumer_config = config.get('consumer', {})
    if not consumer_config:
        return None

    offset_wd_config = defaultdict(
        dict, consumer_config.get('offset_watchdog', {})
    )
    offset_wd_config['backend_config'].update(
        {'group.id': consumer_config['group.id']}
    )
    return offset_wd_config
