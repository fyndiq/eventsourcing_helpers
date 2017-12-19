from collections import defaultdict


def get_producer_config(config):
    producer_config = config.get('producer')
    producer_config.update({
        'bootstrap.servers': config['bootstrap.servers'],
        'schema.registry.url': config['schema.registry.url']
    })
    return producer_config


def get_loader_config(config):
    loader_config = defaultdict(dict, config.get('loader'))
    loader_config.update({
        'bootstrap.servers': config['bootstrap.servers'],
        'schema.registry.url': config['schema.registry.url']
    })
    loader_config['consumer'].update({
        'bootstrap.servers': config['bootstrap.servers'],
        'schema.registry.url': config['schema.registry.url']
    })
    return loader_config
