def get_producer_config(config):
    producer_config = config.get('producer', None)
    if not producer_config:
        return None

    producer_config.update({
        'bootstrap.servers': config['bootstrap.servers'],
        'schema.registry.url': config['schema.registry.url']
    })
    return producer_config


def get_consumer_config(config):
    consumer_config = config.get('consumer')
    if not consumer_config:
        return None

    consumer_config.update({
        'bootstrap.servers': config['bootstrap.servers'],
        'schema.registry.url': config['schema.registry.url']
    })
    return consumer_config
