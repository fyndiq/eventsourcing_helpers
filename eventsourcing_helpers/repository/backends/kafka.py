from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer


class KafkaBackend:

    def __init__(self, config):
        self.producer = AvroProducer(config.get('producer'))
        self.loader = AvroMessageLoader(config.get('loader'))

    def commit(self, aggregate, **kwargs):
        for event in aggregate._events:
            self.producer.produce(key=aggregate.guid, value=event, **kwargs)

    def load(self, key, *args, **kwargs):
        return self.loader.load(key, *args, **kwargs)
