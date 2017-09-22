from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.repository.backends.base import RepositoryBackend


class KafkaBackend(RepositoryBackend):

    def __init__(self, config, producer=AvroProducer, loader=AvroMessageLoader):
        self.producer = producer(config.get('producer'))
        self.loader = loader(config.get('loader'))

    def commit(self, guid, events, **kwargs):
        for event in events:
            self.producer.produce(key=guid, value=event, **kwargs)

    def load(self, guid, *args, **kwargs):
        return self.loader.load(guid, *args, **kwargs)
