from eventsourcing_helpers.repository.backends.kafka import config


class KafkaConfigTests:

    def setup_method(self):
        self.config = {
            "bootstrap.servers": "foo",
            "schema.registry.url": "bar",
            "producer": {"a": 1},
            "loader": {"b": 2, "consumer": {"c": 3}},
        }

    def test_get_producer_config(self):
        """
        Test that we get a correct producer config.
        """
        producer_config = config.get_producer_config(self.config)
        expected_config = {"bootstrap.servers": "foo", "schema.registry.url": "bar", "a": 1}
        assert producer_config == expected_config

    def test_get_loader_config(self):
        """
        Test that we get a correct loader config.
        """
        loader_config = config.get_loader_config(self.config)
        expected_config = {
            "bootstrap.servers": "foo",
            "schema.registry.url": "bar",
            "b": 2,
            "consumer": {"bootstrap.servers": "foo", "schema.registry.url": "bar", "c": 3},
        }
        assert loader_config == expected_config
