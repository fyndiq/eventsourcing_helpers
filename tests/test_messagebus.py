from unittest.mock import MagicMock

from structlog.testing import capture_logs

from eventsourcing_helpers.messagebus import MessageBus


def test_message_bus_does_not_log_credentials():
    backend_config = {
        "producer": {
            "bootstrap.servers": "kafka.example.com:9092",
            "sasl.username": "avnadmin",
            "sasl.password": "s3cr3t-pw",
            "schema.registry.url": "https://avnadmin:s3cr3t-pw@registry.example.com:443",
        }
    }
    importer = MagicMock()

    with capture_logs() as logs:
        MessageBus(
            config={"backend": "some.Backend", "backend_config": backend_config}, importer=importer
        )

    log = next(entry for entry in logs if entry["event"] == "Using message bus backend")
    assert "s3cr3t" not in repr(log)
    assert log["backend"] == "some.Backend"
    assert log["config"] == {
        "producer": {
            "bootstrap.servers": "kafka.example.com:9092",
            "sasl.username": "avnadmin",
            "sasl.password": "***",
            "schema.registry.url": "https://avnadmin:***@registry.example.com:443",
        }
    }
    # the backend still receives the real, unredacted config
    importer.return_value.assert_called_once_with(config=backend_config)
