from unittest.mock import Mock

from structlog.testing import capture_logs

from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog import OffsetWatchdog
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends.memory import (
    InMemoryOffsetWatchdogBackend,
)
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog.backends.null import (
    NullOffsetWatchdogBackend,
)


class OffsetWatchdogTests:
    def setup_method(self):
        self.old_message = self._create_message(8)
        self.current_message = self._create_message(88)
        self.future_message = self._create_message(888)

    def _create_message(self, offset: int):
        message = Mock()
        message._meta = Mock()
        message._meta.offset = offset
        message._meta.partition = 1
        message._meta.topic = "some-topic"
        return message

    def test_nulloffsetwatchdogbackend(self):
        backend = NullOffsetWatchdogBackend(config={"group.id": "consumer"})
        backend.set_seen(self.current_message)
        for message in (self.old_message, self.current_message, self.future_message):
            assert not backend.seen(message)

    def test_inmemoryoffsetwatchdogbackend(self):
        backend = InMemoryOffsetWatchdogBackend(config={"group.id": "consumer"})
        backend.set_seen(self.current_message)
        assert backend.seen(self.old_message)
        assert backend.seen(self.current_message)
        assert not backend.seen(self.future_message)

    def test_default_backend_configured(self):
        watchdog = OffsetWatchdog(config={"backend_config": {"group.id": "consumer"}})
        assert watchdog.backend


def test_offset_watchdog_does_not_log_credentials():
    backend_config = {"url": "redis://:s3cr3t-pw@redis.example.com:6379/0", "group.id": "consumer"}
    importer = Mock()

    with capture_logs() as logs:
        watchdog = OffsetWatchdog(
            config={"backend": "some.Backend", "backend_config": backend_config}, importer=importer
        )

    log = next(entry for entry in logs if entry["event"] == "Using offset watchdog backend")
    assert "s3cr3t" not in repr(log)
    assert log["config"] == {"url": "redis://:***@redis.example.com:6379/0", "group.id": "consumer"}
    # the backend and the watchdog still use the real, unredacted config
    importer.return_value.assert_called_once_with(config=backend_config)
    assert watchdog.config == backend_config
