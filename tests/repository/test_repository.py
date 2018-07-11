from functools import partial
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaException

from eventsourcing_helpers.repository import Repository


class RepositoryTests:
    aggregate_events = [1, 2, 3]
    config = {'backend_config': {}}

    @pytest.fixture(autouse=True)
    def setup_method(
        self, aggregate_root_cls_mock, snapshot_mock, repository_backend_mock,
        importer_mock
    ):
        self.aggregate_root_cls = aggregate_root_cls_mock()
        self.snapshot = snapshot_mock(return_value=None)
        self.repository_backend = repository_backend_mock(
            return_value=self.aggregate_events
        )
        self.importer = importer_mock(return_value=self.repository_backend)
        self.message_deserializer = lambda e, *args, **kwargs: e

        self.repository = partial(
            Repository, config=self.config,
            aggregate_root_cls=self.aggregate_root_cls, importer=self.importer,
            message_deserializer=self.message_deserializer,
            snapshot=self.snapshot
        )

    def test_should_load_aggr_root_from_event_storage(self):
        repository = self.repository()
        aggregate_root = repository.load(id=1)

        assert repository.snapshot.load.called is True
        assert repository.snapshot.load.return_value is None
        assert repository.backend.get_events.called is True
        assert aggregate_root is not None

    def test_should_load_aggr_root_from_snapshot_storage(self, snapshot_mock):
        snapshot = snapshot_mock(return_value=Mock())
        repository = self.repository(snapshot=snapshot)
        aggregate_root = repository.load(id=1)

        assert repository.snapshot.load.called is True
        assert repository.backend.get_events.called is False
        assert aggregate_root is not None

    def test_should_apply_events_when_loading_from_event_storage(
        self, aggregate_root_cls_mock
    ):
        aggregate_root_cls = aggregate_root_cls_mock(exhaust_events=False)
        repository = self.repository(aggregate_root_cls=aggregate_root_cls)
        aggregate_root = repository.load(id=1)

        args, kwargs = aggregate_root._apply_events.call_args
        events, *_ = args

        assert aggregate_root._apply_events.called is True
        assert list(events) == self.aggregate_events

    def test_repository_commit_should_call_backend_and_snapshot(
        self, aggregate_root_cls_mock
    ):
        aggregate_root_cls = aggregate_root_cls_mock(exhaust_events=False)
        aggregate_root_cls.id = 1

        repository = self.repository(aggregate_root_cls=aggregate_root_cls)
        repository.commit(aggregate_root_cls)

        assert repository.backend.commit.called is True
        assert repository.snapshot.save.called is True

    def test_repository_commit_should_delete_snapshot_on_kafka_exception(
        self, aggregate_root_cls_mock
    ):
        aggregate_root_cls = aggregate_root_cls_mock(exhaust_events=False)
        aggregate_root_cls.id = 1

        repository = self.repository(aggregate_root_cls=aggregate_root_cls)
        repository.backend.commit.side_effect = KafkaException

        with pytest.raises(KafkaException):
            repository.commit(aggregate_root_cls)

            assert repository.snapshot.save.called is True
            assert repository.snapshot.delete.called is True
