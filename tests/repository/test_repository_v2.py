from functools import partial
from unittest.mock import Mock, MagicMock

import pytest

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
        assert repository.backend.load.called is True
        assert aggregate_root is not None

    def test_should_load_aggr_root_from_snapshot_storage(self, snapshot_mock):
        snapshot = snapshot_mock(return_value=Mock())
        repository = self.repository(snapshot=snapshot)
        aggregate_root = repository.load(id=1)

        assert repository.snapshot.load.called is True
        assert repository.backend.load.called is False
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

    def test_should_deserialize_events_when_loading_from_event_storage(self):
        repository = self.repository(message_deserializer=Mock())
        repository.load(id=1)

        for event in self.aggregate_events:
            repository.message_deserializer.assert_any_call(event, is_new=False)  # noqa

    def test_hash_gets_model_representation(self, aggregate_root_cls_mock):
        aggregate_root_cls = aggregate_root_cls_mock(exhaust_events=False)
        aggregate_root_cls()._get_model_representation.return_value = 'a'
        repository = self.repository(aggregate_root_cls=aggregate_root_cls)
        repository.get_schema_hash()

        aggregate_root_cls()._get_model_representation.assert_called_once_with()
