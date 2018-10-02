from unittest.mock import MagicMock

import pytest

from eventsourcing_helpers.models import AggregateRoot
from eventsourcing_helpers.repository import import_backend


@pytest.fixture(scope='function')
def aggregate_root_cls_mock():
    def aggregate_root_cls(attrs={}, exhaust_events=True, **kwargs):
        mock = MagicMock(spec=AggregateRoot, **kwargs)
        # since the events is a generator we must exhaust it to trigger
        # the call to `backend.load`.
        if exhaust_events:
            apply_events = lambda events, **kwargs: [e for e in events]
        else:
            apply_events = lambda events, **kwargs: events

        default_attrs = {
            'return_value._apply_events.side_effect': apply_events,
            'return_value.get_representation.return_value': 'adsafasf'}
        mock.configure_mock(**{**default_attrs, **attrs})
        return mock

    return aggregate_root_cls


@pytest.fixture(scope='function')
def importer_mock():
    def importer(return_value, **kwargs):
        mock = MagicMock(spec=import_backend, **kwargs)
        attrs = {'return_value': return_value}
        mock.configure_mock(**attrs)
        return mock

    return importer
