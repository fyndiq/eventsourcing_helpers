from unittest.mock import call, patch

import pytest

from eventsourcing_helpers.metrics import StatsdNullClient, call_counter


class StatsdNullClientTests:
    def setup_method(self):
        self.client = StatsdNullClient()

    def test_method_calls_should_not_fail(self):
        self.client.increment('foo', 1)
        self.client.foo('baz', 2)

    def test_timed_decorator_should_execute_decorated_method(self):
        @self.client.timed('foo', 1)
        def foo(a):
            return a

        value = foo('bar')
        assert value == 'bar'


class CallCounterTests:
    @patch('eventsourcing_helpers.metrics.statsd')
    def test_increments_total_count(self, mock_statsd):
        @call_counter('foo.count')
        def foo(a):
            pass

        foo('a')
        mock_statsd.increment.assert_called_once_with('foo.count.total')

    @patch('eventsourcing_helpers.metrics.statsd')
    def test_increments_counts_and_reraises_exception(self, mock_statsd):
        @call_counter('foo.count')
        def foo(a):
            1 / 0

        with pytest.raises(ZeroDivisionError):
            foo('a')
        expected_calls = [call('foo.count.total'), call('foo.count.error')]
        mock_statsd.increment.assert_has_calls(expected_calls)
