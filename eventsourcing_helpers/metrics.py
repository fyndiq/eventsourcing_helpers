from functools import wraps
from os import getenv

base_metric = "eventsourcing_helpers"


class StatsdNullClient:
    """
    No-op datadog statsd client implementing the null object pattern.
    """

    __call__ = __getattr__ = lambda self, *_, **__: self

    def timed(self, *args, **kwargs):
        return TimedNullDecorator()


class TimedNullDecorator:
    __enter__ = __getattr__ = lambda self, *_, **__: self

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def __exit__(self, *args):
        pass


try:
    if getenv("DATADOG_ENABLE_METRICS") != "1":
        statsd = StatsdNullClient()  # pragma: no cover; # type: ignore
    else:
        import datadog  # pragma: no cover

        statsd = datadog.statsd  # pragma: no cover; # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    statsd = StatsdNullClient()  # pragma: no cover; # type: ignore


def call_counter(metric_name):
    def wrapped(f):
        @wraps(f)
        def decorator(*args, **kwargs):
            statsd.increment(f"{metric_name}.total")
            try:
                return f(*args, **kwargs)
            except Exception:
                statsd.increment(f"{metric_name}.error")
                raise

        return decorator

    return wrapped


def timed(metric_name, tags=None):
    if tags is None:
        tags = []

    def wrapped(f):
        @wraps(f)
        def decorator(*args, **kwargs):
            with statsd.timed(f"{metric_name}.time", tags=tags):
                return f(*args, **kwargs)

        return decorator

    return wrapped
