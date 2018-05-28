from functools import wraps

base_metric = 'eventsourcing_helpers'


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
    import datadog
    statsd = datadog.statsd
except ModuleNotFoundError:
    statsd = StatsdNullClient()


def call_counter(base_metric):
    def wrapped(f):
        @wraps(f)
        def decorator(*args, **kwargs):
            statsd.increment(f"{base_metric}.total")
            try:
                return f(*args, **kwargs)
            except Exception:
                statsd.increment(f"{base_metric}.error")
                raise
        return decorator
    return wrapped


def timed(base_metric, tags=None):
    if tags is None:
        tags = []

    def wrapped(f):
        @wraps(f)
        def decorator(*args, **kwargs):
            with statsd.timed(f"{base_metric}.time", tags=tags):
                return f(*args, **kwargs)
        return decorator
    return wrapped
