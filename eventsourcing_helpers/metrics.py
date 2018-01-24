from functools import wraps


class StatsdNullClient:
    """
    No-op datadog statsd client implementing the null object pattern.
    """
    __call__ = __getattr__ = lambda self, *_, **__: self

    @staticmethod
    def decorator(f):
        def _decorator(*args, **kwargs):
            return f(*args, **kwargs)
        return _decorator

    def timed(self, *args, **kwargs):
        return StatsdNullClient.decorator


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


if __name__ == "__main__":
    class Foo:
        @call_counter('metric')
        def foo(self, a, b):
            1 / 0
            print(a, b)

    f = Foo()
    f.foo(1, 2)
