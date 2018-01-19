class Null:
    # __call__ = __getattr__ = lambda self, *_, **__: self
    def __init__(self, *args, **kwargs):
        import ipdb; ipdb.set_trace()
        pass

    def __call__(self, *args, **kwargs):
        import ipdb; ipdb.set_trace()
        return self

    def __getattr__(self, name):
        import ipdb; ipdb.set_trace()
        return self


class StatsdWrapper:
    def __init__(self, client):
        self._wrapped = client
        self._tags = []

    def _get_tags(self, tags):
        return list(set(tags + self._tags)) if tags else self._tags

    def timed(self, *args, **kwargs):
        tags = self._get_tags(kwargs.pop('tags', None))
        return self._wrapped.timed(*args, **kwargs, tags=tags)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


# try:
#     import datadog
#     statsd = StatsdWrapper()
# except ModuleNotFoundError:
#     statsd = Null()


if __name__ == "__main__":
    s = StatsdWrapper(Null())

    @s.timed('heheh')
    def foo(a, b):
        import ipdb; ipdb.set_trace()
        print("Foo")

    foo(1, 2)
