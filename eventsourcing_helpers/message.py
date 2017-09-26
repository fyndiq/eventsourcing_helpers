from typing import NamedTuple


class Message:

    def __init__(self, cls, *args, **kwargs):
        assert not isinstance(cls, NamedTuple)
        self.__dict__['message'] = cls(**kwargs)

    @property
    def _name(self):
        return self.message.__class__.__name__

    def to_dict(self):
        return self.message._asdict()

    def __repr__(self):
        return repr(self.message)

    def __getattr__(self, name):
        return getattr(self.message, name)

    def __setattr__(self, *args):
        raise AttributeError("Messages are read only")


def message_factory(cls):
    class WrappedMessage(Message):
        def __init__(self, *args, **kwargs):
            super().__init__(cls, *args, **kwargs)

    wrapped = type(cls.__name__, (WrappedMessage, object), {})
    return wrapped


event = lambda cls: message_factory(cls)
command = lambda cls: message_factory(cls)
