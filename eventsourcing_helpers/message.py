from typing import Callable, NamedTuple


class Message:

    def __init__(self, cls: NamedTuple, **kwargs) -> None:
        self.__dict__['message'] = cls(**kwargs)  # type: ignore

    @property
    def _name(self) -> str:
        return self.message.__class__.__name__

    def to_dict(self) -> dict:
        return dict(self.message._asdict())  # type: ignore

    def __repr__(self) -> str:
        return repr(self.message)

    def __getattr__(self, name: str) -> Callable:
        return getattr(self.message, name)

    def __setattr__(self, *args) -> None:
        raise AttributeError("Messages are read only")


def message_factory(cls: NamedTuple) -> type:
    """
    Class decorator used for "enriching" a namedtuple object with some common
    features.

    Currently namedtuples doesn't support inheritance from a base class - which
    means we can't easily add common methods/properties for a message.

    See: https://github.com/python/typing/issues/427.

    To work around this we can wrap the namedtuple in a "proxy class" which
    adds all the common features and redirects all attribute lookups to the
    underlying namedtuple.
    """

    class MessageProxy(Message):

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(cls, **kwargs)

    # create a new proxy class that inherits from MessageProxy
    proxy = type(cls.__name__, (MessageProxy, object), {})  # type: ignore
    return proxy


event = lambda cls: message_factory(cls)
command = lambda cls: message_factory(cls)
