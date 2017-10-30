from typing import Callable, NamedTuple


class Message:

    def __init__(self, message: NamedTuple, **kwargs) -> None:
        self.__dict__['message'] = message(**kwargs)  # type: ignore

    @property
    def _class(self) -> str:
        return self.message.__class__.__name__

    def to_dict(self) -> dict:
        items = self.message._asdict().items()  # type: ignore
        filtered = {k: v for k, v in items if v is not None}
        return filtered

    def __repr__(self) -> str:
        return repr(self.message)

    def __getattr__(self, name: str) -> Callable:
        return getattr(self.message, name)

    def __setattr__(self, *args) -> None:
        raise AttributeError("Messages are read only")


def message_factory(message: NamedTuple) -> type:
    """
    Class decorator used for constructing messages.

    A message is basically a namedtuple extended with some additional features.

    Currently namedtuples doesn't support inheritance from a base class - which
    means we can't easily add additional methods/properties.

    See: https://github.com/python/typing/issues/427.

    To work around this we can wrap the namedtuple in a "proxy class" which
    adds all the additional features and redirects all attribute lookups to the
    underlying namedtuple.

    Example:
        >>> @Event
        ... class OrderCreated(NamedTuple):
        ...     guid: str
        ...     state: str
        >>> event = OrderCreated(guid='1', state='open')
        >>> event.guid, event.state, event._class
        ('1', 'open', 'OrderCreated')
        >>> event.to_dict()
        {'guid': '1', 'state': 'open'}
        >>> isinstance(event, Message)
        True
    """

    class MessageProxy(Message):

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(message, **kwargs)

    # create a new proxy class that inherits from MessageProxy
    proxy = type(message.__name__, (MessageProxy, object), {})  # type: ignore
    return proxy


Event = lambda message: message_factory(message)
Command = lambda message: message_factory(message)
