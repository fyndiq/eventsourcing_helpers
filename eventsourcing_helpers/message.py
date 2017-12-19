from typing import Callable

from cnamedtuple import namedtuple


class Message:
    def __init__(self, _message: namedtuple, **kwargs) -> None:
        self.__dict__['_message'] = _message(**kwargs)

    @property
    def _class(self) -> str:
        return self._message.__class__.__name__

    def to_dict(self) -> dict:
        items = self._message._asdict().items()  # type: ignore
        filtered = {k: v for k, v in items if v is not None}
        return filtered

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return repr(self._message)

    def __getattr__(self, name: str) -> Callable:
        return getattr(self._message, name)

    def __setattr__(self, *args) -> None:
        raise AttributeError("Messages are read only")


class MessageProxy(Message):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self._message, **kwargs)


def message_factory(message: namedtuple) -> type:
    """
    Class decorator used for constructing a message.

    A message is basically a namedtuple extended with some extra features.

    Currently namedtuples doesn't support inheritance from a base class - which
    means we can't easily add extra methods/properties.

    See: https://github.com/python/typing/issues/427.

    To work around this we can wrap the namedtuple in a "proxy class" which
    adds all the extra features and redirects all attribute lookups to the
    underlying namedtuple.

    TODO: use data classes in Python 3.7

    Example:
        >>> @Event
        ... class OrderCreated(NamedTuple):
        ...     id: str
        ...     state: str
        >>> event = OrderCreated(id='1', state='open')
        >>> event.id, event.state, event._class
        ('1', 'open', 'OrderCreated')
        >>> event.to_dict()
        {'id': '1', 'state': 'open'}
        >>> isinstance(event, Message)
        True
    """
    # create a new proxy class that inherits from MessageProxy
    proxy = type(
        message.__name__, (MessageProxy, object), {
            '_message': message
        }
    )
    return proxy


Event = lambda message: message_factory(message)
Command = lambda message: message_factory(message)
