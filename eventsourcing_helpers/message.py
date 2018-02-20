from typing import Callable

from cnamedtuple import namedtuple


class Message:
    """
    Message proxy class.

    Adds some extra features and redirects all attribute lookups to the wrapped
    message class.
    """
    def __init__(self, **kwargs) -> None:
        self.__dict__['_wrapped'] = self._wrapped(**kwargs)  # type: ignore

    @property
    def _class(self) -> str:
        return self._wrapped.__class__.__name__  # type: ignore

    def to_dict(self) -> dict:
        items = self._wrapped._asdict().items()  # type: ignore
        filtered = {k: v for k, v in items if v is not None}
        return filtered

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return repr(self._wrapped)  # type: ignore

    def __setattr__(self, *args) -> None:
        raise AttributeError("Messages are read only")


class NewMessage(Message):
    """
    Newly consumed message.

    If we try to access an attribute that doesn't exist we should raise an
    AttributeError.
    """
    def __getattr__(self, name: str) -> Callable:
        return getattr(self._wrapped, name)


class OldMessage(Message):
    """
    Message loaded from the repository.

    Accessing attributes that doesn't exist should NOT raise an AtrributeError,
    instead we return None.

    Otherwise we would have to implement try/catch logic in our apply methods
    when we add new fields to our Avro schemas.
    """
    def __getattr__(self, name: str) -> Callable:
        return getattr(self._wrapped, name, None)


def message_factory(message_cls: namedtuple, is_new=True) -> type:
    """
    Class decorator used for creating a message proxy class.

    A message proxy is just a message "wrapped" with some extra features and
    where all attribute lookups are passed through to the wrapped message
    class.

    The reason we are doing this is because currently namedtuples doesn't
    support inheritance from a base class - which means we can't easily add
    extra features.

    See: https://github.com/python/typing/issues/427.

    TODO: look into data classes in Python 3.7.

    Args:
        message_cls: Message class to be wrapped.
        is_new: Flag to indicate if the message is new or loaded from the
            repository.

    Returns:
        Message: Message proxy.

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
    proxy_cls = NewMessage if is_new else OldMessage
    proxy = type(
        message_cls.__name__, (proxy_cls, object),
        {'_wrapped': message_cls}
    )
    return proxy


Event = lambda message: message_factory(message)
Command = lambda message: message_factory(message)
