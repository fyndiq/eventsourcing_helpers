import copy
from typing import Callable

try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple


class Message:
    """
    Message proxy class.

    Wraps a message class to provide some extra features.

    All attribute lookups are redirected to the wrapped message class.
    """

    def __init__(self, **kwargs) -> None:
        # At this point the instance variable `self._wrapped` is already set.
        #
        # The reason we are saving it in `self.__dict__` is to skip hitting the
        # `__setattr__`dunder method - and thus getting the "Messages are read
        # only" error.
        self.__dict__['_wrapped'] = self._wrapped(**kwargs)  # type: ignore

    @property
    def _class(self) -> str:
        return self._wrapped.__class__.__name__  # type: ignore

    def to_dict(self) -> dict:
        return self._wrapped._asdict()  # type: ignore

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return repr(self._wrapped)  # type: ignore

    def __setattr__(self, *args) -> None:
        raise AttributeError("Messages are read only")

    def __getattr__(self, name: str) -> Callable:
        # redirects all attribute lookups to the real message class.
        raise NotImplementedError


class NewMessage(Message):
    """
    Newly consumed message.

    If we try to access an attribute that doesn't exist we should raise an
    AttributeError.
    """

    def __getattr__(self, name: str) -> Callable:
        attr = getattr(self._wrapped, name)
        return copy.deepcopy(attr)


class OldMessage(Message):
    """
    Message loaded from the repository.

    Accessing attributes that doesn't exist should NOT raise an AtrributeError,
    instead we return None.

    Otherwise we would have to implement try/catch logic in our apply methods
    when we add new fields to our Avro schemas.
    """

    def __getattr__(self, name: str) -> Callable:
        attr = getattr(self._wrapped, name, None)
        return copy.deepcopy(attr)  # type: ignore


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
        >>> @message_factory
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
    # Dynamically create a new class with the same name as the message
    # (event/command) we are wrapping.
    #
    # The new class will inherit from `proxy_cls` and `object` with one
    # instance variable set `_wrapped` which is the actual message
    # (event/command) being wrapped.
    proxy_cls = NewMessage if is_new else OldMessage
    proxy = type(
        message_cls.__name__, (proxy_cls, object), {'_wrapped': message_cls}
    )
    return proxy


# Make the names more explicit for which type of message we are dealing with.
Event = lambda message: message_factory(message)
Command = lambda message: message_factory(message)
