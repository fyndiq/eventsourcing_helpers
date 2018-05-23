import inspect
from importlib import import_module
from typing import Any, Callable


def import_backend(location: str) -> Any:
    """
    Dynamically load a backend.

    Args:
        location: Location of the backend class in the
            backends package. Format: 'module.class'.

    Returns:
        BackendClass: The backend class.

    Example:
        >>> import_backend(
            'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend'
        )
        <class 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend'>  # noqa
    """
    module_name, class_name = location.rsplit('.', 1)
    module = import_module(f'{module_name}')

    backend_cls = getattr(module, class_name)
    return backend_cls


def get_callable_representation(target: Callable) -> str:
    """
    Get the representation of a callable.

    Ex:
        class Test:
            def method(self):
                pass

            @classmethod
            def klass(cls):
                pass

            @staticmethod
            def static():
                pass

        def myfunc():
            pass

        >> r = get_callable_representation
        >> r(Test), r(Test.method), r(Test.klass), r(Test.static), r(myfunc)
        ('Test', 'Test.method', 'Test.klass', 'Test.static', 'myfunc')

        >> test = Test()
        >> r(test.method), r(test.klass), r(test.static)
        ('Test.method', 'Test.klass', 'Test.static')
    """
    empty_string = ''

    if not callable(target):
        return empty_string

    if inspect.ismethod(target):
        try:
            return _get_method_name_representation(target)
        except Exception:
            return empty_string

    return getattr(target, '__qualname__', empty_string)


def _get_method_name_representation(method):
    """
    We have two types of methods:
        - normal methods
        - classmethods

    The `__self__` attribute from a normal method will return itself, however
    it will return the `class` when it's called from the classmethod.

    This is the reason we need to access the `__class__` attribute for
    the normal method.
    """
    method_name = getattr(method, '__name__', '')
    parent_class = method.__self__

    # Check if this if the parent class is the main class or is the
    # method itself
    is_main_class = type(parent_class) is type
    if not is_main_class:
        parent_class = parent_class.__class__

    return f"{parent_class.__name__}.{method_name}"
