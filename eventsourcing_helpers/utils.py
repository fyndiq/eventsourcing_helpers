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
    return getattr(target, '__qualname__', getattr(target, '__name__', ''))
