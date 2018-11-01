from copy import deepcopy
from importlib import import_module
from typing import Any, Callable, List


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
        <class 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend'>
    """
    module_name, class_name = location.rsplit('.', 1)
    module = import_module(f'{module_name}')

    backend_cls = getattr(module, class_name)
    return backend_cls


def get_all_nested_keys(data: dict, current_keys: List) -> List:
    """
    Get all keys in a dictoinary. The dictionary could have more levels of
    nested dicts.
    The purpose of this is to detect changes to a class. Therefore the value
    for key: `py/object` is also saved.

    Args:
        data: The dictionary to be examined
        current_keys: The keys found so far (used in recursive calls)

    Returns:
        List: All keys found

    Example:
    >>> get_all_nested_keys(
    ...     {
    ...         '_version': 0,
    ...         'id': None,
    ...         'nested_entity': {
    ...             '__dict__': {
    ...                 'nested_entity_id': 'a'
    ...             },
    ...             'py/object': 'eventsourcing_helpers.models.EntityDict'
    ...         },
    ...         'py/object': 'tests.test_models.NestedAggregate'
    ...     },
    ...     []
    ... )

    [
        '_version', 'id', 'nested_entity', 'py/object', '__dict__',
        'py/object', 'nested_entity_id',
        'eventsourcing_helpers.models.EntityDict',
        'tests.test_models.NestedAggregate'
    ]
    """
    all_keys = deepcopy(current_keys)
    if isinstance(data, dict):
        all_keys.extend(list(data.keys()))
        for key, value in data.items():
            if key == 'py/object':
                all_keys.append(value)
            else:
                all_keys = get_all_nested_keys(value, all_keys)
    elif isinstance(data, (list, tuple)):
        for item in data:
            all_keys = get_all_nested_keys(item, all_keys)

    return all_keys


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
