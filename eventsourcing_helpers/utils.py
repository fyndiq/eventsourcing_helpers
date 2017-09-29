from importlib import import_module
from typing import Any


def import_backend(package: str, location: str) -> Any:
    """
    Dynamically load a backend.

    Args:
        location: Location of the backend class in the
            backends package. Format: 'module.class'.

    Returns:
        BackendClass: The backend class.

    Example:
        >>> load_backend('eventsourcing_helpers.repository.backends',
        ...              'kafka.KafkaAvroBackend')
        <class 'eventsourcing_helpers.repository.backends.kafka.KafkaAvroBackend'>
    """
    module_name, class_name = location.rsplit('.', 1)
    module = import_module(f'{package}.{module_name}')
    backend_cls = getattr(module, class_name)

    return backend_cls
