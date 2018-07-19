from typing import Any, Callable, Generator


class RepositoryBackend:
    """
    Repository interface.
    """

    def commit(self, id: str, events: list, **kwargs) -> None:
        raise NotImplementedError()

    def load(self, id: str, **kwargs) -> list:
        raise NotADirectoryError()

    def get_events(
        self, id: str, message_deserializer: Callable
    ) -> Generator[Any, None, None]:
        raise NotImplementedError()
