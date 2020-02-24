from typing import Any


class RepositoryBackend:
    """
    Repository interface.
    """

    def commit(self, id: str, events: list, **kwargs) -> None:
        raise NotImplementedError()

    def load(self, id: str, **kwargs) -> Any:
        raise NotADirectoryError()

    def get_events(self, id: str, **kwargs) -> Any:
        raise NotImplementedError()
