class RepositoryBackend:
    """
    Repository interface.
    """

    def commit(self, id: str, events: list, **kwargs) -> None:
        raise NotImplementedError()

    def load(self, id: str, **kwargs) -> list:
        raise NotADirectoryError()
