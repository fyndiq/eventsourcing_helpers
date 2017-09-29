class RepositoryBackend:
    """
    Repository interface.
    """

    def commit(self, guid: str, events: list, **kwargs) -> None:
        raise NotImplementedError()

    def load(self, guid: str, **kwargs) -> list:
        raise NotADirectoryError()
