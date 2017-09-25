class RepositoryBackend:
    """
    Repository interface.
    """
    def commit(self, guid, events, **kwargs) -> None:
        raise NotImplementedError()

    def load(self, guid, *args, **kwargs) -> list:
        raise NotADirectoryError()
