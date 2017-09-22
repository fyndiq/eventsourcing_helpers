class RepositoryBackend:

    def commit(self):
        raise NotImplementedError()

    def load(self):
        raise NotADirectoryError()
