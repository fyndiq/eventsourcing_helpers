import logging


class ExtraLoggerAdapter(logging.LoggerAdapter):
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, extra={**self.extra, **kwargs})

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, extra={**self.extra, **kwargs})

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, extra={**self.extra, **kwargs})

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, extra={**self.extra, **kwargs})

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, extra={**self.extra, **kwargs})

    def exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, extra={**self.extra, **kwargs})


def get_logger(name, **kwargs):
    logger = logging.getLogger(name)
    adapter = ExtraLoggerAdapter(logger, extra=kwargs)
    return adapter
