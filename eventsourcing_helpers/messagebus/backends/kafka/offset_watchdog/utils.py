import structlog

logger = structlog.get_logger(__name__)


# https://github.com/edenhill/librdkafka/issues/1720
# until this bug is fixed at least make sure we log when it happens.
def check_offset_deviation(offset, last_offset):
    offset_diff = offset - int(last_offset)
    if offset_diff != 1:
        logger.warning(
            "Offset deviation detected", offset_diff=offset_diff
        )
