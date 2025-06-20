import logging
import logging.handlers
from logging import StreamHandler


def configure_logging(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s|%(name)s|%(levelname)s|%(funcName)s:%(lineno)d > %(message)s"
    )
    # rotating file handler
    rotating_handler = logging.handlers.RotatingFileHandler(
        "logs/mkts-backend.log", maxBytes=1048576, backupCount=5
    )
    rotating_handler.setFormatter(formatter)
    logger.addHandler(rotating_handler)
    stream_handler = StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.ERROR)
    logger.addHandler(stream_handler)

    return logger
