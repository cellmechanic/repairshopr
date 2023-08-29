"""loki library"""
import logging
import logging_loki
from . import env_library


def start_loki(name):
    """start the logger"""
    handler = logging_loki.LokiHandler(
        url=f"{env_library.loki_url}",
        version="1",
    )
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
