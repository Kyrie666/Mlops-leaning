import logging
import sys
from logging.config import BaseConfigurator

# log format
from loguru import logger


LEVEL_MAP = {
    5: "TRACE",
    10: "DEBUG",
    20: "INFO",
    25: "SUCCESS",
    30: "WARNING",
    40: "ERROR",
    50: "CRITICAL",
}

# https://blog.csdn.net/qq_43784626/article/details/114916702
# 配置loguru
logger.remove()  # 删去import logger之后自动产生的handler，不删除的话会出现重复输出的现象
logger.add(
    sink=sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss}-{level} {module}:{function}:{line} {message}",
    level="INFO",
    backtrace=False,
    colorize=False,
)


class logHandler(logging.Handler, object):
    def __init__(self, other_attr=None, **kwargs):
        logging.Handler.__init__(self)

    def emit(self, record):
        logger_opt = logger.opt(
            depth=2,
            exception=record.exc_info,
            colors=False,
        )

        logger_opt.log(record.levelname, f" - {record.getMessage()}")


TRANSPORT_MAP = {"logHandler": logHandler}

LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "logHandler": {
            "class": "logHandler",
        }
    },
    "loggers": {
        "uvicorn": {"handlers": ["logHandler"], "level": "INFO"},
        "uvicorn.error": {
            "handlers": ["logHandler"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["logHandler"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


def transport(classname):
    return TRANSPORT_MAP.get(classname)


BaseConfigurator.importer = staticmethod(transport)
