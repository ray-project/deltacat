import os
import logging
import pathlib
from logging import handlers
from deltacat.constants import DELTACAT_LOG_LEVEL, APPLICATION_LOG_LEVEL

DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = \
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
DEFAULT_MAX_BYTES_PER_LOG = 2 ^ 20 * 256  # 256 MiB
DEFAULT_BACKUP_COUNT = 2

DEFAULT_APPLICATION_LOG_DIR = "/tmp/deltacat/var/output/logs/"
DEFAULT_APPLICATION_LOG_BASE_FILE_NAME = "application.info.log"
DEFAULT_DEBUG_APPLICATION_LOG_BASE_FILE_NAME = "application.debug.log"

DEFAULT_DELTACAT_LOG_DIR = "/tmp/deltacat/var/output/logs/"
DEFAULT_DELTACAT_LOG_BASE_FILE_NAME = "deltacat-python.info.log"
DEFAULT_DEBUG_DELTACAT_LOG_BASE_FILE_NAME = "deltacat-python.debug.log"


def _add_logger_handler(
        logger,
        handler):

    logger.setLevel(logging.getLevelName("DEBUG"))
    logger.addHandler(handler)
    return logger


def _create_rotating_file_handler(
        log_directory,
        log_base_file_name,
        logging_level=DEFAULT_LOG_LEVEL,
        max_bytes_per_log_file=DEFAULT_MAX_BYTES_PER_LOG,
        backup_count=DEFAULT_BACKUP_COUNT,
        logging_format=DEFAULT_LOG_FORMAT):

    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    assert log_base_file_name, "log file name is required"
    assert log_directory, "log directory is required"

    log_dir_path = pathlib.Path(log_directory)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_directory, log_base_file_name),
        maxBytes=max_bytes_per_log_file,
        backupCount=backup_count)
    handler.setFormatter(logging.Formatter(logging_format))
    handler.setLevel(logging_level)
    return handler


def _configure_logger(
        logger,
        log_level,
        log_dir,
        log_base_file_name,
        debug_log_base_file_name):

    primary_log_level = log_level
    if log_level.upper() == "DEBUG":
        handler = _create_rotating_file_handler(
            log_dir,
            debug_log_base_file_name,
            "DEBUG"
        )
        _add_logger_handler(logger, handler)
        primary_log_level = "INFO"
    handler = _create_rotating_file_handler(
        log_dir,
        log_base_file_name,
        primary_log_level
    )
    return _add_logger_handler(logger, handler)


def configure_deltacat_logger(logger):
    return _configure_logger(
        logger,
        DELTACAT_LOG_LEVEL,
        DEFAULT_DELTACAT_LOG_DIR,
        DEFAULT_DELTACAT_LOG_BASE_FILE_NAME,
        DEFAULT_DEBUG_DELTACAT_LOG_BASE_FILE_NAME
    )


def configure_application_logger(logger):
    return _configure_logger(
        logger,
        APPLICATION_LOG_LEVEL,
        DEFAULT_APPLICATION_LOG_DIR,
        DEFAULT_APPLICATION_LOG_BASE_FILE_NAME,
        DEFAULT_DEBUG_APPLICATION_LOG_BASE_FILE_NAME
    )
