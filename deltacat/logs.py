import os
import logging
import pathlib
from logging import handlers, Logger, Handler, FileHandler
from deltacat.constants import DELTACAT_LOG_LEVEL, APPLICATION_LOG_LEVEL

DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = \
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
DEFAULT_MAX_BYTES_PER_LOG = 2 ^ 20 * 256  # 256 MiB
DEFAULT_BACKUP_COUNT = 0

DEFAULT_APPLICATION_LOG_DIR = "/tmp/deltacat/var/output/logs/"
DEFAULT_APPLICATION_LOG_BASE_FILE_NAME = "application.info.log"
DEFAULT_DEBUG_APPLICATION_LOG_BASE_FILE_NAME = "application.debug.log"

DEFAULT_DELTACAT_LOG_DIR = "/tmp/deltacat/var/output/logs/"
DEFAULT_DELTACAT_LOG_BASE_FILE_NAME = "deltacat-python.info.log"
DEFAULT_DEBUG_DELTACAT_LOG_BASE_FILE_NAME = "deltacat-python.debug.log"


def _add_logger_handler(
        logger: Logger,
        handler: Handler) -> Logger:

    logger.setLevel(logging.getLevelName("DEBUG"))
    logger.addHandler(handler)
    return logger


def _create_rotating_file_handler(
        log_directory: str,
        log_base_file_name: str,
        logging_level: str = DEFAULT_LOG_LEVEL,
        max_bytes_per_log_file: int = DEFAULT_MAX_BYTES_PER_LOG,
        backup_count: int = DEFAULT_BACKUP_COUNT,
        logging_format: str = DEFAULT_LOG_FORMAT) -> FileHandler:

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


def _file_handler_exists(
        logger: Logger,
        log_dir: str,
        log_base_file_name: str) -> bool:

    handler_exists = False
    base_file_path = os.path.join(log_dir, log_base_file_name)
    if len(logger.handlers) > 0:
        norm_base_file_path = os.path.normpath(base_file_path)
        handler_exists = any([
            isinstance(handler, logging.FileHandler)
            and os.path.normpath(handler.baseFilename) == norm_base_file_path
            for handler in logger.handlers
        ])
    return handler_exists


def _configure_logger(
        logger: Logger,
        log_level: str,
        log_dir: str,
        log_base_file_name: str,
        debug_log_base_file_name: str) -> Logger:

    primary_log_level = log_level
    logger.propagate = False
    if log_level.upper() == "DEBUG":
        if not _file_handler_exists(logger, log_dir, debug_log_base_file_name):
            handler = _create_rotating_file_handler(
                log_dir,
                debug_log_base_file_name,
                "DEBUG",
            )
            _add_logger_handler(logger, handler)
            primary_log_level = "INFO"
    if not _file_handler_exists(logger, log_dir, log_base_file_name):
        handler = _create_rotating_file_handler(
            log_dir,
            log_base_file_name,
            primary_log_level,
        )
        _add_logger_handler(logger, handler)
    return logger


def configure_deltacat_logger(logger: Logger) -> Logger:
    return _configure_logger(
        logger,
        DELTACAT_LOG_LEVEL,
        DEFAULT_DELTACAT_LOG_DIR,
        DEFAULT_DELTACAT_LOG_BASE_FILE_NAME,
        DEFAULT_DEBUG_DELTACAT_LOG_BASE_FILE_NAME
    )


def configure_application_logger(logger: Logger) -> Logger:
    return _configure_logger(
        logger,
        APPLICATION_LOG_LEVEL,
        DEFAULT_APPLICATION_LOG_DIR,
        DEFAULT_APPLICATION_LOG_BASE_FILE_NAME,
        DEFAULT_DEBUG_APPLICATION_LOG_BASE_FILE_NAME
    )
