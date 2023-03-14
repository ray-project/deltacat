import logging
import os
import pathlib
from logging import FileHandler, Handler, Logger, LoggerAdapter, handlers
from typing import Union

import ray
from ray.runtime_context import RuntimeContext

from deltacat.constants import (
    DELTACAT_APP_LOG_LEVEL,
    DELTACAT_SYS_LOG_LEVEL,
    DELTACAT_APP_LOG_DIR,
    DELTACAT_SYS_LOG_DIR,
    DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
)

DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = (
    "%(asctime)s\t%(levelname)s pid=%(process)d %(filename)s:%(lineno)s -- %(message)s"
)
DEFAULT_MAX_BYTES_PER_LOG = 2 ^ 20 * 256  # 256 MiB
DEFAULT_BACKUP_COUNT = 0


class RayRuntimeContextLoggerAdapter(logging.LoggerAdapter):
    """
    Logger Adapter for injecting Ray Runtime Context into logging messages.
    """

    def __init__(self, logger: Logger, runtime_context: RuntimeContext):
        super().__init__(logger, {})
        self.runtime_context = runtime_context

    def process(self, msg, kwargs):
        """
        Injects Ray Runtime Context details into each log message.

        This may include information such as the raylet node ID, task/actor ID, job ID,
        placement group ID of the worker, and assigned resources to the task/actor.

        Args:
            msg: The original log message
            kwargs: Keyword arguments for the log message

        Returns: A log message with Ray Runtime Context details

        """
        runtime_context_dict = self.runtime_context.get()
        runtime_context_dict[
            "worker_id"
        ] = self.runtime_context.worker.core_worker.get_worker_id()
        if self.runtime_context.get_task_id() or self.runtime_context.get_actor_id():
            runtime_context_dict[
                "pg_id"
            ] = self.runtime_context.get_placement_group_id()
            runtime_context_dict[
                "assigned_resources"
            ] = self.runtime_context.get_assigned_resources()

        return "(ray_runtime_context=%s) -- %s" % (runtime_context_dict, msg), kwargs

    def __reduce__(self):
        """
        Used to unpickle the class during Ray object store transfer.
        """

        def deserializer(*args):
            return RayRuntimeContextLoggerAdapter(args[0], ray.get_runtime_context())

        return deserializer, (self.logger,)


def _add_logger_handler(logger: Logger, handler: Handler) -> Logger:

    logger.setLevel(logging.getLevelName("DEBUG"))
    logger.addHandler(handler)
    return logger


def _create_rotating_file_handler(
    log_directory: str,
    log_base_file_name: str,
    logging_level: str = DEFAULT_LOG_LEVEL,
    max_bytes_per_log_file: int = DEFAULT_MAX_BYTES_PER_LOG,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    logging_format: str = DEFAULT_LOG_FORMAT,
) -> FileHandler:

    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    assert log_base_file_name, "log file name is required"
    assert log_directory, "log directory is required"
    log_dir_path = pathlib.Path(log_directory)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    handler = handlers.RotatingFileHandler(
        os.path.join(log_directory, log_base_file_name),
        maxBytes=max_bytes_per_log_file,
        backupCount=backup_count,
    )
    handler.setFormatter(logging.Formatter(logging_format))
    handler.setLevel(logging_level)
    return handler


def _file_handler_exists(logger: Logger, log_dir: str, log_base_file_name: str) -> bool:

    handler_exists = False
    base_file_path = os.path.join(log_dir, log_base_file_name)
    if len(logger.handlers) > 0:
        norm_base_file_path = os.path.normpath(base_file_path)
        handler_exists = any(
            [
                isinstance(handler, logging.FileHandler)
                and os.path.normpath(handler.baseFilename) == norm_base_file_path
                for handler in logger.handlers
            ]
        )
    return handler_exists


def _configure_logger(
    logger: Logger,
    log_level: str,
    log_dir: str,
    log_base_file_name: str,
    debug_log_base_file_name: str,
) -> Union[Logger, LoggerAdapter]:
    primary_log_level = log_level
    logger.propagate = False
    if log_level.upper() == "DEBUG":
        if not _file_handler_exists(logger, log_dir, debug_log_base_file_name):
            handler = _create_rotating_file_handler(
                log_dir, debug_log_base_file_name, "DEBUG"
            )
            _add_logger_handler(logger, handler)
            primary_log_level = "INFO"
    if not _file_handler_exists(logger, log_dir, log_base_file_name):
        handler = _create_rotating_file_handler(
            log_dir, log_base_file_name, primary_log_level
        )
        _add_logger_handler(logger, handler)
    ray_runtime_ctx = ray.get_runtime_context()
    if ray_runtime_ctx.worker.connected:
        logger = RayRuntimeContextLoggerAdapter(logger, ray_runtime_ctx)

    return logger


def configure_deltacat_logger(logger: Logger) -> Union[Logger, LoggerAdapter]:
    return _configure_logger(
        logger,
        DELTACAT_SYS_LOG_LEVEL,
        DELTACAT_SYS_LOG_DIR,
        DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
        DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
    )


def configure_application_logger(logger: Logger) -> Union[Logger, LoggerAdapter]:
    return _configure_logger(
        logger,
        DELTACAT_APP_LOG_LEVEL,
        DELTACAT_APP_LOG_DIR,
        DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
        DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
    )
