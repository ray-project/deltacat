import gzip
import logging
import os
import json
import pathlib
from logging import FileHandler, Handler, Logger, LoggerAdapter, handlers
from typing import Any, Dict, Optional, Union

import ray
from ray.runtime_context import RuntimeContext

from deltacat.constants import (
    BYTES_PER_MEBIBYTE,
    DELTACAT_APP_LOG_LEVEL,
    DELTACAT_SYS_LOG_LEVEL,
    DELTACAT_APP_LOG_DIR,
    DELTACAT_SYS_LOG_DIR,
    DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
    DELTACAT_LOGGER_CONTEXT,
    DELTACAT_LOGGER_USE_SINGLE_HANDLER,
)

DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = {
    "level": "levelname",
    "message": "message",
    "loggerName": "name",
    "processName": "processName",
    "processID": "process",
    "threadName": "threadName",
    "timestamp": "asctime",
    "filename": "filename",
    "lineno": "lineno",
}
DEFAULT_MAX_BYTES_PER_LOG = 64 * BYTES_PER_MEBIBYTE  # 64 MiB
DEFAULT_BACKUP_COUNT = 10  # Default 10 backup files


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.

    @param dict fmt_dict: Key: logging format attribute pairs. Defaults to {"message": "message"}.
    @param str time_format: time.strftime() format string. Default: "%Y-%m-%dT%H:%M:%S"
    @param str msec_format: Microsecond formatting. Appended at the end. Default: "%s.%03dZ"
    """

    def __init__(
        self,
        fmt_dict: dict = None,
        time_format: str = "%Y-%m-%dT%H:%M:%S",
        msec_format: str = "%s.%03dZ",
        context_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.fmt_dict = fmt_dict if fmt_dict is not None else {"message": "message"}
        self.default_time_format = time_format
        self.default_msec_format = msec_format
        self.datefmt = None
        self.additional_context = context_kwargs or {}
        if ray.is_initialized():
            self.ray_runtime_ctx: RuntimeContext = ray.get_runtime_context()
            self.context = {}
            if hasattr(self.ray_runtime_ctx.worker, "worker_id"):
                self.context["worker_id"] = self.ray_runtime_ctx.get_worker_id()
            else:
                self.context["worker_id"] = None
            self.context["node_id"] = self.ray_runtime_ctx.get_node_id()
            self.context["job_id"] = self.ray_runtime_ctx.get_job_id()
        else:
            self.ray_runtime_ctx = None
            self.context = {}

        if DELTACAT_LOGGER_CONTEXT is not None:
            try:
                env_context = json.loads(DELTACAT_LOGGER_CONTEXT)
                self.additional_context.update(env_context)
            except Exception:
                pass

    def usesTime(self) -> bool:
        """
        Overwritten to look for the attribute in the format dict values instead of the fmt string.
        """
        return "asctime" in self.fmt_dict.values()

    def formatMessage(self, record) -> dict:
        """
        Overwritten to return a dictionary of the relevant LogRecord attributes instead of a string.
        KeyError is raised if an unknown attribute is provided in the fmt_dict.
        """
        return {
            fmt_key: record.__dict__[fmt_val]
            for fmt_key, fmt_val in self.fmt_dict.items()
        }

    def format(self, record) -> str:
        """
        Mostly the same as the parent's class method, the difference being that a dict is manipulated and dumped as JSON
        instead of a string.
        """
        record.message = record.getMessage()

        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        message_dict = self.formatMessage(record)

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            message_dict["exc_info"] = record.exc_text

        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)

        if self.ray_runtime_ctx:
            # only workers will have task ID
            if (
                self.ray_runtime_ctx.worker
                and self.ray_runtime_ctx.worker.mode == ray._private.worker.WORKER_MODE
            ):
                self.context["task_id"] = self.ray_runtime_ctx.get_task_id()
                self.context[
                    "assigned_resources"
                ] = self.ray_runtime_ctx.get_assigned_resources()

            message_dict["ray_runtime_context"] = self.context

        if self.additional_context:
            message_dict["additional_context"] = self.additional_context

        return json.dumps(message_dict, default=str)


class DeltaCATLoggerAdapter(logging.LoggerAdapter):
    """
    Logger Adapter class with additional functionality
    """

    def __init__(self, logger: Logger, extra: Optional[Dict[str, Any]] = {}):
        super().__init__(logger, extra)

    def debug_conditional(self, msg, do_print: bool, *args, **kwargs):
        if do_print:
            self.debug(msg, *args, **kwargs)

    def info_conditional(self, msg, do_print: bool, *args, **kwargs):
        if do_print:
            self.info(msg, *args, **kwargs)

    def warning_conditional(self, msg, do_print: bool, *args, **kwargs):
        if do_print:
            self.warning(msg, *args, **kwargs)

    def error_conditional(self, msg, do_print: bool, *args, **kwargs):
        if do_print:
            self.error(msg, *args, **kwargs)


def _add_logger_handler(logger: Logger, handler: Handler) -> Logger:

    logger.setLevel(logging.getLevelName("DEBUG"))
    logger.addHandler(handler)
    return logger


def _safe_rename(source: str, dest: str) -> None:
    """Safely rename a file, ignoring errors from concurrent access.

    When multiple processes attempt to rotate the same log files simultaneously,
    one may succeed while others find the file already moved. This is expected
    and safe to ignore.
    """
    try:
        if os.path.exists(dest):
            os.remove(dest)
        if os.path.exists(source):
            os.rename(source, dest)
    except (FileNotFoundError, OSError):
        pass  # Another process already handled this file


def _safe_compress(source: str, dest: str) -> None:
    """Safely compress a file to gzip, ignoring errors from concurrent access."""
    try:
        if os.path.exists(source) and not os.path.exists(dest):
            with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
                f_out.writelines(f_in)
            os.remove(source)
    except (FileNotFoundError, OSError):
        pass  # Another process already handled this file


class CompressingRotatingFileHandler(handlers.RotatingFileHandler):
    """Rotating file handler that gzips rolled logs after rotation.

    This handler is resilient to concurrent access from multiple processes
    (e.g., Ray workers). When multiple processes try to rotate the same log
    files simultaneously, one process may succeed while others encounter
    missing files - this is expected and safe to ignore.
    """

    def doRollover(self):
        self._close_stream()
        self._rotate_log_files()
        self._open_new_stream()
        self._compress_rotated_logs()

    def _close_stream(self) -> None:
        """Close the current log stream."""
        if self.stream:
            self.stream.close()
            self.stream = None

    def _rotate_log_files(self) -> None:
        """Rotate log files: log.N -> log.N+1, then current -> log.1."""
        if self.backupCount <= 0:
            return

        # Rotate existing backups: log.N -> log.N+1 (from highest to lowest)
        for i in range(self.backupCount - 1, 0, -1):
            source = self.rotation_filename(f"{self.baseFilename}.{i}")
            dest = self.rotation_filename(f"{self.baseFilename}.{i + 1}")
            _safe_rename(source, dest)

        # Rotate current log to log.1
        dest = self.rotation_filename(f"{self.baseFilename}.1")
        _safe_rename(self.baseFilename, dest)

    def _open_new_stream(self) -> None:
        """Open a new log file stream."""
        if not self.delay:
            self.stream = self._open()

    def _compress_rotated_logs(self) -> None:
        """Compress all rotated log files to gzip format."""
        if self.backupCount <= 0:
            return

        for i in range(self.backupCount, 0, -1):
            log_file = f"{self.baseFilename}.{i}"
            compressed_file = f"{log_file}.gz"
            _safe_compress(log_file, compressed_file)


def _create_rotating_file_handler(
    log_directory: str,
    log_base_file_name: str,
    logging_level: Union[str, int] = DEFAULT_LOG_LEVEL,
    max_bytes_per_log_file: int = DEFAULT_MAX_BYTES_PER_LOG,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    logging_format: Union[str, dict] = DEFAULT_LOG_FORMAT,
    context_kwargs: Dict[str, Any] = None,
) -> FileHandler:

    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    assert log_base_file_name, "log file name is required"
    assert log_directory, "log directory is required"
    log_dir_path = pathlib.Path(log_directory)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    handler = CompressingRotatingFileHandler(
        os.path.join(log_directory, log_base_file_name),
        maxBytes=max_bytes_per_log_file,
        backupCount=backup_count,
    )

    if type(logging_format) is str:
        handler.setFormatter(logging.Formatter(logging_format))
    else:
        handler.setFormatter(
            JsonFormatter(logging_format, context_kwargs=context_kwargs)
        )

    handler.setLevel(logging_level)
    return handler


def _file_handler_exists(logger: Logger, log_dir: str, log_base_file_name: str) -> bool:

    handler_exists = False
    base_file_path = os.path.join(log_dir, log_base_file_name)

    if logger.handlers:
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
    log_level: int,
    log_dir: str,
    log_base_file_name: str,
    debug_log_base_file_name: str,
    context_kwargs: Dict[str, Any] = None,
) -> Union[Logger, LoggerAdapter]:
    # This maintains log level of rotating file handlers
    primary_log_level = log_level
    logger.propagate = False
    needs_handler = True
    if log_level <= logging.getLevelName("DEBUG"):
        if not _file_handler_exists(logger, log_dir, debug_log_base_file_name):
            handler = _create_rotating_file_handler(
                log_dir,
                debug_log_base_file_name,
                "DEBUG",
                context_kwargs=context_kwargs,
            )
            _add_logger_handler(logger, handler)
            needs_handler = not DELTACAT_LOGGER_USE_SINGLE_HANDLER
            primary_log_level = logging.getLevelName("INFO")
    if not _file_handler_exists(logger, log_dir, log_base_file_name) and needs_handler:
        handler = _create_rotating_file_handler(
            log_dir,
            log_base_file_name,
            primary_log_level,
            context_kwargs=context_kwargs,
        )
        _add_logger_handler(logger, handler)

    return DeltaCATLoggerAdapter(logger)


def configure_deltacat_logger(
    logger: Logger,
    level: int = None,
    context_kwargs: Dict[str, Any] = None,
) -> Union[Logger, LoggerAdapter]:
    if level is None:
        level = logging.getLevelName(DELTACAT_SYS_LOG_LEVEL)

    return _configure_logger(
        logger,
        level,
        DELTACAT_SYS_LOG_DIR,
        DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
        DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
        context_kwargs,
    )


def configure_application_logger(
    logger: Logger,
    level: int = None,
    context_kwargs: Dict[str, Any] = None,
) -> Union[Logger, LoggerAdapter]:
    if level is None:
        level = logging.getLevelName(DELTACAT_APP_LOG_LEVEL)

    return _configure_logger(
        logger,
        level,
        DELTACAT_APP_LOG_DIR,
        DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
        DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
        context_kwargs,
    )
