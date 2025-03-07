import os
import logging
from typing import Dict, Any

from deltacat import logs

from deltacat.constants import (
    DELTACAT_APP_LOG_LEVEL,
    DELTACAT_SYS_LOG_LEVEL,
    DELTACAT_APP_LOG_DIR,
    DELTACAT_SYS_LOG_DIR,
    DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
    DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
    DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
    DELTACAT_LOGGER_USE_SINGLE_HANDLER,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def create_ray_runtime_environment() -> Dict[str, Any]:
    # log the system environment for debugging
    logger.debug(f"System Environment: {os.environ}")

    # read the stage (e.g. alpha, beta, dev, etc.) from system environment vars
    stage = os.environ.get("STAGE")
    logger.debug(f"Runtime Environment Stage: {stage}")
    runtime_environment = None
    if stage:
        worker_env_vars = {
            # forward the STAGE environment variable to workers
            "STAGE": stage,
            # forward deltacat logging environment variables to workers
            "DELTACAT_APP_LOG_LEVEL": DELTACAT_APP_LOG_LEVEL,
            "DELTACAT_SYS_LOG_LEVEL": DELTACAT_SYS_LOG_LEVEL,
            "DELTACAT_APP_LOG_DIR": DELTACAT_APP_LOG_DIR,
            "DELTACAT_SYS_LOG_DIR": DELTACAT_SYS_LOG_DIR,
            "DELTACAT_APP_INFO_LOG_BASE_FILE_NAME": DELTACAT_APP_INFO_LOG_BASE_FILE_NAME,
            "DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME": DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME,
            "DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME": DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME,
            "DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME": DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME,
            "DELTACAT_LOGGER_USE_SINGLE_HANDLER": str(
                DELTACAT_LOGGER_USE_SINGLE_HANDLER
            ),
        }
        # setup runtime environment from system environment variables:
        runtime_environment = {
            "env_vars": worker_env_vars,
        }
    return runtime_environment
