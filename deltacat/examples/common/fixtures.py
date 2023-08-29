import os
import logging
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
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def create_runtime_environment():
    # read packages to install in the runtime environment from system environment vars
    package = os.environ.get("PACKAGE")
    logger.debug(f"Pip Install Packages: {package}")

    runtime_environment = None
    if package:
        # read the stage (e.g. alpha, beta, dev, etc.) from the system environment vars
        stage = os.environ["STAGE"]
        logger.debug(f"Job Run Stage: {stage}")

        # log the job run system environment for debugging
        logger.debug(f"Job Run System Environment: {os.environ}")

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
        }

        # setup runtime environment from system environment variables:
        #   1. STAGE: The execution environment stage (e.g. alpha, beta, dev, etc.)
        #   2. PACKAGE: Package to Pip Install. (e.g. signed S3 URL for DeltaCAT)
        runtime_environment = {
            "env_vars": worker_env_vars,
            "pip": package,
        }
    return runtime_environment
