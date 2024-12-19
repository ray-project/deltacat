import os
import ray
import logging

from deltacat import logs
from deltacat.constants import DELTACAT_APP_LOG_DIR, DELTACAT_SYS_LOG_DIR
from deltacat.examples.common.fixtures import (
    create_runtime_environment,
    store_cli_args_in_os_environ,
)

# initialize the driver logger
driver_logger = logs.configure_application_logger(logging.getLogger(__name__))


@ray.remote
def logging_worker(var1, var2):
    # for AWS Glue, worker loggers must be initialized within the worker process
    worker_logger = logs.configure_application_logger(logging.getLogger(__name__))

    log_line_1 = f"Worker System Environment: {os.environ}"
    print(
        f"Writing DEBUG log line from Worker to {DELTACAT_APP_LOG_DIR}: '{log_line_1}'"
    )
    worker_logger.debug(log_line_1)

    log_line_2 = f"Worker Variable 1: {var1}"
    print(
        f"Writing INFO log line from Worker to {DELTACAT_APP_LOG_DIR}: '{log_line_2}'"
    )
    worker_logger.info(log_line_2)

    log_line_3 = f"Worker Variable 2: {var2}"
    print(
        f"Writing INFO log line from Worker to {DELTACAT_APP_LOG_DIR}: '{log_line_3}'"
    )
    worker_logger.info(log_line_3)


def run(var1="default1", var2="default2", **kwargs):
    log_line_1 = f"Driver Variable 1: {var1}"
    print(
        f"Writing INFO log line from Driver to {DELTACAT_APP_LOG_DIR}: '{log_line_1}'"
    )
    driver_logger.info(log_line_1)

    log_line_2 = f"Driver Variable 2: {var2}"
    print(
        f"Writing INFO log line from Driver to {DELTACAT_APP_LOG_DIR}: '{log_line_2}'"
    )
    driver_logger.info(log_line_2)

    print("Starting worker...")
    ray.get(logging_worker.remote(var1, var2))
    print(
        f"The driver is shutting down. Additional DeltaCAT system logs have been written to {DELTACAT_SYS_LOG_DIR}"
    )


if __name__ == "__main__":
    example_script_args = [
        (
            [
                "--var1",
            ],
            {
                "help": "First argument to log.",
                "type": str,
            },
        ),
        (
            [
                "--var2",
            ],
            {
                "help": "Second argument to log.",
                "type": str,
            },
        ),
        (
            [
                "--STAGE",
            ],
            {
                "help": "Example runtime environment stage (e.g. dev, alpha, beta, prod).",
                "type": str,
            },
        ),
    ]

    # store any CLI args in the runtime environment
    store_cli_args_in_os_environ(example_script_args)

    # create any runtime environment required to run the example
    runtime_env = create_runtime_environment()

    # initialize ray
    ray.init(runtime_env=runtime_env)

    # run the example using os.environ as kwargs
    run(**os.environ)
