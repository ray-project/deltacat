import os
import ray
import logging

from deltacat import logs
from deltacat.examples.common.fixtures import create_runtime_environment

# initialize the driver logger
driver_logger = logs.configure_application_logger(logging.getLogger(__name__))


@ray.remote
def logging_worker(var1, var2):
    # for AWS Glue, worker loggers must be initialized within the worker process
    worker_logger = logs.configure_application_logger(logging.getLogger(__name__))
    worker_logger.debug(f"Worker System Environment: {os.environ}")
    worker_logger.info(f"Worker Variable 1: {var1}")
    worker_logger.info(f"Worker Variable 2: {var2}")


def run(var1="default1", var2="default2", **kwargs):
    driver_logger.info(f"Driver Variable 1: {var1}")
    driver_logger.info(f"Driver Variable 2: {var2}")
    logging_worker.remote(var1, var2)


if __name__ == "__main__":
    # create any runtime environment required to run the example
    runtime_env = create_runtime_environment()

    # initialize ray
    ray.init(address="auto", runtime_env=runtime_env)

    # run the example using os.environ as kwargs
    run(**os.environ)
