import os
import logging
import argparse
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def store_cli_args_in_os_environ(script_args_list=[]):
    parser = argparse.ArgumentParser()
    for args, kwargs in script_args_list:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")
    os.environ.update(vars(args))
