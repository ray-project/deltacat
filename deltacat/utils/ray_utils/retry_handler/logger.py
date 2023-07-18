from logging import Logger
import logging


def configure_logger(logger: Logger) -> Logger:
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s [%(name)s;%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S')

    # These modules were not configured to honor the log level specified,
    # Hence, explicitly setting log level for them.
    logging.getLogger("deltacat.utils.pyarrow").setLevel(logging.INFO)
    logging.getLogger("amazoncerts.cacerts_helpers").setLevel(logging.ERROR)
    return logger