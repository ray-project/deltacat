import logging
import deltacat.logs

logs.configure_deltacat_logger(logging.getLogger(__name__))

__version__ = "0.1.0"

__all__ = [
    "__version__",
]
