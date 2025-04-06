from typing import List, Optional
from deltacat.utils.ray_utils.retry_handler.task_exception_retry_config import TaskExceptionRetryConfig
def get_retry_strategy_config_for_known_exception(exception: Exception,
                                                  exception_retry_strategy_configs: List[TaskExceptionRetryConfig]) -> Optional[TaskExceptionRetryConfig]:
    """
    Checks whether the exception seen is recognized as a retryable error or not
    """
    for exception_retry_strategy_config in exception_retry_strategy_configs:
        if type(exception) == type(exception_retry_strategy_config.exception):
            return exception_retry_strategy_config

    for exception_retry_strategy_config in exception_retry_strategy_configs:
        if isinstance(exception, type(exception_retry_strategy_config.exception)):
            return exception_retry_strategy_config

    return None