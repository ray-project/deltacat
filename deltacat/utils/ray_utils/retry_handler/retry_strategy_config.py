from typing import List, Optional

from ray_manager.models.ray_remote_task_exception_retry_strategy_config import RayRemoteTaskExceptionRetryConfig

def get_retry_strategy_config_for_known_exception(exception: Exception, exception_retry_strategy_configs: List[RayRemoteTaskExceptionRetryConfig]) -> Optional[RayRemoteTaskExceptionRetryConfig]:
    for exception_retry_strategy_config in exception_retry_strategy_configs:
        if type(exception) == type(exception_retry_strategy_config.exception):
            return exception_retry_strategy_config

    for exception_retry_strategy_config in exception_retry_strategy_configs:
        if isinstance(exception, type(exception_retry_strategy_config.exception)):
            return exception_retry_strategy_config

    return None