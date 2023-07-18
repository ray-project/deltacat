"""
Default max retry attempts of Ray remote task
"""
DEFAULT_MAX_RAY_REMOTE_TASK_RETRY_ATTEMPTS = 3
"""
Default initial backoff before Ray remote task retry, in milli seconds
"""
DEFAULT_RAY_REMOTE_TASK_RETRY_INITIAL_BACK_OFF_IN_MS = 5000
"""
Default Ray remote task retry back off factor
"""
DEFAULT_RAY_REMOTE_TASK_RETRY_BACK_OFF_FACTOR = 2
"""
Default Ray remote task memory multiplication factor
"""
DEFAULT_RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR = 1
"""
Ray remote task memory multiplication factor for Ray out of memory error
"""
RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR_FOR_OUT_OF_MEMORY_ERROR = 2
"""
Default Ray remote task batch negative feedback back off in milli seconds
"""
DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BACK_OFF_IN_MS = 0
"""
Default Ray remote task batch positive feedback batch size additive increase
"""
DEFAULT_RAY_REMOTE_TASK_BATCH_POSITIVE_FEEDBACK_BATCH_SIZE_ADDITIVE_INCREASE = 0
"""
Default Ray remote task batch positive feedback batch size multiplicative decrease factor
"""
DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BATCH_SIZE_MULTIPLICATIVE_DECREASE_FACTOR = 1