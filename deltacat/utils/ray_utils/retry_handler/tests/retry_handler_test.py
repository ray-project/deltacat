import unittest
from unittest.mock import Mock, call
from ray_manager.models.ray_remote_task_exception_retry_strategy_config import RayRemoteTaskExceptionRetryConfig
from deltacat.utils.ray_utils.retry_handler.retry_task_default import RetryTaskDefault
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.task_execution_error import TaskExecutionError
from deltacat.utils.ray_utils.retry_handler.ray_task_submission_handler import RayTaskSubmissionHandler
from exceptions import RetryableError, NonRetryableError


class RetryHandlerTest(unittest.TestCase):
    def setUp(self):
        self.retryable_exception = RetryableError("This is a retryable error.")
        self.non_retryable_exception = NonRetryableError("This is a non-retryable error.")
        self.retry_config = RayRemoteTaskExceptionRetryConfig(
            exception=self.retryable_exception,
            retry_attempts=2,
            backoff_factor=2,
            backoff_initial_delay=1,
        )
        self.retry_handler = RetryTaskDefault(max_retries=self.retry_config.retry_attempts)
        self.task_info_object = TaskInfoObject(task_id="1", args=[], kwargs={})
        self.task_info_objects = [self.task_info_object]

    def test_retry_handler_retryable_exception(self):
        mock_task_execution = Mock()
        mock_task_execution.side_effect = [
            TaskExecutionError(self.retryable_exception, self.task_info_object),  # First attempt - fails
            TaskExecutionError(self.retryable_exception, self.task_info_object),  # Second attempt - fails
            None  # Third attempt - succeeds
        ]

        ray_task_handler = RayTaskSubmissionHandler(
            task_execution_func=mock_task_execution,
            task_info_objects=self.task_info_objects,
            exception_retry_strategy_configs=[self.retry_config],
            retry_handler=self.retry_handler,
        )
        ray_task_handler.start_task_execution()

        # Check that the task_execution_func was called the correct number of times (3 times in this case)
        self.assertEqual(mock_task_execution.call_count, 3)

    def test_retry_handler_non_retryable_exception(self):
        mock_task_execution = Mock()
        mock_task_execution.side_effect = TaskExecutionError(self.non_retryable_exception, self.task_info_object)  # Fails

        ray_task_handler = RayTaskSubmissionHandler(
            task_execution_func=mock_task_execution,
            task_info_objects=self.task_info_objects,
            exception_retry_strategy_configs=[self.retry_config],
            retry_handler=self.retry_handler,
        )
        ray_task_handler.start_task_execution()

        # Check that the task_execution_func was only called once
        self.assertEqual(mock_task_execution.call_count, 1)

if __name__ == '__main__':
    unittest.main()