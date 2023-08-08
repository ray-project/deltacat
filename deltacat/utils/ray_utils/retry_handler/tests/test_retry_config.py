import unittest
from unittest.mock import patch, MagicMock


from ray_handler import RayTaskSubmissionHandler, RayRemoteTaskExecutionError, TaskInfoObject

class TestRayTaskSubmissionHandler(unittest.TestCase):

    def setUp(self):
        self.handler = RayTaskSubmissionHandler()
        # Create mock tasks for testing
        self.tasks = [TaskInfoObject(i, None, None) for i in range(5)]

    @patch('ray_handler.submit_single_task')
    def test_all_tasks_succeed(self, mock_submit_task):
        # Mock successful task completion
        mock_submit_task.return_value = "success"

        # Start tasks execution
        self.handler.start_tasks_execution(self.tasks)

        # Assuming some mechanism to verify that all tasks completed successfully
        self.assertTrue(all(task.completed for task in self.tasks))

    @patch('ray_handler.submit_single_task')
    def test_task_failure_with_retry(self, mock_submit_task):
        # Simulate a failure for the third task
        def side_effect(taskObj, *args, **kwargs):
            if taskObj.task_id == 2:
                raise Exception("AWSSecurityTokenRateExceededException(RetryableError)")
            return "success"

        mock_submit_task.side_effect = side_effect

        # Start tasks execution
        with self.assertRaises(RayRemoteTaskExecutionError):
            self.handler.start_tasks_execution(self.tasks)

        # Assuming some mechanism to verify that the failed task was retried
        # Here, we are just checking that the task's attempt count was incremented
        self.assertEqual(self.tasks[2].attempt_count, 2)

if __name__ == '__main__':
    unittest.main()