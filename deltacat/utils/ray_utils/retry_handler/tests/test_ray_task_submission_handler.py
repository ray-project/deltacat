import unittest
from unittest.mock import Mock, patch

import ray
from deltacat.utils.ray_utils.retry_handler.ray_task_submission_handler import RayTaskSubmissionHandler

from deltacat.utils.ray_utils.retry_handler.tests.task_util import square_num_ray_task_with_failures
from deltacat.utils.ray_utils.retry_handler.tests.task_util import square_num_ray_task
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject

from deltacat.utils.ray_utils.retry_handler.failures.aws_security_token_rate_exceeded_exception import \
    AWSSecurityTokenRateExceededException




class TestRayTaskSubmissionHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.shutdown()
        ray.init(local_mode=True)

    def setUp(self):
        self.handler = RayTaskSubmissionHandler()
        # lets set up 10 TaskInfoObjects here
        task_input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.tasks = [TaskInfoObject(i, square_num_ray_task_with_failures, i) for i in
                      task_input_list]  # id:1-10, callable:square_num_ray_task, input 1-10

    def test_get_task_results_successes(self):
        results = self.handler.start_tasks_execution(self.tasks)
        expected_successes = [[1], []]

        self.assertEqual(results, expected_successes)

    #def test_get_task_results_failures(self):
        #results = self.handler.start_tasks_execution(self.tasks)
        #expected_successes = [i **2 for i in range(1,6)]
        #print(results)
        #print(expected_successes)
        #self.assertEqual(results, expected_successes)

    # when there are failures
    # calling correct APIs
    # when there are all successes


if __name__ == '__main__':
    unittest.main()
