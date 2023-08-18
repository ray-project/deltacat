import unittest
import ray
from deltacat.utils.ray_utils.retry_handler.ray_task_submission_handler import RayTaskSubmissionHandler
from deltacat.utils.ray_utils.retry_handler.tests.task_util import square_num_ray_task_with_failures
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject


class TestRayTaskSubmissionHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.shutdown()
        ray.init(local_mode=True)

    def setUp(self):
        self.handler = RayTaskSubmissionHandler()
        task_input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        self.tasks = [TaskInfoObject(i, square_num_ray_task_with_failures, i) for i in
                      task_input_list]


    def test_get_task_results_failures_and_successes_and_nonRetryable(self):
        results = self.handler.start_tasks_execution(self.tasks)
        expected_successful_results = [i ** 2 for i in range(2, 14) if i not in [1, 5, 10, 11]]
        expected_failed_tasks_ids = [1, 5, 10, 11]
        expected_3_retries = [1, 5, 10]
        expected_output = [expected_successful_results, expected_failed_tasks_ids]
        self.assertEqual(sorted(results[0]), sorted(expected_successful_results))
        self.assertEqual(sorted(results[1]), sorted(expected_failed_tasks_ids))
        self.assertTrue(all(self.handler.attempts[task_id] == 3 for task_id in expected_3_retries))
        other_task_ids = set(range(1, 14)) - set(expected_3_retries)
        self.assertTrue(all(self.handler.attempts[task_id] == 1 for task_id in other_task_ids))

    #def test_start_task_execution_raises_exception(self):
        #with self.assertRaises(UnexpectedRayTaskError) as context:
            #self.handler.start_tasks_execution(self.tasks)
        #captured_exception = context.exception
        #self.assertIsInstance(captured_exception, UnexpectedRayTaskError)
        #self.assertIsInstance(captured_exception.args[0], UnexpectedRayTaskError)

if __name__ == '__main__':
    unittest.main()
