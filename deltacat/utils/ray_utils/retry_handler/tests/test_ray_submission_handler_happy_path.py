import unittest
import ray
from deltacat.utils.ray_utils.retry_handler.ray_task_submission_handler import RayTaskSubmissionHandler
from deltacat.utils.ray_utils.retry_handler.tests.task_util import square_num_ray_task
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject


class TestRayTaskSubmissionHandlerHappyPath(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.shutdown()
        ray.init(local_mode=True)

    def setUp(self):
        self.handler = RayTaskSubmissionHandler()
        task_input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        self.tasks = [TaskInfoObject(i, square_num_ray_task, i) for i in
                      task_input_list]

    def test_get_task_results_successes(self):
        results = self.handler.start_tasks_execution(self.tasks)
        expected_successful_results = [i ** 2 for i in range(1, 14)]
        expected_failed_tasks_ids = []
        expected_output = [expected_successful_results, expected_failed_tasks_ids]
        self.assertEqual(sorted(results[0]), sorted(expected_successful_results))
        self.assertEqual(sorted(results[1]), sorted(expected_failed_tasks_ids))
        self.assertTrue(all(value == 1 for value in self.handler.attempts.values()))


if __name__ == '__main__':
    unittest.main()
