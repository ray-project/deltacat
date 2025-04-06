import unittest
from unittest.mock import MagicMock
from deltacat.utils.ray_utils.retry_handler.aimd_based_batch_scaling_strategy import AIMDBasedBatchScalingStrategy
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.tests.task_util import square_num_ray_task


class TestAIMDBatchScaling(unittest.TestCase):

    def setUp(self):
        # Setup commonly used variables here
        # set up a taskInfoObject with a mocked callable, etc
        # create multiple of these
        task_input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.tasks = [TaskInfoObject(f"task_{i}", square_num_ray_task, i) for i in task_input_list]  # id:1, callable:
        self.batch_strategy = AIMDBasedBatchScalingStrategy(self.tasks, 2, 10, 1, 2, 0.5)
        # set up instance of handler

    def test_has_next_batch(self):
        for _ in range(5):
            self.assertTrue(self.batch_strategy.has_next_batch())
            _ = self.batch_strategy.next_batch()

        self.assertFalse(self.batch_strategy.has_next_batch())

    def test_next_batch(self):
        initial_index = self.batch_strategy.batch_index
        batch = self.batch_strategy.next_batch()
        self.assertEqual(len(batch), 2)

    def test_mark_task_complete(self):
        # assuming we call a success, want to see that completion dictionary is updated
        # batch size gets updated
        initial_batch_size = 2
        self.assertEqual(self.batch_strategy.batch_size, initial_batch_size)
        task_to_complete = self.tasks[0]
        self.batch_strategy.mark_task_complete(task_to_complete.task_id)
        self.assertTrue(
            self.batch_strategy.is_task_completed(task_to_complete.task_id))  # task id should be marked as completed
        self.assertEqual(self.batch_strategy.batch_size, 4)  # batch should increase by 2

    def test_mark_task_failed(self):
        initial_batch_size = 2
        self.assertEqual(self.batch_strategy.batch_size, initial_batch_size)
        task_to_fail = self.tasks[0]
        self.batch_strategy.mark_task_failed(task_to_fail.task_id)
        self.assertFalse(self.batch_strategy.is_task_completed(task_to_fail.task_id))
        self.assertEqual(self.batch_strategy.batch_size, 1)


if __name__ == '__main__':
    unittest.main()
