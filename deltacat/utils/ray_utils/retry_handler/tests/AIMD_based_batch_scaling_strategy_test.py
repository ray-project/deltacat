import unittest
from unittest.mock import MagicMock
from deltacat.compute.compactor.compactor import start_tasks_execution
from deltacat.compute.compactor.models.task_info import TaskInfoObject
from deltacat.compute.compactor.models.task_info import AIMDBasedBatchScalingStrategy

class TestAIMDBatchScaling(unittest.TestCase):

    def test_batch_scaling(self):
        # Initialize task_info_objects
        num_tasks = 15
        task_info_objects = [TaskInfoObject(i, None, None, None, None, None) for i in range(num_tasks)]

        # Initialize AIMD-based batch scaling strategy
        scaling_strategy = AIMDBasedBatchScalingStrategy(task_info_objects, 1, 10, 1, 1, 0.5) #

        # Define a callable that simply marks the task as complete or failed based on task_outcomes
        def task_callable(task_info_object):
            if task_outcomes[task_info_object.task_id]:
                scaling_strategy.mark_task_complete(task_info_object)
                return "success"
            else:
                scaling_strategy.mark_task_failed(task_info_object)
                raise Exception("Task failed")

        # Define the sequence of task successes and failures
        task_outcomes = [True]*5 + [False] + [True]*5

        # Define the expected sequence of batch sizes
        expected_batch_sizes = [1, 2, 3, 4, 5, 5, 2, 3, 4, 5, 5, 5, 5, 5, 5]

        # Run the tasks and collect the actual sequence of batch sizes
        actual_batch_sizes = []
        for _ in range(num_tasks):
            actual_batch_sizes.append(scaling_strategy.batch_size)
            start_tasks_execution(scaling_strategy.next_batch(), task_callable, 3, 3)

        # Verify that the actual sequence of batch sizes matches the expected sequence
        self.assertEqual(expected_batch_sizes, actual_batch_sizes)

if __name__ == '__main__':
    unittest.main()