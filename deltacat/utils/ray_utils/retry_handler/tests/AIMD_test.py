import unittest
from deltacat.utils.ray_utils.retry_handler.AIMD_based_batch_scaling_strategy import AIMDBasedBatchScalingStrategy
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.ray_task_submission_handler import RayTaskSubmissionHandler


class AIMDTest(unittest.TestCase):
    def test_batch_scaling(self):
    # Initialize task_info_objects
    handler_instance = RayTaskSubmissionHandler()
    num_tasks = 15
    # Define a callable that simply marks the task as complete or failed based on task_outcomes
    def task_callable(task_info_object):
        if task_outcomes[task_info_object.task_id]:
            scaling_strategy.mark_task_complete(task_info_object)
            return "success"
        else:
            scaling_strategy.mark_task_failed(task_info_object)
            raise Exception("Task failed")

    task_outcomes = [True]*5 + [False] + [True]*5
    expected_batch_sizes = [1, 2, 3, 4, 5, 5, 2, 3, 4, 5, 5, 5, 5, 5, 5]

    # Assign the task_callable to each TaskInfoObject
    task_info_objects = [TaskInfoObject(i, task_callable, None, None, None) for i in range(num_tasks)]

    # Initialize AIMD-based batch scaling strategy
    scaling_strategy = AIMDBasedBatchScalingStrategy(task_info_objects, 1, 10, 1, 1, 0.5)

    # Run the tasks and collect the actual sequence of batch sizes
    actual_batch_sizes = []

    for _ in range(num_tasks):
        actual_batch_sizes.append(scaling_strategy.batch_size)

        # For simplicity, executing one task at a time
        for task_info in task_info_objects:
            task_info.task_callable(task_info)

    self.assertEqual(expected_batch_sizes, actual_batch_sizes)

def test_batch_scaling_max_cap(self):
    # Initialize task_info_objects
    handler_instance = RayTaskSubmissionHandler()
    num_tasks = 10

    # Define a callable that simply marks the task as complete
    def task_callable(task_info_object):
        scaling_strategy.mark_task_complete(task_info_object)
        return "success"

    # Assign the task_callable to each TaskInfoObject
    task_info_objects = [TaskInfoObject(i, task_callable, None, None, None, None) for i in range(num_tasks)]

    # Initialize AIMD-based batch scaling strategy
    scaling_strategy = AIMDBasedBatchScalingStrategy(task_info_objects, 1, 5, 1, 1, 0.5)

    # Here we expect all tasks to be successful
    task_outcomes = [True]*num_tasks
    # The expected batch sizes will cap at 5
    expected_batch_sizes = [1, 2, 3, 4, 5, 5, 5, 5, 5, 5]

    # Run the tasks and collect the actual sequence of batch sizes
    actual_batch_sizes = []

    for _ in range(num_tasks):
        actual_batch_sizes.append(scaling_strategy.batch_size)

        # For simplicity, executing one task at a time
        for task_info in task_info_objects:
            task_info.task_callable(task_info)

    self.assertEqual(expected_batch_sizes, actual_batch_sizes)

    def test_batch_scaling_min_cap(self):
        # Initialize task_info_objects
        num_tasks = 10
        task_info_objects = [TaskInfoObject(i, None, None, None, None, None) for i in range(num_tasks)]

        # Initialize AIMD-based batch scaling strategy
        # Starts with 5 task, max 10 tasks, increase by 1 additive, mult decrease by 0.5
        scaling_strategy = AIMDBasedBatchScalingStrategy(task_info_objects, 5, 10, 1, 1, 0.5)

        # Define a callable that simply marks the task as failed
        def task_callable(task_info_object):
            scaling_strategy.mark_task_failed(task_info_object)
            raise Exception("Task failed")

        # The expected batch sizes will bottom out at 1
        expected_batch_sizes = [5, 2, 1, 1, 1, 1, 1, 1, 1, 1]

        # Run the tasks and collect the actual sequence of batch sizes
        actual_batch_sizes = []
        for _ in range(num_tasks):
            actual_batch_sizes.append(scaling_strategy.batch_size)
            start_tasks_execution(scaling_strategy.next_batch(), task_callable, 3, 3)

        # Verify that the actual sequence of batch sizes matches the expected sequence
        self.assertEqual(expected_batch_sizes, actual_batch_sizes)

if __name__ == '__main__':
    unittest.main()

