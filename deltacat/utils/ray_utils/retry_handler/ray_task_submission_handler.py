from __future__ import annotations
from typing import Any, Dict, List, cast, Optional
import ray
import time
import logging
from deltacat import logs
from deltacat.utils.ray_utils.retry_handler.task_execution_error import TaskExecutionError
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.exception_util import get_retry_strategy_config_for_known_exception
from deltacat.utils.ray_utils.retry_handler.aimd_based_batch_scaling_strategy import AIMDBasedBatchScalingStrategy
from deltacat.utils.ray_utils.retry_handler.failures.unexpected_ray_task_error import UnexpectedRayTaskError
from deltacat.utils.ray_utils.retry_handler.retry_task_default import RetryTaskDefault
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def submit_single_task(taskObj: TaskInfoObject, TaskContext: Optional[Interface] = None) -> Any:
    """
     Submits a single task for execution, handles any exceptions that may occur during execution,
     and applies appropriate retry strategies if they are defined.
    """
    try:
        logger.debug(f"Executing the submitted ray task")
        return taskObj.task_callable(taskObj.task_input)
    except Exception as exception:
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, taskObj.task_exception_retry_config)
        logger.error(f"Submitted Ray task failed with exception - {exception}")
        if exception_retry_strategy_config is not None:
            return TaskExecutionError(exception_retry_strategy_config.exception, taskObj)
        raise UnexpectedRayTaskError(str(exception)) #self.assertRaises


class RayTaskSubmissionHandler:
    """
    Starts execution of all given a list of Ray tasks with optional arguments: scaling strategy and straggler detection
    """

    def start_tasks_execution(self,
                              ray_remote_task_infos: List[TaskInfoObject],
                              scaling_strategy: Optional[BatchScalingStrategy] = None,
                              straggler_detection: Optional[StragglerDetectionInterface] = None,
                              retry_strategy: Optional[RetryTaskInterface] = None,
                              task_context: Optional[TaskContext] = None) -> List:
        """
        Prepares and initiates the execution of a batch of tasks and can optionally support
        custom client batch scaling, straggler detection, and retry strategy
        """
        assert isinstance(ray_remote_task_infos, list)
        self.failed_tasks = {}
        self.unfinished_promises: List[Any] = []
        self.task_promise_obj_ref_to_task_info_map: Dict[Any, RayRemoteTaskInfo] = {}
        self.attempts = {task.task_id: 0 for task in ray_remote_task_infos}
        assert len(self.attempts) == len(ray_remote_task_infos)
        if scaling_strategy is None:
            scaling_strategy = AIMDBasedBatchScalingStrategy(ray_remote_task_infos, 2, 6, 1, 2, 0.5)  # Default Scaling Strategy: Initial_batch: 2, Max_Batch Size: 6, Min_Batch: 1, Additive: 2, Mult Decrease: 0.5
        if retry_strategy is None:
            retry_strategy = RetryTaskDefault(max_retries=3)
        active_tasks = []
        results = {}
        logger.info(f"Starting execution of {len(ray_remote_task_infos)}.")
        while scaling_strategy.has_next_batch():
            current_batch = scaling_strategy.next_batch()
            self._submit_tasks(current_batch)
            for task in current_batch:
                active_tasks.append(task)
            while active_tasks:
                initial_active_task_count = len(active_tasks)
                completed_task = self._get_task_results(1)
                if completed_task:
                    task_result = completed_task[0]
                    ray_obj = completed_task[1]
                    task_obj = self.task_promise_obj_ref_to_task_info_map[str(ray_obj)]
                    print("this is the task result" + str(task_result))
                    if task_result is not None:
                        if isinstance(task_result, TaskExecutionError):
                            scaling_strategy.mark_task_failed(task_result.task_info.task_id)
                            if retry_strategy.should_retry(task_result.exception):
                                active_tasks.remove(task_result.task_info)
                        else:
                            scaling_strategy.mark_task_complete(task_obj.task_id)
                            active_tasks.remove(task_obj)
                            results[task_obj.task_id] = task_result
                assert len(active_tasks) <= initial_active_task_count
        filtered_results = {task_id: result for task_id, result in results.items() if result != [None]}
        filtered_results_list = list(filtered_results.values())
        flattened_results_list = [item[0] for item in filtered_results_list if item]
        assert len(flattened_results_list) == len(ray_remote_task_infos) - len(list(self.failed_tasks.keys()))
        assert all(attempts_count >= 1 for attempts_count in self.attempts.values())
        return [flattened_results_list, list(self.failed_tasks.keys())]

    def _wait_and_get_all_task_results(self) -> List[Any]:
        return self._get_task_results(self.num_of_submitted_tasks)

    def _get_task_results(self, num_of_results: int) -> List[Any]:
        """
        Gets results from a list of tasks to be executed, and if exceptions are caught on the task level, they are immediately retried here
        """
        if not self.unfinished_promises or num_of_results == 0:
            return []
        elif num_of_results > len(self.unfinished_promises):
            num_of_results = len(self.unfinished_promises)
        finished_promises, self.unfinished_promises = ray.wait(self.unfinished_promises, num_returns=num_of_results)
        successful_results = []
        assert len(finished_promises) == num_of_results
        for finished in finished_promises:
            finished_result = None
            try:
                finished_result = ray.get(finished)
            except (Exception) as exception:
                saved_exception_to_map = exception
                finished_result = self._handle_ray_exception(exception=exception, ray_remote_task_info=
                self.task_promise_obj_ref_to_task_info_map[str(finished)])

            self.attempts[self.task_promise_obj_ref_to_task_info_map[str(finished)].task_id] += 1
            if finished_result is None:
                self.failed_tasks[self.task_promise_obj_ref_to_task_info_map[str(finished)].task_id] = saved_exception_to_map
            if finished_result and type(finished_result) == TaskExecutionError:
                finished_result = cast(TaskExecutionError, finished_result)
                exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(
                    finished_result.exception,
                    finished_result.task_info.task_exception_retry_config)
                curr_id = finished_result.task_info.task_id
                if exception_retry_strategy_config is None or (self.attempts[curr_id] > exception_retry_strategy_config.max_retry_attempts):
                    self.failed_tasks[curr_id] = finished_result.exception
                    return [finished_result, finished]
                else:
                    self.attempts[finished_result.task_info.task_id] += 1
                    self._update_ray_remote_task_options_on_exception(finished_result.exception, finished_result.task_info)
                    self.unfinished_promises.append(self._invoke_ray_remote_task(ray_remote_task_info=finished_result.task_info))
            else:
                successful_results.append(finished_result)
                return [successful_results, finished]

    def _enqueue_new_tasks(self, num_of_tasks: int) -> None:
        new_tasks_submitted = self.remaining_ray_remote_task_infos[:num_of_tasks]
        num_of_new_tasks_submitted = len(new_tasks_submitted)
        self._submit_tasks(new_tasks_submitted)
        self.remaining_ray_remote_task_infos = self.remaining_ray_remote_task_infos[num_of_tasks:]
        self.current_batch_size += num_of_new_tasks_submitted

    def _submit_tasks(self, info_objs: List[TaskInfoObject]) -> None:
        for info_obj in info_objs:
            time.sleep(0.005)
            self.unfinished_promises.append(self._invoke_ray_remote_task(ray_remote_task_info=info_obj))

    def _invoke_ray_remote_task(self, ray_remote_task_info: TaskInfoObject) -> Any:
        ray_remote_task_options_arguments = dict()

        if ray_remote_task_info.ray_remote_task_options["memory"]:
            ray_remote_task_options_arguments['memory'] = ray_remote_task_info.ray_remote_task_options.memory

        if ray_remote_task_info.ray_remote_task_options["num_cpus"]:
            ray_remote_task_options_arguments['num_cpus'] = ray_remote_task_info.ray_remote_task_options.num_cpus

        if ray_remote_task_info.ray_remote_task_options["placement_group"]:
            ray_remote_task_options_arguments[
                'placement_group'] = ray_remote_task_info.ray_remote_task_options.placement_group
        if ray_remote_task_info.ray_remote_task_options["scheduling_strategy"]:
            ray_remote_task_options_arguments[
                'scheduling_strategy'] = ray_remote_task_info.ray_remote_task_options.scheduling_strategy

        ray_remote_task_promise_obj_ref = submit_single_task.options(**ray_remote_task_options_arguments).remote(taskObj=ray_remote_task_info)
        self.task_promise_obj_ref_to_task_info_map[str(ray_remote_task_promise_obj_ref)] = ray_remote_task_info
        return ray_remote_task_promise_obj_ref

    def _update_ray_remote_task_options_on_exception(self, exception: Exception,
                                                     ray_remote_task_info: TaskInfoObject):
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception,
                                                                                        ray_remote_task_info.task_exception_retry_config)
        if exception_retry_strategy_config and ray_remote_task_info.ray_remote_task_options["memory"]:
            ray_remote_task_memory_multiply_factor = exception_retry_strategy_config.ray_remote_task_memory_multiply_factor
            ray_remote_task_info.ray_remote_task_options["memory"] *= ray_remote_task_memory_multiply_factor

    def _handle_ray_exception(self, exception: Exception,
                              ray_remote_task_info: RayRemoteTaskInfo) -> RayRemoteTaskExecutionError:
        if type(exception).__name__ == "AWSSecurityTokenRateExceededException(RetryableError)":
            return TaskExecutionError(exception=AWSSecurityTokenRateExceededException(str(exception)),
                                      ray_remote_task_info=ray_remote_task_info)

