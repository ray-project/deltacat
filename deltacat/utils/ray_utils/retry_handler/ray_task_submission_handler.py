from __future__ import annotations
from typing import Any, Dict, List, cast, Optional
from deltacat.utils.ray_utils.retry_handler.ray_remote_tasks_batch_scaling_strategy import RayRemoteTasksBatchScalingStrategy
import ray
import time
import logging
from deltacat.logs import configure_logger
from deltacat.utils.ray_utils.retry_handler.task_execution_error import RayRemoteTaskExecutionError
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.retry_strategy_config import get_retry_strategy_config_for_known_exception

logger = configure_logger(logging.getLogger(__name__))

@ray.remote
def submit_single_task(taskObj: TaskInfoObject, TaskContext: Optional[Interface] = None) -> Any:
    """
     Submits a single task for execution, handles any exceptions that may occur during execution,
     and applies appropriate retry strategies if they are defined.
    """
    try:
        taskObj.attempt_count += 1
        curr_attempt = taskObj.attempt_count
        logger.debug(f"Executing the submitted Ray remote task as part of attempt number: {current_attempt_number}")
        return taskObj.task_callable(taskObj.task_input)
    except (Exception) as exception:
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, taskObj.exception_retry_strategy_configs)
        if exception_retry_strategy_config is not None:
            return RayRemoteTaskExecutionError(exception_retry_strategy_config.exception, taskObj)

        logger.error(f"The exception thrown by submitted Ray task during attempt number: {current_attempt_number} is non-retryable or unexpected, hence throwing Non retryable exception: {exception}")
        raise UnexpectedRayTaskError(str(exception))

class RayTaskSubmissionHandler:
    """
    Starts execution of all given a list of Ray tasks with optional arguments: scaling strategy and straggler detection
    """
    def start_tasks_execution(self,
                              ray_remote_task_infos: List[TaskInfoObject],
                              scaling_strategy: Optional[BatchScalingStrategy] = None,
                              straggler_detection: Optional[StragglerDetectionInterface] = None,
                              task_context: Optional[TaskContext]) -> None:
        """
        Prepares and initiates the execution of a batch of tasks and can optionally support
        custom client batch scaling, straggler detection, and task context
        """
        if scaling_strategy is None:
            scaling_strategy = RayRemoteTasksBatchScalingStrategy(ray_remote_task_infos)

        if straggler_detection is not None:
            while scaling_strategy.hasNextBatch():
                current_batch = scaling_strategy.next_batch()
                for task in current_batch:
                    if straggler_detection.isStraggler(task):
                        ray.cancel(task)
                    else:
                        self._submit_tasks(task)

    def _wait_and_get_all_task_results(self, straggler_detection: Optional[StragglerDetectionInterface]) -> List[Any]:
        return self._get_task_results(self.num_of_submitted_tasks, straggler_detection)

    def _get_task_results(self, num_of_results: int, straggler_detection: Optional[StragglerDetectionInterface]) -> List[Any]:
        """
        Gets results from a list of tasks to be executed, and catches exceptions to manage the retry strategy.
        Optional: Given a StragglerDetectionInterface, can detect and handle straggler tasks according to the client logic
        """
        if not self.unfinished_promises or num_of_results == 0:
            return []
        elif num_of_results > len(self.unfinished_promises):
            num_of_results = len(self.unfinished_promises)

        finished, self.unfinished_promises = ray.wait(self.unfinished_promises, num_of_results)
        successful_results = []

        for finished in finished:
            finished_result = None
            try:
                finished_result = ray.get(finished)
            except (Exception) as exception:
                #if exception send to method handle_ray_exception to determine what to do and assign the corresp error
                finished_result = self._handle_ray_exception(exception=exception, ray_remote_task_info=self.task_promise_obj_ref_to_task_info_map[str(finished_promise)] )#evaluate the exception and return the error

            if finished_result and type(finished_result) == RayRemoteTaskExecutionError:
                finished_result = cast(RayRemoteTaskExecutionError, finished_result)

                if straggler_detection and straggler_detection.isStraggler(finished_result):
                    ray.cancel(finished_result)
                exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(finished_result.exception,
                     finished_result.ray_remote_task_info.exception_retry_strategy_configs)
                if (exception_retry_strategy_config is None or finished_result.ray_remote_task_info.num_of_attempts > exception_retry_strategy_config.max_retry_attempts):
                    logger.error(f"The submitted task has exhausted all the maximum retries configured and finally throws exception - {finished_result.exception}")
                    raise finished_result.exception
                self._update_ray_remote_task_options_on_exception(finished_result.exception, finished_result.ray_remote_task_info)
                self.unfinished_promises.append(self._invoke_ray_remote_task(ray_remote_task_info=finished_result.ray_remote_task_info))
            else:
                successful_results.append(finished_result)
                del self.task_promise_obj_ref_to_task_info_map[str(finished_promise)]

        num_of_successful_results = len(successful_results)
        self.num_of_submitted_tasks_completed += num_of_successful_results
        self.current_batch_size -= num_of_successful_results

        self._enqueue_new_tasks(num_of_successful_results)

        if num_of_successful_results < num_of_results:
            successful_results.extend(self._get_task_results(num_of_results - num_of_successful_results))
            return successful_results
        else:
            return successful_results


    def _enqueue_new_tasks(self, num_of_tasks: int) -> None:
        """
        Helper method to submit a specified number of tasks
        """
        new_tasks_submitted = self.remaining_ray_remote_task_infos[:num_of_tasks]
        num_of_new_tasks_submitted = len(new_tasks_submitted)
        self._submit_tasks(new_tasks_submitted)
        self.remaining_ray_remote_task_infos = self.remaining_ray_remote_task_infos[num_of_tasks:]
        self.current_batch_size += num_of_new_tasks_submitted
        logger.info(f"Enqueued {num_of_new_tasks_submitted} new tasks. Current concurrency of tasks execution: {self.current_batch_size}, Current Task progress: {self.num_of_submitted_tasks_completed}/{self.num_of_submitted_tasks}")

    def _submit_tasks(self, ray_remote_task_infos: List[RayRemoteTaskInfo]) -> None:
        for ray_remote_task_info in ray_remote_task_infos:
            time.sleep(0.005)
            if self.straggler_detection and self.straggler_detection.is_straggler(ray_remote_task_info):
                ray.cancel(ray_remote_task_info)
            else:
            self.unfinished_promises.append(self._invoke_ray_remote_task(ray_remote_task_info))
    #replace with ray.options
    def _invoke_ray_remote_task(self, ray_remote_task_info: RayRemoteTaskInfo) -> Any:
        ray_remote_task_options_arguments = dict()

        if ray_remote_task_info.ray_remote_task_options.memory:
            ray_remote_task_options_arguments['memory'] = ray_remote_task_info.ray_remote_task_options.memory

        if ray_remote_task_info.ray_remote_task_options.num_cpus:
            ray_remote_task_options_arguments['num_cpus'] = ray_remote_task_info.ray_remote_task_options.num_cpus

        if ray_remote_task_info.ray_remote_task_options.placement_group:
            ray_remote_task_options_arguments['placement_group'] = ray_remote_task_info.ray_remote_task_options.placement_group

        ray_remote_task_promise_obj_ref = submit_single_task.options(**ray_remote_task_options_arguments).remote(ray_remote_task_info=ray_remote_task_info)
        self.task_promise_obj_ref_to_task_info_map[str(ray_remote_task_promise_obj_ref)] = ray_remote_task_info

        return ray_remote_task_promise_obj_ref

    #replace with ray.options
    def _update_ray_remote_task_options_on_exception(self, exception: Exception, ray_remote_task_info: RayRemoteTaskInfo):
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, ray_remote_task_info.exception_retry_strategy_configs)
        if exception_retry_strategy_config and ray_remote_task_info.ray_remote_task_options.memory:
            logger.info(f"Updating the Ray remote task options after encountering exception: {exception}")
            ray_remote_task_memory_multiply_factor = exception_retry_strategy_config.ray_remote_task_memory_multiply_factor
            ray_remote_task_info.ray_remote_task_options.memory *= ray_remote_task_memory_multiply_factor
            logger.info(f"Updated ray remote task options Memory: {ray_remote_task_info.ray_remote_task_options.memory}")
    #replace with own exceptions
    def _handle_ray_exception(self, exception: Exception, ray_remote_task_info: RayRemoteTaskInfo) -> RayRemoteTaskExecutionError:
        logger.error(f"Ray remote task failed with {type(exception)} Ray exception: {exception}")
        if type(exception).__name__ == "AWSSecurityTokenRateExceededException(RetryableError)"