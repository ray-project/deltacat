from __future__ import annotations
from typing import Any, Dict, List, cast, Optional
from deltacat.utils.ray_utils.retry_handler.ray_remote_tasks_batch_scaling_params import RayRemoteTasksBatchScalingParams
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
    try:
        taskObj.attempt_count += 1
        curr_attempt = taskObj.attempt_count
        if TaskContext is not None:
            # custom logic for checking if taskContext has progress and then use to detect stragglers
        #track time/progress in here
        logger.debug(f"Executing the submitted Ray remote task as part of attempt number: {current_attempt_number}")
        return taskObj.task_callable(taskObj.task_input)
    except (Exception) as exception:
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, taskObj.exception_retry_strategy_configs)
        if exception_retry_strategy_config is not None:
            return RayRemoteTaskExecutionError(exception_retry_strategy_config.exception, taskObj)


class RayTaskSubmissionHandler:
    """
    Starts execution of all given a list of Ray tasks with optional arguments: scaling strategy and straggler detection
    """
    def start_tasks_execution(self,
                              ray_remote_task_infos: List[TaskInfoObject],
                              scaling_strategy: Optional[BatchScalingStrategy] = None,
                              straggler_detection: Optional[StragglerDetectionInterface] = None,
                              task_context: Optional[TaskContext]) -> None:
        if scaling_strategy is None:
            scaling_strategy = RayRemoteTasksBatchScalingParams(len(ray_remote_task_infos))
        while scaling_strategy.hasNextBatch:
            current_batch = scaling_strategy.next_batch()
            for tasks in current_batch:
                #execute and retry and detect straggler if avail



            #use interface methods and data to detect stragglers in ray
        self.num_of_submitted_tasks = len(ray_remote_task_infos)
        self.current_batch_size = min(scaling_strategy.get_batch_size, self.num_of_submitted_tasks)
        self.num_of_submitted_tasks_completed = 0
        self.remaining_ray_remote_task_infos = ray_remote_task_infos
        self.batch_scaling_params = batch_scaling_params
        self.task_promise_obj_ref_to_task_info_map: Dict[Any, RayRemoteTaskInfo] = {}

        self.unfinished_promises: List[Any] = []
        logger.info(f"Starting the execution of {len(ray_remote_task_infos)} Ray remote tasks. Concurrency of tasks execution: {self.current_batch_size}")
        if straggler_detection is not None:
            #feed to non-detection only retry handler
            self.__wait_and_get_all_task_results(straggler_detection)
        else:
            self.__submit_tasks(self.remaining_ray_remote_task_infos[:self.current_batch_size])
            self.remaining_ray_remote_task_infos = self.remaining_ray_remote_task_infos[self.current_batch_size:]


    def __wait_and_get_all_task_results(self, straggler_detection: Optional[StragglerDetectionInterface]) -> List[Any]:
        return self.__get_task_results(self.num_of_submitted_tasks, straggler_detection)

    #Straggler detection will go in here
    def __get_task_results(self, num_of_results: int, straggler_detection: Optional[StragglerDetectionInterface]) -> List[Any]:
        if straggler_detection is not None:
            finished, unfinished = ray.wait(unfinished, num_of_results, straggler_detection.calc_timeout_val)
        else:
            finished, unfinished = ray.wait(unfinished, num_of_results)
        for finished in finished:
            finished_result = None
            try:
                finished_result = ray.get(finished)
            except (Exception) as exception:
                #if exception send to method handle_ray_exception to determine what to do and assign the corresp error
                finished_result = self.handle_ray_exception(exception=exception, ray_remote_task_info=self.task_promise_obj_ref_to_task_info_map[str(finished_promise)] )#evaluate the exception and return the error

            if finished_result and type(finished_result) == RayRemoteTaskExecutionError:
                finished_result = cast(RayRemoteTaskExecutionError, finished_result)
                exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(finished_result.exception,
                     finished_result.ray_remote_task_info.exception_retry_strategy_configs)
                if (exception_retry_strategy_config is None or finished_result.ray_remote_task_info.num_of_attempts > exception_retry_strategy_config.max_retry_attempts):
                    logger.error(f"The submitted task has exhausted all the maximum retries configured and finally throws exception - {finished_result.exception}")
                    raise finished_result.exception
                self.__update_ray_remote_task_options_on_exception(finished_result.exception, finished_result.ray_remote_task_info)
                self.unfinished_promises.append(self.__invoke_ray_remote_task(ray_remote_task_info=finished_result.ray_remote_task_info))
            else:
                successful_results.append(finished_result)
                del self.task_promise_obj_ref_to_task_info_map[str(finished_promise)]

        num_of_successful_results = len(successful_results)
        self.num_of_submitted_tasks_completed += num_of_successful_results
        self.current_batch_size -= num_of_successful_results

        self.__enqueue_new_tasks(num_of_successful_results)

        if num_of_successful_results < num_of_results:
            successful_results.extend(self.wait_and_get_task_results(num_of_results - num_of_successful_results))
            return successful_results
        else:
            return successful_results


    def __enqueue_new_tasks(self, num_of_tasks: int) -> None:
            new_tasks_submitted = self.remaining_ray_remote_task_infos[:num_of_tasks]
            num_of_new_tasks_submitted = len(new_tasks_submitted)
            self.__submit_tasks(new_tasks_submitted)
            self.remaining_ray_remote_task_infos = self.remaining_ray_remote_task_infos[num_of_tasks:]
            self.current_batch_size += num_of_new_tasks_submitted
            logger.info(f"Enqueued {num_of_new_tasks_submitted} new tasks. Current concurrency of tasks execution: {self.current_batch_size}, Current Task progress: {self.num_of_submitted_tasks_completed}/{self.num_of_submitted_tasks}")

    def __submit_tasks(self, ray_remote_task_infos: List[RayRemoteTaskInfo]) -> None:
        for ray_remote_task_info in ray_remote_task_infos:
            time.sleep(0.005)
            self.unfinished_promises.append(self.__invoke_ray_remote_task(ray_remote_task_info))

    def __invoke_ray_remote_task(self, ray_remote_task_info: RayRemoteTaskInfo) -> Any:
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

    def __update_ray_remote_task_options_on_exception(self, exception: Exception, ray_remote_task_info: RayRemoteTaskInfo):
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, ray_remote_task_info.exception_retry_strategy_configs)
        if exception_retry_strategy_config and ray_remote_task_info.ray_remote_task_options.memory:
            logger.info(f"Updating the Ray remote task options after encountering exception: {exception}")
            ray_remote_task_memory_multiply_factor = exception_retry_strategy_config.ray_remote_task_memory_multiply_factor
            ray_remote_task_info.ray_remote_task_options.memory *= ray_remote_task_memory_multiply_factor
            logger.info(f"Updated ray remote task options Memory: {ray_remote_task_info.ray_remote_task_options.memory}")

    def __handle_ray_exception(self, exception: Exception, ray_remote_task_info: RayRemoteTaskInfo) -> RayRemoteTaskExecutionError:
        logger.error(f"Ray remote task failed with {type(exception)} Ray exception: {exception}")
        if type(exception).__name__ ==