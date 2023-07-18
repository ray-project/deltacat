from __future__ import annotations
from typing import Any, Dict, List, cast
from deltacat.utils.ray_utils.retry_handler.ray_remote_tasks_batch_scaling_params import RayRemoteTasksBatchScalingParams
#import necessary errors here
import ray
import time
import logging
from deltacat.utils.ray_utils.retry_handler.logger import configure_logger
from deltacat.utils.ray_utils.retry_handler.task_execution_error import RayRemoteTaskExecutionError
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.retry_strategy_config import get_retry_strategy_config_for_known_exception

logger = configure_logger(logging.getLogger(__name__))

import ray
import time
import logging
from typing import Any, Dict, List, cast
from ray.types import ObjectRef
from RetryExceptions.retryable_exception import RetryableException
from RetryExceptions.non_retryable_exception import NonRetryableException
from RetryExceptions.TaskInfoObject import TaskInfoObject

#inputs: task_callable, task_input, ray_remote_task_options, exception_retry_strategy_configs
#include a seperate class for errors: break down into retryable and non-retryable
#seperate class to put info in a way that the retry class can handle: ray retry task info

#This is what specifically retries a single task
@ray.remote
def submit_single_task(taskObj: TaskInfoObject, progressNotifier: Optional[NotificationInterface] = None) -> Any:
    try:
        taskObj.attempt_count += 1
        curr_attempt = taskObj.attempt_count
        if progressNotifier is not None:
            #method call to straggler detection using notifier
        logger.debug(f"Executing the submitted Ray remote task as part of attempt number: {current_attempt_number}")
        return tackObj.task_callable(taskObj.task_input)
    except (Exception) as exception:
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, task_info_object.exception_retry_strategy_configs)
        #pass to a new method that handles exception strategy
        #retry_strat = ...exception_retry_strategy_configs
        if exception_retry_strategy_config is not None:
            return RayRemoteTaskExecutionError(exception_retry_strategy_config.exception, task_info_object)




class RetryHandler:
    #given a list of tasks that are failing, we want to classify the error messages and redirect the task
    #depending on the exception type using a wrapper
    #wrapper function that before execution, checks what exception is being thrown and go to second method to
    #commence retrying
    def execute_task(self, ray_remote_task_info: RayRemoteTaskInfo) -> Any:
            self.start_tasks_execution([ray_remote_task_info])
            return self.wait_and_get_all_task_results()[0]

    """
    Starts execution of all given Ray remote tasks
    """
    def start_tasks_execution(self, ray_remote_task_infos: List[TaskInfoObject]) -> None:
        self.start_tasks_execution_in_batches(ray_remote_task_infos, RayRemoteTasksBatchScalingParams(initial_batch_size=len(ray_remote_task_infos)))

    """
    Starts execution of given Ray remote tasks in batches depending on the given Batch scaling params
    """
    def start_tasks_execution_in_batches(self, ray_remote_task_infos: List[RayRemoteTaskInfo], batch_scaling_params: RayRemoteTasksBatchScalingParams) -> None:
        self.num_of_submitted_tasks = len(ray_remote_task_infos)
        self.current_batch_size = min(batch_scaling_params.initial_batch_size, self.num_of_submitted_tasks)
        self.num_of_submitted_tasks_completed = 0
        self.remaining_ray_remote_task_infos = ray_remote_task_infos
        self.batch_scaling_params = batch_scaling_params
        self.task_promise_obj_ref_to_task_info_map: Dict[Any, RayRemoteTaskInfo] = {}

        self.unfinished_promises: List[Any] = []
        logger.info(f"Starting the execution of {len(ray_remote_task_infos)} Ray remote tasks. Concurrency of tasks execution: {self.current_batch_size}")
        self.__submit_tasks(self.remaining_ray_remote_task_infos[:self.current_batch_size])
        self.remaining_ray_remote_task_infos = self.remaining_ray_remote_task_infos[self.current_batch_size:]


    def wait_and_get_all_task_results(self) -> List[Any]:
        return self.wait_and_get_task_results(self.num_of_submitted_tasks)

    def get_task_results(self, num_of_results: int) -> List[Any]:
    #implement wrapper here that before execution will try catch an exception
        #get what tasks we need to run our execution on
        finished, unfinished = ray.wait(unfinished, num_of_results)
        #assuming we have the tasks we want to get results of
        for finished in finished:
            finished_result = None
            try:
                finished_result = ray.get(finished)
            except (Exception) as exception:
                #if exception send to method handle_ray_exception to determine what to do and assign the corresp error
                finished_result = self.handle_ray_exception(exception=exception, TaskInfoObject = )#evaluate the exception and return the error

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
            self.unfinished_promises.append(self.__invoke_ray_remote_task(ray_remote_task_info=ray_remote_task_info))

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
        if type(exception).__name__ == "RayTaskError(UnexpectedRayTaskError)":
            raise UnexpectedRayTaskError(str(exception))
        elif type(exception).__name__ == "RayTaskError(RayOutOfMemoryError)":
            return RayRemoteTaskExecutionError(exception=RayOutOfMemoryError(str(exception)), ray_remote_task_info=ray_remote_task_info)
        elif type(exception) == ray.exceptions.OwnerDiedError:
            return RayRemoteTaskExecutionError(exception=RayOwnerDiedError(str(exception)), ray_remote_task_info=ray_remote_task_info)
        elif type(exception) == ray.exceptions.WorkerCrashedError:
            return RayRemoteTaskExecutionError(exception=RayWorkerCrashedError(str(exception)), ray_remote_task_info=ray_remote_task_info)
        elif type(exception) == ray.exceptions.LocalRayletDiedError:
            return RayRemoteTaskExecutionError(exception=RayLocalRayletDiedError(str(exception)), ray_remote_task_info=ray_remote_task_info)
        elif type(exception) == ray.exceptions.RaySystemError:
            return RayRemoteTaskExecutionError(exception=RaySystemError(str(exception)), ray_remote_task_info=ray_remote_task_info)

        raise UnexpectedRayPlatformError(str(exception))