from __future__ import annotations
from typing import Any, Dict, List, cast, Optional
import ray
import time
from deltacat.utils.ray_utils.retry_handler.task_execution_error import TaskExecutionError
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.exception_util import get_retry_strategy_config_for_known_exception
from deltacat.utils.ray_utils.retry_handler.AIMD_based_batch_scaling_strategy import AIMDBasedBatchScalingStrategy
from deltacat.utils.ray_utils.retry_handler.failures.unexpected_ray_task_error import UnexpectedRayTaskError

from deltacat.utils.ray_utils.retry_handler.retry_task_default import RetryTaskDefault


@ray.remote
def submit_single_task(taskObj: TaskInfoObject, TaskContext: Optional[Interface] = None) -> Any:
    """
     Submits a single task for execution, handles any exceptions that may occur during execution,
     and applies appropriate retry strategies if they are defined.
    """
    try:
        #increment attempt count here with the dictionary/List
        return taskObj.task_callable(taskObj.task_input)
    except Exception as exception:
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, taskObj.task_exception_retry_config)
        if exception_retry_strategy_config is not None:
            return TaskExecutionError(exception_retry_strategy_config.exception, taskObj) #wrap exception if detected
        raise UnexpectedRayTaskError(str(exception))


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
        custom client batch scaling, straggler detection, and task context
        """
        self.failed_tasks = []
        self.unfinished_promises: List[Any] = []
        self.task_promise_obj_ref_to_task_info_map: Dict[Any, RayRemoteTaskInfo] = {} # dictionary promise object to task info object
        if scaling_strategy is None:
            scaling_strategy = AIMDBasedBatchScalingStrategy(ray_remote_task_infos, 2, 6, 1, 2,
                                                             0.5)  # default strategy initial_batch: 50, max_batch: 100, min_batch: 10, Additive: 2, MultDecrease: 0.5
        if retry_strategy is None:
            retry_strategy = RetryTaskDefault(max_retries=3)  # do we need retry interface?

        active_tasks = []
        self.attempts = {task.task_id: 0 for task in ray_remote_task_infos}
        results = []
        while scaling_strategy.has_next_batch():
            current_batch = scaling_strategy.next_batch()
            print("current batch size" + str(scaling_strategy.batch_size))
            self._submit_tasks(current_batch)  # all unfinished_promises added and enqueued
            for task in current_batch:
                active_tasks.append(task)  # maybe should be task_id can make this a dictionary instead and using the
            while active_tasks:
                completed_task = self._get_task_results(1)  # gets first finishing result success or failure and returns the result what does this method return
                if completed_task:
                    task_result = completed_task[0]
                    ray_obj = completed_task[1]
                    task_obj = self.task_promise_obj_ref_to_task_info_map[str(ray_obj)]
                    print("Task Result:" + str(task_result) + "Ray_obj" + str(ray_obj) + "task_obj" + str(task_obj) + str(task_obj.task_id))
                    if isinstance(task_result, TaskExecutionError):
                        scaling_strategy.mark_task_failed(task_result.task_info.task_id)
                        if retry_strategy.should_retry(task,
                                                       task_result.exception):  # dont need task just exception but hanlde_exception does this, maybe can take out and rely on retry capabilities in wait
                            #self.attempts[task.task_id] += 1
                            #self.ray_remote_task_infos.append(completed_task)
                            active_tasks.remove(task_result.task_info)
                            #active_tasks.append(task_result.task_info)
                            print("active tasks" + str(active_tasks))
                            for i in active_tasks:
                                print("this is the task_id" + str(i.task_id))
                    else:
                        print("task idasdasdasda" + str(task_obj.task_id))
                        scaling_strategy.mark_task_complete(task_obj.task_id)
                        active_tasks.remove(task_obj)
                        results.append(task_result)
                        print("active tasks" + str(active_tasks))

        for i in range(1,10):
            print ("task completness" + str(i) + str(scaling_strategy.is_task_completed(i)))
        return {
            "successful_results" : results,
            #"failed_tasks": self.failed_tasks
        }
        # if straggler_detection is not None:
        #    for task in active_tasks[:]:
        #       if straggler_detection.is_straggler(task, task_context):
        #          ray.cancel(task)
        #         active_tasks.remove(task)
        #        # If you want to re-add the cancelled stragglers to the task queue
        #       self.ray_remote_task_infos.append(task)

    def _wait_and_get_all_task_results(self) -> List[Any]:
        return self._get_task_results(self.num_of_submitted_tasks)

    def _get_task_results(self, num_of_results: int) -> List[Any]:
        """
        Gets results from a list of tasks to be executed, and catches exceptions to manage the retry strategy.
        Optional: Given a StragglerDetectionInterface, can detect and handle straggler tasks according to the client logic
        """
        if not self.unfinished_promises or num_of_results == 0:
            return []
        elif num_of_results > len(self.unfinished_promises):
            num_of_results = len(self.unfinished_promises)

        finished_promises, self.unfinished_promises = ray.wait(self.unfinished_promises, num_returns=num_of_results, timeout = 5.0)

        successful_results = []

        for finished in finished_promises:
            finished_result = None

            try:
                finished_result = ray.get(finished)  # if get returns an exception, detect it and go to retry strategy
            except (Exception) as exception:
                print(f"Exception message: {exception}")
                print(str(finished))
                finished_result = self._handle_ray_exception(exception=exception, ray_remote_task_info=
                    self.task_promise_obj_ref_to_task_info_map[str(finished)])  # evaluate the exception and return the error
                print("promisetoobjrefmap" + str(self.task_promise_obj_ref_to_task_info_map))
            if finished_result and type(finished_result) == TaskExecutionError:
                finished_result = cast(TaskExecutionError, finished_result)
                print("taskExecutionError attributes" + str(finished_result.task_info.task_id))
                exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(
                    finished_result.exception,
                    finished_result.task_info.task_exception_retry_config)
                #get num of attempts on id here
                curr_id = finished_result.task_info.task_id
                print("ATTEMEPTPTPTPSS COUNTNSNSNS" + str(self.attempts[curr_id]))
                if (exception_retry_strategy_config is None or (self.attempts[curr_id] > exception_retry_strategy_config.max_retry_attempts)): #or finished_result.task_info.num_of_attempts > exception_retry_strategy_config.max_retry_attempts):  # have to change num of attempts here to use the map I made in start_task_execution
                    #raise finished_result.exception
                    print("exception raised")
                    self.failed_tasks.append((curr_id, finished_result.exception))
                    return[finished_result, finished]
                else:
                    self.attempts[finished_result.task_info.task_id] += 1
                    self._update_ray_remote_task_options_on_exception(finished_result.exception, finished_result.task_info)
                    self.unfinished_promises.append(self._invoke_ray_remote_task(ray_remote_task_info=finished_result.task_info))
                #return[finished_result, finished]
                print("attempts map" + str(self.attempts))
            else:
                successful_results.append(finished_result)
                return[successful_results, finished]
                print(successful_results)
       #num_of_successful_results = len(successful_results)
        #self._enqueue_new_tasks(num_of_successful_results)

       # if num_of_successful_results < num_of_results:
            #successful_results.extend(self._get_task_results(num_of_results - num_of_successful_results))
           # return [successful_results, finished]
       # else:
          #  return [successful_results, finished]

    def _enqueue_new_tasks(self, num_of_tasks: int) -> None:
        """
        Helper method to submit a specified number of tasks
        """
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

        if ray_remote_task_info.ray_remote_task_options.memory:
            ray_remote_task_options_arguments['memory'] = ray_remote_task_info.ray_remote_task_options.memory

        if ray_remote_task_info.ray_remote_task_options.num_cpus:
            ray_remote_task_options_arguments['num_cpus'] = ray_remote_task_info.ray_remote_task_options.num_cpus

        if ray_remote_task_info.ray_remote_task_options.placement_group:
            ray_remote_task_options_arguments[
                'placement_group'] = ray_remote_task_info.ray_remote_task_options.placement_group

        ray_remote_task_promise_obj_ref = submit_single_task.options(**ray_remote_task_options_arguments).remote(
            taskObj=ray_remote_task_info)
        self.task_promise_obj_ref_to_task_info_map[str(ray_remote_task_promise_obj_ref)] = ray_remote_task_info
        #print("in invoke" + str(self.task_promise_obj_ref_to_task_info_map))

        return ray_remote_task_promise_obj_ref

    # replace with ray.options
    def _update_ray_remote_task_options_on_exception(self, exception: Exception,
                                                     ray_remote_task_info: TaskInfoObject):
        exception_retry_strategy_config = get_retry_strategy_config_for_known_exception(exception, ray_remote_task_info.task_exception_retry_config)
        if exception_retry_strategy_config and ray_remote_task_info.ray_remote_task_options.memory:
            ray_remote_task_memory_multiply_factor = exception_retry_strategy_config.ray_remote_task_memory_multiply_factor
            ray_remote_task_info.ray_remote_task_options.memory *= ray_remote_task_memory_multiply_factor

    # replace with own exceptions
    def _handle_ray_exception(self, exception: Exception,
                              ray_remote_task_info: RayRemoteTaskInfo) -> RayRemoteTaskExecutionError:
        print("in handle_ray" + type(exception).__name__)
        if type(exception).__name__ == "AWSSecurityTokenRateExceededException(RetryableError)":
            return TaskExecutionError(exception=AWSSecurityTokenRateExceededException(str(exception)),
                                      ray_remote_task_info=ray_remote_task_info)
