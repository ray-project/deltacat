import ray
import time
import logging
from typing import List, Callable
from ray.types import ObjectRef
from RetryExceptions.retryable_exception import RetryableException
from RetryExceptions.non_retryable_exception import NonRetryableException
from RetryExceptions.TaskInfoObject import TaskInfoObject

#inputs: task_callable, task_input, ray_remote_task_options, exception_retry_strategy_configs
#include a seperate class for errors: break down into retryable and non-retryable
#seperate class to put info in a way that the retry class can handle: ray retry task info

#This is what specifically retries a single task
@ray.remote
def submit_single_task(taskObj: TaskInfoObject) -> Any:
    try:
        taskObj.attempt_count += 1
        curr_attempt = taskObj.attempt_count
        return tackObj.task_callable(taskObj.task_input)
    except (Exception) as exception:
        # if exception is detected we want to figure out how to handle it
        #pass to a new method that handles exception strategy
        #retry_strat = ...exception_retry_strategy_configs
        retry_config = get_retry_strategy() #need to come up with fields needed for this
        if retry_config is not None:
            return the exception that retry_config detected



class RetryHandler:
    #given a list of tasks that are failing, we want to classify the error messages and redirect the task
    #depending on the exception type using a wrapper
    #wrapper function that before execution, checks what exception is being thrown and go to second method to
    #commence retrying

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
                finished_result = #evaluate the exception and return the error

            if finished_result == RetryableException:
                #feed into submit_single_task
            else:



    def handle_ray_exception(self, exception: Exception, TaskInfo: TaskInfoObject) -> Error:
        #will compare the exception with known exceptions and determine way to handle it based off that
        #if RayOOM Error then: raise that error

    def get_retry_strategy()