This module represents a straggler detection and retry handler framework 

Within retry_strategy_config.py, the client can provide 3 parameters to start_tasks_execution to perform retries and detect stragglers 
Params:
1. ray_remote_task_info: A list of Ray task objects 
2. scaling_strategy: Batch scaling parameters for how many tasks to execute per batch (Optional)
    a. If not provided, a default AIMD (additive increase, multiplicative decrease) strategy will be assigned for retries
3. straggler_detection: Client-provided class that holds logic for how they want to detect straggler tasks (Optional)
   a. Client algorithm must inherit the interface for detection which will be used in wait_and_get_results

Use cases:
1. Notifying progress
    This will be done through ProgressNotifierInterface. The client can use has_progress and send_progress
    to recieve updates on task level progress. This can be an SNSQueue or any type of indicator the client may choose.
2. Detecting stragglers
   Given the straggler detection algorithm implemented by StragglerDetectionInterface, the method is_straggler will inform 
   the customer if the current node is a straggler according to their own logic and proving them with TaskContext, the information 
   they might need to make that decision.
3. Retrying retryable exceptions
   Within the failure directory, there are common errors that are retryable and when detected as an instance 
   of the retryable class, will cause the task to be retried when the exception is caught. If the client would like
   to create their own exceptions to be handles, they can create a class that is an extension of retryable_error or 
   non_retryable_error and the framework should handle it based on the configuration strategy.




