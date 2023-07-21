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
    a. TaskContext (progressNotifier - (send_heartbeat, send_progress, get_progress), timeout_time) from StragglerDetectionInterface
2. Detecting stragglers
   Given the straggler detection algorithm fed in by the client, the method get_timeout_val will be used to determine how
   long the task will run before it is considered a straggler. The logic for this must be provided by the client internally.
3. Retrying retryable exceptions
   a. Within the failure directory, there are common errors that are retryable and when detected as an instance 
    of the retryable class, will cause the task to be retried through submitting the task. 

The client can provide these inputs to fulfil the following use cases:

Given a list of 1000 tasks, we will first scale each batch to a reasonable size and run the retry and detection on each batch 
