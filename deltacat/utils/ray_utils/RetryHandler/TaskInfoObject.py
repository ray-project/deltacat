Class TaskInfoObject:
    def __init__(self, task_callable: Callable, task_input, num_retries: int, retry_delay: int): #what inputs do I need here
        self.task_callable = task_callable
        self.task_input = task_input
        #self.remote_task_options = ray_remote_task_options
        self.num_retries = num_retries
        self.retry_delay = retry_delay
        self.attempt_count = 0