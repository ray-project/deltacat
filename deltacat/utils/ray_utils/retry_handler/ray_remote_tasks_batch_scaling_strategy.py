from deltacat.utils.ray_utils.retry_handler.batch_scaling_interface import BatchScalingInterface
class RayRemoteTasksBatchScalingStrategy(BatchScalingInterface):
    """
    Default batch scaling parameters for if the client does not provide their own batch_scaling parameters
    """

    def init_tasks(self)-> None:
            """
            Setup AIMD scaling for the batches as the default
            """
        pass

    def next_batch(self) -> List:
        """
        Returns the list of tasks included in the next batch of whatever size based on AIMD
        """

        pass

    def has_next_batch(self) -> bool:
        """
        If there are no more tasks to execute that can not create a batch, return False
        """
        pass