from exceptions import Exception
class NonRetryableError(Exception):
    """
    Class represents a non-retryable error
    """
    def __init__(self, *args: object):
        super().__init__(*args)