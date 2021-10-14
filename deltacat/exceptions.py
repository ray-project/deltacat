class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class ConcurrentModificationError(Exception):
    pass
