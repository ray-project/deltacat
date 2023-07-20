class NonRetryableError(RuntimeError):
"""
Class represents a non-retryable error
"""

    def __init__(self, *args:object) --> None:
        super().__init__(*args)
