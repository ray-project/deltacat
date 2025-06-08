"""
Mock module that provides storage-specific error categorization functions for main storage testing.
"""

from deltacat.tests.utils.exceptions import (
    InvalidNamespaceError,
    MainStorageValidationError,
)


def can_categorize(e: BaseException, **kwargs) -> bool:
    """
    Mock implementation of can_categorize for main storage testing.
    Returns True if the input error can be categorized by main storage.
    """
    if isinstance(e, InvalidNamespaceError):
        return True
    else:
        return False


def raise_categorized_error(e: BaseException, **kwargs):
    """
    Mock implementation of raise_categorized_error for main storage testing.
    Converts categorizable errors to their main storage equivalent.
    """
    if isinstance(e, InvalidNamespaceError):
        raise MainStorageValidationError("Namespace provided is invalid!")
    else:
        # If we can't categorize it, re-raise the original exception
        raise e
