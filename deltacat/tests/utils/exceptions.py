"""
Exception classes for main storage testing that mirror the local storage exceptions.
These are used to test the main metastore error categorization functionality.
"""


class InvalidNamespaceError(Exception):
    """Exception raised when an invalid namespace is provided to main storage."""

    error_name = "InvalidNamespaceError"


class MainStorageValidationError(Exception):
    """Exception raised when main storage validation fails."""

    error_name = "MainStorageValidationError"


class MainStorageError(Exception):
    """General exception for main storage operations."""

    error_name = "MainStorageError"
