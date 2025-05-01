def ExperimentalAPI(obj):
    """Decorator for documenting experimental APIs.

    Experimental APIs are classes and methods that are in development and may
    change at any time in their development process. You should not expect
    these APIs to be stable until their tag is changed to `DeveloperAPI` or
    `PublicAPI`.

    Subclasses that inherit from a ``@ExperimentalAPI`` base class can be
    assumed experimental as well.

    This decorator has no effect on runtime behavior
    """
    return obj


def DeveloperAPI(obj):
    """Decorator for documenting experimental APIs.

    Developer APIs are classes and methods explicitly exposed to developers
    for low level integrations with DeltaCAT (e.g.: compute engines, other catalogs).
    You can generally expect these APIs to be stable sans minor changes (but less stable than public APIs).

    This decorator has no effect on runtime behavior
    """
    return obj


def PublicAPI(obj):
    """Decorator for documenting public APIs.

    Public APIs are classes and methods exposed to end users which are expected to remain stable across releases.

    This decorator has no effect on runtime behavior
    """
    return obj
