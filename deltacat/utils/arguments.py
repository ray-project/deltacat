import inspect
from typing import Any, Dict, List


def sanitize_kwargs_to_callable(callable: Any, kwargs: Dict) -> Dict:
    """
    This method removes any upsupported key word arguments if variable
    kwargs are not allowed in the method signature.

    Returns: a sanitized dict of kwargs.
    """
    signature = inspect.signature(callable)
    params = signature.parameters

    new_kwargs = {**kwargs}

    for key in params:
        if params[key].kind == inspect.Parameter.VAR_KEYWORD:
            return kwargs

    for key in kwargs.keys():
        if key not in params:
            new_kwargs.pop(key)

    return new_kwargs


def sanitize_kwargs_by_supported_kwargs(
    supported_kwargs: List[str], kwargs: Dict
) -> Dict:
    """
    This method only keeps the kwargs in the list provided above and ignores any other kwargs passed.
    This method will specifically be useful where signature cannot be automatically determined
    (say the definition is part C++ implementation).

    Returns: a sanitized dict of kwargs.
    """

    new_kwargs = {}
    for key in supported_kwargs:
        if key in kwargs:
            new_kwargs[key] = kwargs[key]

    return new_kwargs
