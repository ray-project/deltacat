import inspect
from typing import Any, Dict


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
