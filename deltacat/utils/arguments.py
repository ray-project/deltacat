import inspect
import functools
from typing import Any, Callable, Dict, List


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


def alias(aliases: Dict[str, str]) -> Callable:
    """
    This decorator allows for aliases to be used for function arguments.
    :param aliases: A dictionary of aliases to use for the function arguments.
    :return: A decorator that can be used to decorate a function.

    For example:
    >>> @alias({'long_parameter_name': 'param'})
    >>> def example_fn(long_parameter_name='foo', **kwargs):
    ...     print(long_parameter_name)
    >>> example_fn(long_parameter_name="bar")
    >>> bar
    >>> example_fn(param="baz")
    >>> baz
    >>> example_fn()
    >>> foo
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(**kwargs: Any) -> Any:
            for name, alias in aliases.items():
                if name not in kwargs and alias in kwargs:
                    kwargs[name] = kwargs[alias]
            return func(**kwargs)

        return wrapper

    return decorator
