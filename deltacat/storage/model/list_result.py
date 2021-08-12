import ray
from typing import Any, Callable, Dict, List, Optional


def of(
        items: List[Any],
        pagination_key: Optional[str],
        next_page_provider: Optional[Callable[..., Dict[str, Any]]]) \
        -> Dict[str, Any]:

    return {
        "items": items,
        "paginationKey": pagination_key,
        "nextPageProvider": next_page_provider
    }


def get_items(list_result: Dict[str, Any]) -> List[Any]:
    return list_result["items"]


def get_pagination_key(list_result: Dict[str, Any]) -> Optional[str]:
    return list_result.get("paginationKey")


def get_next_page_provider(list_result: Dict[str, Any]) \
        -> Optional[Callable[..., Dict[str, Any]]]:

    return list_result.get("nextPageProvider")


def next_page(list_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if list_result:
        pagination_key = get_pagination_key(list_result)
        if pagination_key:
            next_page_provider = get_next_page_provider(list_result)
            if next_page_provider is None:
                raise ValueError(f"Pagination key ('{pagination_key}') "
                                 f"specified without a page provider!")
            next_list_result = next_page_provider(pagination_key)
            if get_next_page_provider(next_list_result) is None:
                next_list_result["nextPageProvider"] = next_page_provider
            return next_list_result
    return None


def all_items(list_result: Dict[str, Any]) -> List[Any]:
    """
    Eagerly loads and returns a flattened list of all pages of items from the
    input list result producer.

    Args:
        list_result (Dict[str, Any]): List result to eagerly load all items
        from.
    Returns:
        items (List[Any]): Flattened list of items from all list result pages.
    """
    items = []
    while list_result:
        items.extend(get_items(list_result))
        list_result = next_page(list_result)
    return items


@ray.remote
def all_items_ray(list_result: Dict[str, Any]) -> List[Any]:
    """
    Eagerly loads and returns a flattened list of all pages of items from the
    input list result producer.

    Args:
        list_result (Dict[str, Any]): List result to eagerly load all items
        from.
    Returns:
        items (List[Any]): Flattened list of items from all list result pages.
    """
    return all_items(list_result)
