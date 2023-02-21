# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Callable, Generic, List, Optional, TypeVar

import ray

T = TypeVar("T")


class ListResult(dict, Generic[T]):
    @staticmethod
    def of(
        items: Optional[List[T]],
        pagination_key: Optional[str],
        next_page_provider: Optional[Callable[..., ListResult[T]]],
    ) -> ListResult:
        list_result = ListResult()
        list_result["items"] = items
        list_result["paginationKey"] = pagination_key
        list_result["nextPageProvider"] = next_page_provider
        return list_result

    def read_page(self) -> Optional[List[T]]:
        return self.get("items")

    @property
    def pagination_key(self) -> Optional[str]:
        return self.get("paginationKey")

    @property
    def next_page_provider(self) -> Optional[Callable[..., ListResult[T]]]:
        return self.get("nextPageProvider")

    def next_page(self) -> Optional[ListResult[T]]:
        pagination_key = self.pagination_key
        if pagination_key:
            next_page_provider = self.next_page_provider
            if next_page_provider is None:
                raise ValueError(
                    f"Pagination key ('{pagination_key}') "
                    f"specified without a next page provider!"
                )
            next_list_result = next_page_provider(pagination_key)
            if next_list_result.next_page_provider is None:
                next_list_result["nextPageProvider"] = next_page_provider
            return next_list_result
        return None

    def all_items(self) -> List[T]:
        """
        Eagerly loads and returns a flattened list of all pages of items from
        this list result.

        Returns:
            items (List[Any]): Flattened list of items from all list result
                pages.
        """
        items = []
        list_result = self
        while list_result:
            items.extend(list_result.read_page())
            list_result = list_result.next_page()
        return items

    @staticmethod
    @ray.remote
    def all_items_ray(list_result: ListResult) -> List[T]:
        """
        Eagerly loads and returns a flattened list of all pages of items from
        this list result.

        Returns:
            items (List[Any]): Flattened list of items from all list result
                pages.
        """
        return list_result.all_items()
