from __future__ import annotations

import typing
from typing import Optional

from deltacat.storage.model.shard import Shard

T = typing.TypeVar("T")  # Type of primary key in query expression. Must be comparable


class QueryExpression(typing.Generic[T]):
    """
    Top level class for creating representing queries on a riv dataset.

    For now, this is a minimal implementation which just allows for different predicates.

    FUTURE IMPROVEMENTS
    1. Support builder using operator overloading or fluent builder pattern,e.g.
       (operator overloading) query = Column("Foo") < 10 & Column("PK")==100
       (fluent interface) query = builder.column("colA").less_than(10)
          .and_()
          .column("PK").equals(100)
          .build()

    2. Support better push down predicate integration end to end. Specifically,
        scan operation will need to return which query predicates were honored
    """

    def __init__(self):
        self.key_range: Optional[(T, T)] = None

    def with_key(self, val: T) -> QueryExpression:
        """
        Syntactic sugar for setting key range to a single value
        """
        if self.key_range:
            raise ValueError(
                f"Query expression already has set key range to: {self.key_range}"
            )
        self.key_range = (val, val)
        return self

    def with_range(self, bound1: T, bound2: T) -> QueryExpression:
        if self.key_range:
            raise ValueError(f"Key range already set to {self.key_range}")
        self.key_range = tuple(sorted([bound1, bound2]))
        return self

    @staticmethod
    def with_shard(query: Optional[QueryExpression], shard: Shard):
        """
        Generate a query expression that accounts for the shard boundaries.
        Shard boundaries are inclusive and mark the outer bounds of the query.
        """
        if shard is None:
            return query

        if query.key_range is None:
            return QueryExpression().with_range(shard.min_key, shard.max_key)

        min_key = shard.min_key
        max_key = shard.max_key

        if min_key > query.min_key:
            min_key = query.min_key

        if max_key < query.max_key:
            max_key = query.max_key

        return QueryExpression().with_range(min_key, max_key)

    @property
    def min_key(self) -> T | None:
        if not self.key_range:
            return None
        return self.key_range[0]

    @property
    def max_key(self) -> T | None:
        if not self.key_range:
            return None
        return self.key_range[1]

    def matches_query(self, key: any) -> bool:
        """
        Returns true if the key is within the range of the query expression
        """
        if not self.key_range:
            return True
        return self.min_key <= key <= self.max_key

    def below_query_range(self, key: any) -> bool:
        """
        Returns true if the key is below the range of the query expression
        will return false if key range is not set
        """
        if not self.key_range:
            return False
        return self.min_key > key
