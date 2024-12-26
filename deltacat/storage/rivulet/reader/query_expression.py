from __future__ import annotations

import typing
from typing import Optional

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
        self.primary_key_range: Optional[(T, T)] = None

    def with_primary_key(self, val: T) -> "QueryExpression":
        """
        Syntactic sugar for setting primary key range to a single value
        """
        if self.primary_key_range:
            raise ValueError(
                f"Query expression already has set primary key range to: {self.primary_key_range}"
            )
        self.primary_key_range = (val, val)
        return self

    def with_primary_range(self, bound1: T, bound2: T) -> "QueryExpression":
        if self.primary_key_range:
            raise ValueError(
                f"Primary key range already set to {self.primary_key_range}"
            )
        self.primary_key_range = tuple(sorted([bound1, bound2]))
        return self

    @property
    def min_key(self) -> T | None:
        if not self.primary_key_range:
            return None
        return self.primary_key_range[0]

    @property
    def max_key(self) -> T | None:
        if not self.primary_key_range:
            return None
        return self.primary_key_range[1]

    def matches_query(self, primary_key: any) -> bool:
        """
        Returns true if the primary key is within the range of the query expression
        """
        if not self.primary_key_range:
            return True
        return self.min_key <= primary_key <= self.max_key

    def below_query_range(self, primary_key: any) -> bool:
        """
        Returns true if the primary key is below the range of the query expression
        will return false if primary key range is not set
        """
        if not self.primary_key_range:
            return False
        return self.min_key > primary_key
