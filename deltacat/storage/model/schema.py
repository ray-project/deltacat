# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Any

import pyarrow as pa


class Schema(dict):
    @staticmethod
    def of(
        schema: Optional[pa.Schema],
        native_object: Optional[Any] = None,
    ) -> Schema:
        return Schema(
            {
                "schema": schema,
                "nativeObject": native_object,
            }
        )

    @property
    def schema(self) -> Optional[pa.Schema]:
        return self.get("schema")

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")
