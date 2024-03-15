# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import List, Optional


class DeleteParameters(dict):
    """
    Represents parameters relevant to the underlying contents of manifest entry. Contains all parameters required to support DELETEs
    equality_column_names: List of column names that would be used to determine row equality for equality deletes.  Relevant only to equality deletes
    """

    @staticmethod
    def of(
        equality_column_names: Optional[List[str]] = None,
    ) -> DeleteParameters:
        delete_parameters = DeleteParameters()
        if equality_column_names is not None:
            delete_parameters["equality_column_names"] = equality_column_names
        return delete_parameters

    @property
    def equality_column_names(self) -> Optional[List[str]]:
        return self.get("equality_column_names")

    @staticmethod
    def merge_delete_parameters(
        delete_parameters: List[DeleteParameters],
    ) -> Optional[DeleteParameters]:
        def _merge_equality_column_names(equality_column_names: List[List[str]]):
            intersection = set([equality_column_names[0]])
            for column_name in equality_column_names[1:]:
                intersection &= set(column_name)
            return list(intersection)

        if len(delete_parameters) == 0:
            return None
        all_equality_column_names = [
            delete.equality_column_names for delete in delete_parameters
        ]
        merged_equality_column_names: List[str] = _merge_equality_column_names(
            all_equality_column_names
        )
        merge_delete_parameters = DeleteParameters.of(merged_equality_column_names)
        return merge_delete_parameters
