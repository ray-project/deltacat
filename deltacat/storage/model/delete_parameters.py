# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import List, Optional


class DeleteParameters(dict):
    """
    Contains all parameters required to support DELETEs
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
        if len(delete_parameters) < 2:
            return delete_parameters
        equality_column_names = delete_parameters[0].equality_column_names
        assert all(
            delete_prev.equality_column_names == delete_curr.equality_column_names
            for delete_prev, delete_curr in zip(
                delete_parameters, delete_parameters[1:]
            )
        ), "We cannot merge two delete parameters if their equality column names are different."
        merge_delete_parameters = DeleteParameters.of(equality_column_names)
        return merge_delete_parameters
