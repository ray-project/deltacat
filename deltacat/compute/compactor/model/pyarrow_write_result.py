# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import List


class PyArrowWriteResult(dict):
    @staticmethod
    def of(file_count: int,
           pyarrow_bytes: int,
           file_bytes: int,
           record_count: int) -> PyArrowWriteResult:
        return PyArrowWriteResult({
            "files": file_count,
            "paBytes": pyarrow_bytes,
            "fileBytes": file_bytes,
            "records": record_count,
        })

    @staticmethod
    def union(results: List[PyArrowWriteResult]) -> PyArrowWriteResult:
        """
        Create a new PyArrowWriteResult containing all results from all input
        PyArrowWriteResults.
        """
        return PyArrowWriteResult.of(
            sum([result.files for result in results]),
            sum([result.pyarrow_bytes for result in results]),
            sum([result.file_bytes for result in results]),
            sum([result.records for result in results]),
        )

    @property
    def files(self) -> int:
        return self["files"]

    @property
    def pyarrow_bytes(self) -> int:
        return self["paBytes"]

    @property
    def file_bytes(self) -> int:
        return self["fileBytes"]

    @property
    def records(self) -> int:
        return self["records"]
