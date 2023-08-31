from __future__ import annotations

from typing import Dict, Any, Optional, List
from pyarrow.parquet import FileMetaData


class PartialFileDownloadParams(Dict[str, Any]):
    """
    A content type params class used to represent arguments required
    to down the file partially. This is useful specifically in cases
    where you'd like to instruct downloader to partially download a
    manifest entry.
    """

    pass


class PartialParquetParameters(PartialFileDownloadParams):
    @staticmethod
    def of(
        row_groups_to_download: Optional[List[int]] = None,
        num_row_groups: Optional[int] = None,
        num_rows: Optional[int] = None,
        in_memory_size_bytes: Optional[float] = None,
        pq_metadata: Optional[FileMetaData] = None,
    ) -> PartialParquetParameters:

        if (
            row_groups_to_download is None
            or num_row_groups is None
            or num_rows is None
            or in_memory_size_bytes is None
        ):
            assert (
                pq_metadata is not None
            ), "Parquet file metadata must be passed explicitly"

            num_row_groups = pq_metadata.num_row_groups
            row_groups_to_download = [rg for rg in range(num_row_groups)]
            in_memory_size_bytes = 0.0
            num_rows = pq_metadata.num_rows

            for rg in row_groups_to_download:
                row_group_meta = pq_metadata.row_group(rg)
                in_memory_size_bytes += row_group_meta.total_byte_size

        result = PartialParquetParameters(
            {
                "row_groups_to_download": row_groups_to_download,
                "num_row_groups": num_row_groups,
                "num_rows": num_rows,
                "in_memory_size_bytes": in_memory_size_bytes,
            }
        )

        if pq_metadata:
            result["pq_metadata"] = pq_metadata

        return result

    @property
    def row_groups_to_download(self) -> List[int]:
        return self["row_groups_to_download"]

    @property
    def num_row_groups(self) -> List[int]:
        return self["num_row_groups"]

    @property
    def num_rows(self) -> int:
        return self["num_rows"]

    @property
    def in_memory_size_bytes(self) -> float:
        return self["in_memory_size_bytes"]

    @property
    def pq_metadata(self) -> Optional[FileMetaData]:
        return self.get("pq_metadata")

    @pq_metadata.setter
    def pq_metadata(self, metadata: FileMetaData) -> None:
        self["pq_metadata"] = metadata
