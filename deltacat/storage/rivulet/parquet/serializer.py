from typing import Iterator, List, Any

import pyarrow as pa
from pyarrow.parquet import FileMetaData

from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet.schema import Schema
from deltacat.storage.rivulet.arrow.serializer import ArrowSerializer

from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider


class ParquetDataSerializer(ArrowSerializer):
    """
    Parquet data writer. Responsible for flushing rows to parquet and returning SSTable rows for any file(s) written
    """

    def __init__(self, location_provider: FileLocationProvider, schema: Schema):
        super().__init__(location_provider, schema)

    def serialize(self, table: pa.Table) -> List[SSTableRow]:
        file = self.location_provider.new_data_file_uri("parquet")
        with file.create() as outfile:
            metadata_collector: list[Any] = []
            pa.parquet.write_table(table=table,
                                   where=outfile,
                                   metadata_collector=metadata_collector
                                   )
            # look for file metadata
            file_metadata: FileMetaData = next(item for item in metadata_collector if isinstance(item, FileMetaData))
            row_group_count = file_metadata.num_row_groups

        # Because ParquetWriter only writes one row group, it only creates one SSTableRow
        #  we may have more granular SST indexes for other formats
        key_min, key_max = self._get_min_max_key(table)
        return [SSTableRow(key_min, key_max, file.location, 0, 0 + row_group_count)]
