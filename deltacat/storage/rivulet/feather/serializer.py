from typing import List

import pyarrow as pa
from pyarrow import feather

from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.arrow.serializer import ArrowSerializer
from deltacat.storage.rivulet.fs.file_provider import FileProvider


class FeatherDataSerializer(ArrowSerializer):
    """
    Feather data writer. Responsible for flushing rows to feather files and returning SSTable rows for any file(s) written

    TODO Support recording byte range offsets. Deferring this for now
        We may need to provide a wrapper class over fsspec which introspects how many bytes written
        when .write is called on output stream
    """

    def __init__(self, file_provider: FileProvider, schema: Schema):
        super().__init__(file_provider, schema)

    def serialize(self, table: pa.Table) -> List[SSTableRow]:
        file = self.file_provider.provide_data_file("feather")

        with file.create() as outfile:
            # Note that write_feather says that dest is a string, but it is really any object implementing write()
            feather.write_feather(table, dest=outfile)

        # Because we only write one row group, it only creates one SSTableRow
        #  we may have more granular SST indexes for other formats
        key_min, key_max = self._get_min_max_key(table)
        # TODO need to populate byte offsets. For now, we are writing single files per SSTableRow
        return [SSTableRow(key_min, key_max, file.location, 0, 0)]
