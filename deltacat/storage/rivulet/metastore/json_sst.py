import logging
import json

from itertools import zip_longest
from typing import List

from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile
from deltacat.storage.rivulet.metastore.sst import (
    SSTWriter,
    SSTableRow,
    SSTReader,
    SSTable,
)
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class JsonSstWriter(SSTWriter):
    """
    Class for reading and writing Json SST files

    TODO use a more efficient format or compression. Also can factor out URI prefix across rows
    We can also optimize by omitting offset_end if sequential rows use the same uri
    """

    def write(self, file: OutputFile, rows: List[SSTableRow]) -> None:
        """
        Writes SST file
        """
        if len(rows) == 0:
            return

        # File-level metadata for key min/max and offset end
        min_key = rows[0].key_min
        max_key = rows[-1].key_max
        offset_end = rows[-1].offset_end

        # Convert to dict for json serialization
        file_rows = [
            {
                "key_min": row.key_min,
                "key_max": row.key_max,
                "offset": row.offset_start,
                "uri": row.uri,
            }
            for row in rows
        ]

        file_as_dict = {
            "key_min": min_key,
            "key_max": max_key,
            "offset_end": offset_end,
            "metadata": file_rows,
        }

        try:
            with file.create() as f:
                f.write(json.dumps(file_as_dict).encode())
            logger.debug(f"SSTable data successfully written to {file.location}")
        except Exception as e:
            # TODO better error handling for IO
            logger.debug(f"Unexpected error occurred while writing SSTable data: {e}")
            raise e


class JsonSstReader(SSTReader):
    """
    interface for reading SST files
    """

    def read(self, file: InputFile) -> SSTable:
        with file.open() as f:
            data = json.loads(f.read())
        sst_rows: List[SSTableRow] = []
        file_offset_end = data["offset_end"]

        # each row only has "key", "offset", "uri"
        # need to get "key_end", "offset_end" from either next row
        # OR from top level metadata
        for row1, row2 in zip_longest(data["metadata"], data["metadata"][1:]):
            # if not row2, we are on the very last row and need to use file metadata for key and offset end
            if not row2:
                sst_rows.append(
                    SSTableRow(
                        row1["key_min"],
                        row1["key_max"],
                        row1["uri"],
                        row1["offset"],
                        file_offset_end,
                    )
                )
            else:
                sst_rows.append(
                    SSTableRow(
                        row1["key_min"],
                        row1["key_max"],
                        row1["uri"],
                        row1["offset"],
                        file_offset_end,
                    )
                )

        return SSTable(sst_rows, data["key_min"], data["key_max"])
