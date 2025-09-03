from __future__ import annotations

import logging
# import math
# import posixpath
import os
import tarfile
from typing import Optional, Iterable

# import pyarrow.fs
import pyarrow as pa
import pyarrow.json


from deltacat.constants import (
    DEFAULT_NAMESPACE,
)

# from deltacat.experimental.storage.rivulet.fs.file_store import FileStore
# from deltacat.experimental.storage.rivulet import Schema, Field
# from deltacat.utils.export import export_dataset
# from ..schema.schema import Datatype


from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class WebDatasetReader:
    def __init__(
        self,
        name: str,
        file_uri: str,
        merge_keys: str | Iterable[str] = None,
        # metadata_uri: Optional[str] = None,
        schema_mode: str = "union",
        batch_size: Optional[int] = 1,
        # filesystem: Optional[pyarrow.fs.FileSystem] = None,
        namespace: str = DEFAULT_NAMESPACE,
    ):
        merge_key = self._validate_single_merge_key(merge_keys)

        self.name = name
        self.file_uri = file_uri
        self.merge_key = merge_key
        # self.metadata_uri = metadata_uri
        self.schema_mode = schema_mode
        self.batch_size = batch_size
        # self.filesystem = filesystem
        self.namespace = namespace

        # self.tar =
        # self.
        pass

    # def _align_table_to_schema(
    #     self, pyarrow_table: pa.Table, schema: pa.Schema
    # ) -> pa.Table:
    #     """Fills empty values in given table to make it match the given schema"""
    #     # Start with existing columns (may need casts)
    #     arrays = []
    #     cols = {f.name: i for i, f in enumerate(pyarrow_table.schema)}
    #     for field in schema:
    #         if field.name in cols:
    #             arr = pyarrow_table.column(field.name)
    #             # If type differs, cast the column
    #             if not arr.type.equals(field.type):
    #                 arr = arr.cast(field.type, safe=False)
    #             arrays.append(arr)
    #         else:
    #             # Add a null-filled column of the right type
    #             arrays.append(pa.nulls(pyarrow_table.num_rows, type=field.type))

    #     return pa.Table.from_arrays(arrays, schema=schema)

    # def _concat_with_schema_union(self, t1: pa.Table, t2: pa.Table) -> pa.Table:
    #     """Takes two tables and concatenates them, unioning their schema to do so."""
    #     unified = pa.unify_schemas([t1.schema, t2.schema])
    #     t1a = self._align_table_to_schema(t1, unified)
    #     t2a = self._align_table_to_schema(t2, unified)
    #     # promote=True lets Arrow upcast (e.g., int32→int64) if needed
    #     return pa.concat_tables([t1a, t2a], promote=True, ignore_metadata=True)

    def _validate_single_merge_key(self, merge_keys):
        """Checks that there is only one merge key, and returns this merge key (Iterable or String)."""
        if not merge_keys or not isinstance(merge_keys, str):
            if len(merge_keys) == 1:
                return merge_keys[0]
            else:
                raise ValueError(
                    "Multiple merge keys are not supported in from_webdataset(). Please specify only 1 merge key as a string."
                )
        return merge_keys

    # def process_batch(self, batch, tar):
    #     """Takes a batch of JSONs, returns pyarrow table for JSON values and list of media binaries corresponding to JSON members."""
    #     media_binaries = []
    #     pyarrow_table = []
    #     for member in batch:
    #         if member.name.startswith("._"):
    #             continue
    #         if member.isfile() and member.name.endswith(".json"):
    #             f = tar.extractfile(member)
    #             if f:
    #                 try:
    #                     # merge_key = self.merge_keys

    #                     table = pyarrow.json.read_json(f)
    #                     media_filename = table[self.merge_key][0].as_py()
    #                     media_basename = os.path.basename(media_filename)
    #                     media_member = next(
    #                         (t for t in batch if t.name.endswith(media_basename)),
    #                         None,
    #                     )

    #                     if media_member:
    #                         fi = tar.extractfile(media_member)
    #                         if fi:
    #                             media_binary = fi.read()
    #                             media_binaries.extend([media_binary])

    #                     if pyarrow_table is None:
    #                         pyarrow_table = table
    #                     else:
    #                         # TODO: batch size 1 but still concating?
    #                         if pyarrow_table.schema == table.schema:
    #                             pyarrow_table = pa.concat_tables(
    #                                 [pyarrow_table, table],
    #                                 unify_schemas=True,
    #                             )
    #                         else:
    #                             pyarrow_table = self.concat_with_schema_union(
    #                                 pyarrow_table, table
    #                             )
    #                 except Exception as e:
    #                     print(f"Error with {member.name}:", e)
    #     return (pyarrow_table, media_binaries)

    # def from_webdataset(
    #     self,
    #     name: str,
    #     # file_uri: str,
    #     dataset_schema: Schema,
    #     merge_keys: str | Iterable[str] = None,
    #     metadata_uri: Optional[str] = None,
    #     schema_mode: str = "union",
    #     batch_size: Optional[int] = 1,
    #     filesystem: Optional[pyarrow.fs.FileSystem] = None,
    #     namespace: str = DEFAULT_NAMESPACE,
    # ) -> WebDatasetParser:
    #     """
    #     Create a Dataset from a single webdataset tar file.

    #     TODO: Add support for reading directories with multiple WDS files.

    #     Args:
    #         name: Unique identifier for the dataset.
    #         metadata_uri: Base URI for the dataset, where dataset metadata is stored. If not specified, will be placed in ${file_uri}/riv-meta
    #         file_uri: Path to a single webdataset file.
    #         merge_keys: Fields to specify as merge keys for future 'zipper merge' operations on the dataset.
    #         schema_mode: Currently ignored as this is for a single file.

    #     Returns:
    #         Dataset: New dataset instance with the schema automatically inferred
    #                  from the tar file.
    #     """
    #     # Read the WebDataset into a PyArrow Table
    #     with tarfile.open(self.file_uri, "r") as tar:
    #         media_binaries = []
    #         tar_members = tar.getmembers()
    #         final_table = None  # contains the results of processing all batches
    #         reading_frame_size = batch_size
    #         total_batches = math.ceil(len(tar_members) / reading_frame_size)
    #         # Iterate through and process each batch
    #         for i in range(total_batches):
    #             reading_frame_start = i * reading_frame_size
    #             reading_frame_end = reading_frame_start + reading_frame_size
    #             batch = tar_members[reading_frame_start:reading_frame_end]
    #             batch_table, batch_media_binaries = self.process_batch(batch, tar)

    #             media_binaries.extend(batch_media_binaries)

    #             if batch_table is not None:
    #                 try:
    #                     dataset_schema.merge(
    #                         Schema.from_pyarrow(
    #                             batch_table.schema, merge_keys=self.merge_key
    #                         )
    #                     )
    #                 except Exception as e:
    #                     print(f"Error merging schema: {e}")
    #             # here

    #         if final_table is not None and media_binaries:
    #             if len(media_binaries) == final_table.num_rows:
    #                 try:
    #                     binary_column = pyarrow.array(
    #                         media_binaries, type=pyarrow.binary()
    #                     )
    #                     final_table = final_table.add_column(
    #                         len(final_table.schema), "media_binary", binary_column
    #                     )
    #                     # Edit dataset_schema to have media_binaries as a field object
    #                     dataset_schema.add_field(
    #                         Field("media_binary", Datatype.from_pyarrow(pa.binary()))
    #                     )
    #                 except Exception as e:
    #                     print(f"Mismatch between media binaries and batch rows: {e}")

    #     if final_table is None:
    #         raise ValueError("No valid JSON files found in the webdataset tar file.")

    def _align_to_schema(self, tbl: pa.Table, schema: pa.Schema) -> pa.Table:
        n = tbl.num_rows
        # add missing fields
        for f in schema:
            if f.name not in tbl.column_names:
                tbl = tbl.append_column(f.name, pa.nulls(n, type=f.type))
        # now cast to reorder & upcast types
        return tbl.cast(schema, safe=False)

    def to_pyarrow(self):
        """Returns a pyarrow table of the tar members, storing file content for each member in the webdataset in a new media_binary column."""
        tables = []
        media_binaries = []
        with tarfile.open(self.file_uri, "r") as tar:
            tar_members = tar.getmembers()
            tar_members = [
                member
                for member in tar_members
                if member.isfile() and not member.name.startswith("._")
            ]
            # Get individual pyarrow tables (1 per json member) and media_binaries
            for i in range(0, len(tar_members), 2):
                # Get the json member and corresponding media member by index
                # With webdatasets: guaranteed that the json and its corresponding media file are next to each other
                # However the order of media file first or json first is not specified
                member, media_member = tar_members[i], tar_members[i + 1]
                if not member.name.endswith(".json"):
                    member, media_member = media_member, member
                try:
                    f = tar.extractfile(member)
                    if not f:
                        continue
                    tbl = pyarrow.json.read_json(f)
                    # Validate that the member actually corresponds with the media_member
                    media_filename = os.path.basename(tbl[self.merge_key][0].as_py())
                    key, _ = member.name.split(".", 1)
                    expected_media_name = f"{key}.{media_filename}"
                    if expected_media_name != media_member.name:
                        logger.warning(
                            "Mismatched filename for sample %s: expected media %s but found %s",
                            key,
                            expected_media_name,
                            media_member.name,
                        )
                        continue
                    f_media = tar.extractfile(media_member)
                    if not f_media:
                        continue
                    media_binary = f_media.read()
                    # Add media binary and table to respective lists
                    media_binaries.extend([media_binary])
                    tables.append(tbl)
                except Exception as e:
                    logger.warning("Error processing member %s: %s", member.name, e)
            if len(tables) != len(media_binaries):
                logger.error(
                    "Mismatch between number of JSON tables (%d) and media binaries (%d)",
                    len(tables),
                    len(media_binaries),
                )
                return

            unified = pa.unify_schemas([t.schema for t in tables])
            tables = [self._align_to_schema(t, unified) for t in tables]
            final_table = pa.concat_tables(tables, unify_schemas=False, promote=False)
            media_binary_array = pa.array(media_binaries, type=pa.binary())
            final_table = final_table.append_column("media_binary", media_binary_array)
        return final_table
