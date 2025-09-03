from __future__ import annotations

import logging

import os
import tarfile
from typing import Optional, Iterable

import pyarrow as pa
import pyarrow.json
from deltacat.constants import (
    DEFAULT_NAMESPACE,
)

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class WebDatasetReader:
    def __init__(
        self,
        name: str,
        file_uri: str,
        merge_keys: str | Iterable[str] = None,
        schema_mode: str = "union",
        batch_size: Optional[int] = 1,
        namespace: str = DEFAULT_NAMESPACE,
    ):
        merge_key = self._validate_single_merge_key(merge_keys)

        self.name = name
        self.file_uri = file_uri
        self.merge_key = merge_key
        self.schema_mode = schema_mode
        self.batch_size = batch_size
        self.namespace = namespace

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

    def _align_to_schema(self, tbl: pa.Table, schema: pa.Schema) -> pa.Table:
        # add missing fields
        for f in schema:
            if f.name not in tbl.column_names:
                tbl = tbl.append_column(f.name, pa.nulls(1, type=f.type))
        # reorder to match schema field order + rebuild table in the correct order
        tbl = pa.table([tbl[f.name] for f in schema], schema=schema)
        return tbl

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

            unified = pa.unify_schemas([t.schema for t in tables], promote_options="permissive")
            tables = [self._align_to_schema(t, unified) for t in tables]
            final_table = pa.concat_tables(tables, unify_schemas=False, promote=False)
            media_binary_array = pa.array(media_binaries, type=pa.binary())
            final_table = final_table.append_column("media_binary", media_binary_array)
        return final_table
