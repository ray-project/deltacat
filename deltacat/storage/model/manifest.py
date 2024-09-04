from __future__ import annotations

import logging
import itertools

from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import uuid4

from deltacat import logs
from deltacat.storage import PartitionValues

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class EntryType(str, Enum):
    """
    Enum representing all possible content categories of a manifest entry file.

    DATA: The entry contains fully qualified records compliant with the parent
    table's schema to insert and/or update. Data files for upsert Deltas use
    this entry's parameters to find matching fields to update. If no entry
    parameters are specified, then the parent table's primary keys are used.
    Only records from entries in Deltas with lower stream positions than this
    entry will be targeted for update.

    POSITIONAL_DELETE: The entry contains pointers to records in other entries
    to delete. Deletes will be applied logically when serving merge-on-read
    query results, or applied physically to entry files during copy-on-write
    table compaction.

    EQUALITY_DELETE: The entry contains a subset of field values from prior
    table records to find and delete. The full record of any matching data
    entries in Deltas with a lower stream position than this entry's Delta
    will be deleted. The fields used for record discovery are controlled by
    this entry's parameters. If no entry parameters are specified, then the
    fields used for record discovery are linked to the parent table's primary
    keys. The entry may contain additional fields not used for delete record
    discovery which will be ignored. Deletes will be applied logically when
    serving merge-on-read query results, or applied physically to entry files
    during copy-on-write table compaction.
    """
    DATA = "data"
    POSITIONAL_DELETE = "positional_delete"
    EQUALITY_DELETE = "equality_delete"

    @classmethod
    def get_default(cls):
        return EntryType.DATA

    @classmethod
    def list(cls):
        return [c.value for c in EntryType]


class EntryParams(dict):
    """
    Parameters that control interpretation manifest entry interpretation.

    For EQUALITY_DELETE manifest entry types, parameters include equality
    field identifiers.
    """

    @staticmethod
    def of(
            equality_column_names: Optional[List[str]] = None,
    ) -> EntryParams:
        params = EntryParams()
        if equality_column_names is not None:
            params["equality_column_names"] = equality_column_names
        return params

    @property
    def equality_column_names(self) -> Optional[List[str]]:
        return self.get("equality_column_names")

    @staticmethod
    def merge(
            parameters: List[EntryParams],
    ) -> Optional[EntryParams]:
        if len(parameters) < 2:
            return parameters
        equality_column_names = parameters[0].equality_column_names
        assert all(
            prev.equality_column_names == curr.equality_column_names
            for prev, curr in zip(
                parameters, parameters[1:]
            )
        ), "Cannot merge entry parameters with different equality fields."
        merged_params = EntryParams.of(equality_column_names)
        return merged_params


class Manifest(dict):
    """
    A DeltaCAT manifest contains metadata common to multiple manifest formats
    like Amazon Redshift, Apache Iceberg, etc.
    """
    @staticmethod
    def _build_manifest(
        meta: Optional[ManifestMeta],
        entries: Optional[ManifestEntryList],
        author: Optional[ManifestAuthor] = None,
        uuid: str = None,
    ) -> Manifest:
        if not uuid:
            uuid = str(uuid4())
        manifest = Manifest()
        manifest["id"] = uuid
        if meta is not None:
            manifest["meta"] = meta
        if entries is not None:
            manifest["entries"] = entries
        if author is not None:
            manifest["author"] = author
        return manifest

    @staticmethod
    def of(
        entries: ManifestEntryList,
        author: Optional[ManifestAuthor] = None,
        uuid: str = None,
        entry_type: Optional[EntryType] = None,
        entry_params: Optional[EntryParams] = None,
        partition_values: Optional[PartitionValues] = None,
    ) -> Manifest:
        if not uuid:
            uuid = str(uuid4())
        total_record_count = 0
        total_content_length = 0
        total_source_content_length = 0
        content_type = None
        content_encoding = None
        partition_values_set = set()
        if entries:
            content_type = entries[0].meta.content_type
            content_encoding = entries[0].meta.content_encoding
            for entry in entries:
                meta = entry.meta
                if meta.content_type != content_type:
                    content_type = None
                if meta.content_encoding != content_encoding:
                    content_encoding = None
                entry_content_type = meta.content_type
                if entry_content_type != content_type:
                    msg = (
                        f"Expected all manifest entries to have content "
                        f"type '{content_type}' but found "
                        f"'{entry_content_type}'"
                    )
                    raise ValueError(msg)
                entry_content_encoding = meta["content_encoding"]
                if entry_content_encoding != content_encoding:
                    msg = (
                        f"Expected all manifest entries to have content "
                        f"encoding '{content_encoding}' but found "
                        f"'{entry_content_encoding}'"
                    )
                    raise ValueError(msg)
                actual_entry_type = meta.entry_type
                if entry_type and (actual_entry_type != entry_type):
                    msg = (
                        f"Expected all manifest entries to have type "
                        f"'{entry_type}' but found '{actual_entry_type}'"
                    )
                    raise ValueError(msg)
                actual_entry_params = meta.entry_params
                if entry_params and (actual_entry_params != entry_params):
                    msg = (
                        f"Expected all manifest entries to have params "
                        f"'{entry_params}' but found '{actual_entry_params}'"
                    )
                    raise ValueError(msg)
                entry_partition_values = meta.partition_values
                if (partition_values and
                        (entry_partition_values != partition_values)):
                    msg = (
                        f"Expected all manifest entries to have partition "
                        f"values '{partition_values}' but found "
                        f"'{entry_partition_values}'"
                    )
                    raise ValueError(msg)

                total_record_count += meta.record_count or 0
                total_content_length += meta.content_length or 0
                total_source_content_length += meta.source_content_length or 0
                if len(partition_values_set) <= 1:
                    partition_values_set.add(entry.meta.partition_values)

        if len(partition_values_set) == 1:
            partition_values = partition_values_set.pop()

        meta = ManifestMeta.of(
            total_record_count,
            total_content_length,
            content_type,
            content_encoding,
            total_source_content_length,
            entry_type,
            entry_params,
            partition_values,
        )
        manifest = Manifest._build_manifest(meta, entries, author, uuid)
        return manifest

    @staticmethod
    def merge_manifests(
        manifests: List[Manifest], author: Optional[ManifestAuthor] = None
    ) -> Manifest:
        all_entries = ManifestEntryList(
            itertools.chain(*[m.entries for m in manifests])
        )
        merged_manifest = Manifest.of(all_entries, author)
        return merged_manifest

    @property
    def meta(self) -> Optional[ManifestMeta]:
        val: Dict[str, Any] = self.get("meta")
        if val is not None and not isinstance(val, ManifestMeta):
            self["meta"] = val = ManifestMeta(val)
        return val

    @property
    def entries(self) -> Optional[ManifestEntryList]:
        val: List[ManifestEntry] = self.get("entries")
        if val is not None and not isinstance(val, ManifestEntryList):
            self["entries"] = val = ManifestEntryList.of(val)
        return val

    @property
    def id(self) -> str:
        return self["id"]

    @property
    def author(self) -> Optional[ManifestAuthor]:
        val: Dict[str, Any] = self.get("author")
        if val is not None and not isinstance(val, ManifestAuthor):
            self["author"] = val = ManifestAuthor(val)
        return val


class ManifestMeta(dict):
    @staticmethod
    def of(
            record_count: Optional[int],
            content_length: Optional[int],
            content_type: Optional[str],
            content_encoding: Optional[str],
            source_content_length: Optional[int] = None,
            credentials: Optional[Dict[str, str]] = None,
            content_type_parameters: Optional[List[Dict[str, str]]] = None,
            entry_type: Optional[EntryType] = None,
            entry_params: Optional[EntryParams] = None,
            partition_values: Optional[PartitionValues] = None,
    ) -> ManifestMeta:
        manifest_meta = ManifestMeta()
        if record_count is not None:
            manifest_meta["record_count"] = record_count
        if content_length is not None:
            manifest_meta["content_length"] = content_length
        if source_content_length is not None:
            manifest_meta["source_content_length"] = source_content_length
        if content_type is not None:
            manifest_meta["content_type"] = content_type
        if content_type_parameters is not None:
            manifest_meta["content_type_parameters"] = content_type_parameters
        if content_encoding is not None:
            manifest_meta["content_encoding"] = content_encoding
        if credentials is not None:
            manifest_meta["credentials"] = credentials
        if entry_type is not None:
            manifest_meta["entry_type"] = entry_type.value
        if entry_params is not None:
            manifest_meta["entry_params"] = entry_params
        if partition_values is not None:
            manifest_meta["partition_values"] = partition_values
        return manifest_meta

    @property
    def record_count(self) -> Optional[int]:
        return self.get("record_count")

    @property
    def content_length(self) -> Optional[int]:
        return self.get("content_length")

    @property
    def content_type(self) -> Optional[str]:
        return self.get("content_type")

    @property
    def content_encoding(self) -> Optional[str]:
        return self.get("content_encoding")

    @property
    def source_content_length(self) -> Optional[int]:
        return self.get("source_content_length")

    @property
    def content_type_parameters(self) -> Optional[List[Dict[str, str]]]:
        return self.get("content_type_parameters")

    @content_type_parameters.setter
    def content_type_parameters(self, params: List[Dict[str, str]]) -> None:
        self["content_type_parameters"] = params

    @property
    def credentials(self) -> Optional[Dict[str, str]]:
        return self.get("credentials")

    @property
    def entry_type(self) -> Optional[EntryType]:
        val = self.get("entry_type")
        if val is not None:
            return EntryType(self["entry_type"])
        return val

    @property
    def entry_params(self) -> Optional[EntryType]:
        return self.get("entry_params")

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        return self.get("partition_values")


class ManifestEntry(dict):
    @staticmethod
    def of(
        url: Optional[str],
        meta: Optional[ManifestMeta],
        mandatory: bool = True,
        uri: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> ManifestEntry:
        manifest_entry = ManifestEntry()
        if not (uri or url):
            raise ValueError("No URI or URL specified for manifest entry contents.")
        if (uri and url) and (uri != url):
            raise ValueError(f"Manifest entry URI ({uri}) != URL ({url})")
        if url:
            manifest_entry["url"] = manifest_entry["uri"] = url
        elif uri:
            manifest_entry["url"] = manifest_entry["uri"] = uri
        if meta is not None:
            manifest_entry["meta"] = meta
        if mandatory is not None:
            manifest_entry["mandatory"] = mandatory
        if uuid is not None:
            manifest_entry["id"] = uuid
        return manifest_entry

    @staticmethod
    def from_s3_obj_url(
        url: str,
        record_count: int,
        source_content_length: Optional[int] = None,
        **s3_client_kwargs,
    ) -> ManifestEntry:
        from deltacat.aws import s3u as s3_utils

        s3_obj = s3_utils.get_object_at_url(url, **s3_client_kwargs)
        logger.debug(f"Building manifest entry from {url}: {s3_obj}")
        manifest_entry_meta = ManifestMeta.of(
            record_count,
            s3_obj["ContentLength"],
            s3_obj["ContentType"],
            s3_obj["ContentEncoding"],
            source_content_length,
        )
        manifest_entry = ManifestEntry.of(url, manifest_entry_meta)
        return manifest_entry

    @property
    def uri(self) -> Optional[str]:
        return self.get("uri")

    @property
    def url(self) -> Optional[str]:
        return self.get("url")

    @property
    def meta(self) -> Optional[ManifestMeta]:
        val: Dict[str, Any] = self.get("meta")
        if val is not None and not isinstance(val, ManifestMeta):
            self["meta"] = val = ManifestMeta(val)
        return val

    @property
    def mandatory(self) -> bool:
        return self["mandatory"]

    @property
    def id(self) -> Optional[str]:
        return self.get("id")


class ManifestAuthor(dict):
    @staticmethod
    def of(name: Optional[str], version: Optional[str]) -> ManifestAuthor:
        manifest_author = ManifestAuthor()
        if name is not None:
            manifest_author["name"] = name
        if version is not None:
            manifest_author["version"] = version
        return manifest_author

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def version(self) -> Optional[str]:
        return self.get("version")


class ManifestEntryList(List[ManifestEntry]):
    @staticmethod
    def of(entries: List[ManifestEntry]) -> ManifestEntryList:
        manifest_entries = ManifestEntryList()
        for entry in entries:
            if entry is not None and not isinstance(entry, ManifestEntry):
                entry = ManifestEntry(entry)
            manifest_entries.append(entry)
        return manifest_entries

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, ManifestEntry):
            self[item] = val = ManifestEntry(val)
        return val
