from __future__ import annotations

import logging
import itertools

from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import uuid4

from deltacat import logs

from deltacat.storage.model.schema import FieldLocator

import json

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
    to delete. Deleted records will be filtered from query results at runtime.

    EQUALITY_DELETE: The entry contains a subset of field values from the
    table records to find and delete. The full record of any matching data
    entries in Deltas with a lower stream position than this entry's Delta
    will be deleted. The fields used for record discovery are controlled by
    this entry's parameters. If no entry parameters are specified, then the
    fields used for record discovery are linked to the parent table's merge
    keys. The entry may contain additional fields not used for delete record
    discovery which will be ignored. Deleted records will be filtered from
    query results at runtime.
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
    Parameters that control manifest entry interpretation.

    For EQUALITY_DELETE manifest entry types, parameters include equality
    field identifiers.
    """

    @staticmethod
    def of(
        equality_field_locators: Optional[List[FieldLocator]] = None,
    ) -> EntryParams:
        params = EntryParams()
        if equality_field_locators is not None:
            params["equality_field_locators"] = equality_field_locators
        return params

    @property
    def equality_field_locators(self) -> Optional[List[FieldLocator]]:
        return self.get("equality_field_locators")


class Manifest(dict):
    """
    A DeltaCAT manifest contains metadata common to multiple manifest formats
    like Amazon Redshift and Apache Iceberg to simplify dataset import/export.
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
    ) -> Manifest:
        if not uuid:
            uuid = str(uuid4())
        total_record_count = 0
        total_content_length = 0
        total_source_content_length = 0
        content_type = None
        content_encoding = None
        credentials = None
        content_type_params = None
        if entries:
            content_type = entries[0].meta.content_type
            content_encoding = entries[0].meta.content_encoding
            credentials = entries[0].meta.credentials
            content_type_params = entries[0].meta.content_type_parameters
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
                entry_content_encoding = meta.get("content_encoding", None)
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
                actual_credentials = meta.credentials
                if credentials and (actual_credentials != credentials):
                    msg = (
                        f"Expected all manifest entries to have credentials "
                        f"'{credentials}' but found '{actual_credentials}'"
                    )
                    raise ValueError(msg)
                actual_content_type_params = meta.content_type_parameters
                if content_type_params and (
                    actual_content_type_params != content_type_params
                ):
                    msg = (
                        f"Expected all manifest entries to have content type params "
                        f"'{content_type_params}' but found '{actual_content_type_params}'"
                    )
                    raise ValueError(msg)

                total_record_count += meta.record_count or 0
                total_content_length += meta.content_length or 0
                total_source_content_length += meta.source_content_length or 0

        meta = ManifestMeta.of(
            record_count=total_record_count,
            content_length=total_content_length,
            content_type=content_type,
            content_encoding=content_encoding,
            source_content_length=total_source_content_length,
            credentials=credentials,
            content_type_parameters=content_type_params,
            entry_type=entry_type,
            entry_params=entry_params,
        )
        manifest = Manifest._build_manifest(meta, entries, author, uuid)
        return manifest

    @staticmethod
    def from_json(json_string: str) -> Manifest:
        parsed_dict = json.loads(json_string)
        return Manifest.of(
            entries=ManifestEntryList.of(
                [
                    ManifestEntry.from_dict(entry)
                    for entry in parsed_dict.get("entries", [])
                ]
            ),
            author=ManifestAuthor.from_dict(parsed_dict.get("author")),
            uuid=parsed_dict.get("id"),
        )

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
            manifest_meta["entry_type"] = (
                entry_type.value if isinstance(entry_type, EntryType) else entry_type
            )
        if entry_params is not None:
            manifest_meta["entry_params"] = entry_params
        return manifest_meta

    @staticmethod
    def from_dict(obj: dict) -> Optional[ManifestMeta]:
        if obj is None:
            return None

        return ManifestMeta.of(
            record_count=obj.get("record_count"),
            content_length=obj.get("content_length"),
            content_type=obj.get("content_type"),
            content_encoding=obj.get("content_encoding"),
            source_content_length=obj.get("source_content_length"),
            credentials=obj.get("credentials"),
            content_type_parameters=obj.get("content_type_parameters"),
            entry_type=obj.get("entry_type"),
            entry_params=obj.get("entry_params"),
        )

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
    def entry_params(self) -> Optional[EntryParams]:
        val: Dict[str, Any] = self.get("entry_params")
        if val is not None and not isinstance(val, EntryParams):
            self["entry_params"] = val = EntryParams(val)
        return val


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
            record_count=record_count,
            content_length=s3_obj["ContentLength"],
            content_type=s3_obj["ContentType"],
            content_encoding=s3_obj["ContentEncoding"],
            source_content_length=source_content_length,
        )
        manifest_entry = ManifestEntry.of(url, manifest_entry_meta)
        return manifest_entry

    @staticmethod
    def from_dict(obj: dict) -> ManifestEntry:
        return ManifestEntry.of(
            url=obj.get("url"),
            uri=obj.get("uri"),
            meta=ManifestMeta.from_dict(obj.get("meta")),
            mandatory=obj.get("mandatory", True),
            uuid=obj.get("id"),
        )

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

    @staticmethod
    def from_dict(obj: dict) -> Optional[ManifestAuthor]:
        if obj is None:
            return None
        return ManifestAuthor.of(obj.get("name"), obj.get("version"))

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
