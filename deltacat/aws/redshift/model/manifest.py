# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import itertools
import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4
from enum import Enum

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DeleteTypeArgs(dict):
    @staticmethod
    def of(
        canonical_column_ids: Optional[List[str]] = None,
        file_path: Optional[str] = None,
        deleted_row_ordinal_pos: Optional[int] = None,
    ):
        delete_type_args = DeleteTypeArgs()
        if canonical_column_ids is not None:
            delete_type_args["canonical_column_ids"] = canonical_column_ids
        if file_path is not None:
            delete_type_args["file_path"] = file_path
        if deleted_row_ordinal_pos is not None:
            delete_type_args["deleted_row_ordinal_pos"] = deleted_row_ordinal_pos
        return delete_type_args


class ContentFileCategory(str, Enum):
    DATA = "data"  # the default value
    POSITIONAL_DELETE = "positional_delete"
    EQUALITY_DELETE = "equality_delete"

    @classmethod
    def list(cls):
        return [c.value for c in ContentFileCategory]


class Manifest(dict):
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
    ) -> Manifest:
        if not uuid:
            uuid = str(uuid4())
        total_record_count = 0
        total_content_length = 0
        total_source_content_length = 0
        content_type = None
        content_encoding = None
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
                total_record_count += meta.record_count or 0
                total_content_length += meta.content_length or 0
                total_source_content_length += meta.source_content_length or 0
        meta = ManifestMeta.of(
            total_record_count,
            total_content_length,
            content_type,
            content_encoding,
            total_source_content_length,
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


class ManifestEntry(dict):
    @staticmethod
    def of(
        url: Optional[str],
        meta: Optional[ManifestMeta],
        mandatory: bool = True,
        uri: Optional[str] = None,
        uuid: Optional[str] = None,
        content_file_category: Optional[ContentFileCategory] = ContentFileCategory.DATA,
        delete_type_args: Optional[DeleteTypeArgs] = None,
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
        if content_file_category is not None:
            manifest_entry["content_file_category"] = content_file_category
        if delete_type_args is not None:
            manifest_entry["delete_type_args"] = delete_type_args
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

    @property
    def content_file_category(self) -> Optional[ContentFileCategory]:
        return ContentFileCategory(self.get("content_file_category"))

    @property
    def delete_type_args(self) -> Optional[DeleteTypeArgs]:
        val: Dict[str, Any] = self.get("delete_type_args")
        if val is not None and not isinstance(val, DeleteTypeArgs):
            self["delete_type_args"] = val = DeleteTypeArgs(val)
        return val


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
