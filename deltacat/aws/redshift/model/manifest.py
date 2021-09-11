import logging
import itertools
from deltacat import logs
from uuid import uuid4
from typing import Any, Dict, List, Optional
from deltacat.aws.redshift.model import manifest_meta as rsmm
from deltacat.aws.redshift.model import manifest_entry as rsme

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _build_manifest(
        meta: Optional[Dict[str, Any]],
        entries: Optional[List[Dict[str, Any]]],
        author: Optional[Dict[str, Any]] = None,
        uuid: str = str(uuid4())) -> Dict[str, Any]:

    manifest = {"id": uuid}
    if meta is not None:
        manifest["meta"] = meta
    if entries is not None:
        manifest["entries"] = entries
    if author is not None:
        manifest["author"] = author
    return manifest


def of(
        entries: List[Dict[str, Any]],
        author: Optional[Dict[str, Any]] = None,
        uuid: str = str(uuid4())) -> Dict[str, Any]:

    total_record_count = 0
    total_content_length = 0
    total_source_content_length = 0
    if entries:
        content_type = rsmm.get_content_type(rsme.get_meta(entries[0]))
        content_encoding = rsmm.get_content_encoding(rsme.get_meta(entries[0]))
    for entry in entries:
        meta = rsme.get_meta(entry)
        if rsmm.get_content_type(meta) != content_type:
            content_type = None
        if rsmm.get_content_encoding(meta) != content_encoding:
            content_encoding = None
        entry_content_type = rsmm.get_content_type(meta)
        if entry_content_type != content_type:
            msg = f"Expected all manifest entries to have content type " \
                  f"'{content_type}' but found '{entry_content_type}'"
            raise ValueError(msg)
        entry_content_encoding = meta["content_encoding"]
        if entry_content_encoding != content_encoding:
            msg = f"Expected all manifest entries to have content " \
                  f"encoding '{content_encoding}' but found " \
                  f"'{entry_content_encoding}'"
            raise ValueError(msg)
        total_record_count += rsmm.get_record_count(meta, 0)
        total_content_length += rsmm.get_content_length(meta, 0)
        total_source_content_length += rsmm.get_source_content_length(meta, 0)
    meta = rsmm.of(
        total_record_count,
        total_content_length,
        content_type,
        content_encoding,
        total_source_content_length
    )
    manifest = _build_manifest(
        meta,
        entries,
        author,
        uuid
    )
    return manifest


def merge_manifests(
        manifests: List[Dict[str, Any]],
        author: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

    all_entries = list(itertools.chain(*[get_entries(m) for m in manifests]))
    print(f"all_entries: {all_entries}")
    merged_manifest = of(
        all_entries,
        author)
    return merged_manifest


def get_meta(manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return manifest.get("meta")


def get_entries(manifest: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    return manifest.get("entries")


def get_id(manifest: Dict[str, Any]) -> str:
    return manifest["id"]


def get_author(manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return manifest.get("author")
