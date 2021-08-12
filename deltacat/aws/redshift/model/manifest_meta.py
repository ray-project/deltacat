import logging
from deltacat import logs
from typing import Any, Dict, List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def of(
        record_count: Optional[int],
        content_length: Optional[int],
        content_type: Optional[str],
        content_encoding: Optional[str],
        source_content_length: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
        content_type_parameters: Optional[List[Dict[str, str]]] = None) \
        -> Dict[str, Any]:

    manifest_meta = {}
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


def get_record_count(manifest_meta: Dict[str, Any], *args) -> Optional[int]:
    return manifest_meta.get("record_count", *args)


def get_content_length(manifest_meta: Dict[str, Any], *args) -> Optional[int]:
    return manifest_meta.get("content_length", *args)


def get_content_type(manifest_meta: Dict[str, Any]) -> Optional[str]:
    return manifest_meta.get("content_type")


def get_content_encoding(manifest_meta: Dict[str, Any]) -> Optional[str]:
    return manifest_meta.get("content_encoding")


def get_source_content_length(manifest_meta: Dict[str, Any], *args) \
        -> Optional[int]:
    return manifest_meta.get("source_content_length", *args)


def get_content_type_parameters(
        manifest_meta: Dict[str, Any]) -> Optional[List[Dict[str, str]]]:

    return manifest_meta.get("content_type_parameters")


def get_credentials(manifest_meta: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return manifest_meta.get("credentials")
