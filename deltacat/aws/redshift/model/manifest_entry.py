import logging
from deltacat import logs
from deltacat.aws.redshift.model import manifest_meta
from deltacat.aws import s3u as s3_utils
from typing import Any, Dict, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def of(
        uri: Optional[str],
        meta: Optional[Dict[str, Any]],
        mandatory: bool = True,
        url: Optional[str] = None,
        uuid: Optional[str] = None) -> Dict[str, Any]:

    manifest_entry = {}
    if not (uri or url):
        raise ValueError("No URI or URL specified for manifest entry contents.")
    if (uri and url) and (uri != url):
        raise ValueError(f"Manifest entry URI ({uri}) != URL ({url})")
    if uri:
        manifest_entry["uri"] = uri
    elif url:
        manifest_entry["url"] = url
    if meta is not None:
        manifest_entry["meta"] = meta
    if mandatory is not None:
        manifest_entry["mandatory"] = mandatory
    if uuid is not None:
        manifest_entry["id"] = uuid
    return manifest_entry


def from_s3_obj_url(
        url: str,
        record_count: int,
        source_content_length: Optional[int] = None,
        **s3_client_kwargs) -> Dict[str, Any]:

    s3_obj = s3_utils.get_object_at_url(url, **s3_client_kwargs)
    logger.debug(f"Building manifest entry from {url}: {s3_obj}")
    manifest_entry_meta = manifest_meta.of(
        record_count,
        s3_obj["ContentLength"],
        s3_obj["ContentType"],
        s3_obj["ContentEncoding"],
        source_content_length,
    )
    manifest_entry = of(
        url,
        manifest_entry_meta,
    )
    return manifest_entry


def get_uri(manifest_entry: Dict[str, Any]) -> Optional[str]:
    return manifest_entry.get("uri")


def get_url(manifest_entry: Dict[str, Any]) -> Optional[str]:
    return manifest_entry.get("url")


def get_meta(manifest_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return manifest_entry.get("meta")


def get_mandatory(manifest_entry: Dict[str, Any]) -> bool:
    return manifest_entry["mandatory"]


def get_id(manifest_entry: Dict[str, Any]) -> Optional[str]:
    return manifest_entry.get("id")
