import logging
from deltacat import logs
from typing import Any, Dict, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def of(
        name: Optional[str],
        version: Optional[str]) -> Dict[str, Any]:

    manifest_author = {}
    if name is not None:
        manifest_author["name"] = name
    if version is not None:
        manifest_author["version"] = version
    return manifest_author


def get_name(manifest_author: Dict[str, Any]) -> Optional[str]:
    return manifest_author.get("name")


def get_version(manifest_author: Dict[str, Any]) -> Optional[str]:
    return manifest_author.get("version")
