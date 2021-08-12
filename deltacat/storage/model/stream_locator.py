import json
from typing import Any, Dict, Optional
from deltacat.utils.common import sha1_hexdigest


def of(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
        stream_id: Optional[str],
        storage_type: Optional[str]) -> Dict[str, Any]:

    return {
        "namespace": namespace,
        "tableName": table_name,
        "tableVersion": table_version,
        "streamId": stream_id,
        "storageType": storage_type,
    }


def get_namespace(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("namespace")


def set_namespace(
        stream_locator: Dict[str, Any],
        namespace: Optional[str]):

    stream_locator["namespace"] = namespace


def get_table_name(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("tableName")


def set_table_name(
        stream_locator: Dict[str, Any],
        table_name: Optional[str]):

    stream_locator["tableName"] = table_name


def get_table_version(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("tableVersion")


def set_table_version(
        stream_locator: Dict[str, Any],
        table_version: Optional[str]):

    stream_locator["tableVersion"] = table_version


def get_stream_id(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("streamId")


def set_stream_id(
        stream_locator: Dict[str, Any],
        stream_id: Optional[str]):

    stream_locator["streamId"] = stream_id


def get_storage_type(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("storageType")


def set_storage_type(
        stream_locator: Dict[str, Any],
        storage_type: Optional[str]):

    stream_locator["storageType"] = storage_type


def hexdigest(stream_locator: Dict[str, Any]) -> str:
    return sha1_hexdigest(
        json.dumps([stream_locator], sort_keys=True).encode("utf-8")
    )
