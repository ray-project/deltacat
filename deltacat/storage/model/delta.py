from deltacat.storage.model.types import DeltaType
from typing import Any, Dict, Optional


def of(
        delta_locator: Optional[Dict[str, Any]],
        delta_type: Optional[DeltaType],
        manifest_meta: Optional[Dict[str, Any]],
        properties: Optional[Dict[str, str]]) -> Dict[str, Any]:
    """
    Creates a Delta metadata model with the given Delta Locator, Delta Type,
    manifest metadata, and properties. Note that this does not include the
    full Delta manifest, and is thus better suited for use-cases that require
    a small memory footprint and/or low-latency exchange than the related
    Delta Manifest model.

    However, the Delta Manifest model is better suited for use-cases whose
    latency may suffer via repeated Manifest downloads, or that require merging
    multiple Manifests (e.g. from incrementally staged Deltas) into a single
    Delta.
    """
    return {
        "meta": manifest_meta,
        "type": delta_type,
        "locator": delta_locator,
        "properties": properties,
    }


def get_meta(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return delta.get("meta")


def set_meta(
        delta: Dict[str, Any],
        meta: Optional[Dict[str, Any]]) -> None:

    delta["meta"] = meta


def get_properties(delta: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return delta.get("properties")


def set_properties(
        delta: Dict[str, Any],
        properties: Optional[Dict[str, str]]) -> None:

    delta["properties"] = properties


def get_delta_type(delta: Dict[str, Any]) -> Optional[DeltaType]:
    delta_type = delta.get("type")
    return None if delta_type is None else DeltaType(delta_type)


def set_delta_type(
        delta: Dict[str, Any],
        delta_type: Optional[DeltaType]) -> None:

    delta["type"] = delta_type


def get_delta_locator(delta: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return delta.get("locator")


def set_delta_locator(
        delta: Dict[str, Any],
        delta_locator: Optional[Dict[str, Any]]) -> None:

    delta["locator"] = delta_locator
