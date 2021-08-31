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


def get_meta(delta_manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return delta_manifest.get("meta")


def set_meta(
        delta_manifest: Dict[str, Any],
        meta: Optional[Dict[str, Any]]):

    delta_manifest["meta"] = meta


def get_properties(delta_manifest: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return delta_manifest.get("properties")


def set_properties(
        delta_manifest: Dict[str, Any],
        properties: Optional[Dict[str, str]]):

    delta_manifest["properties"] = properties


def get_delta_type(delta_manifest: Dict[str, Any]) -> Optional[DeltaType]:
    return delta_manifest.get("type")


def set_delta_type(
        delta_manifest: Dict[str, Any],
        delta_type: Optional[DeltaType]):

    delta_manifest["type"] = delta_type


def get_delta_locator(delta_manifest: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return delta_manifest.get("locator")


def set_delta_locator(
        delta_manifest: Dict[str, Any],
        delta_locator: Optional[Dict[str, Any]]):

    delta_manifest["locator"] = delta_locator
