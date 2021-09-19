from deltacat.storage.model.types import DeltaType
from deltacat.storage.model import delta_locator as dl, partition_locator as pl
from deltacat.aws.redshift.model import manifest as rsm
from typing import Any, Dict, List, Optional


def of(
        delta_locator: Optional[Dict[str, Any]],
        delta_type: Optional[DeltaType],
        manifest_meta: Optional[Dict[str, Any]],
        properties: Optional[Dict[str, str]],
        manifest: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Creates a Delta metadata model with the given Delta Locator, Delta Type,
    manifest metadata, properties, and manifest.
    """
    return {
        "manifest": manifest,
        "meta": manifest_meta,
        "type": delta_type,
        "deltaLocator": delta_locator,
        "properties": properties,
    }


def merge_deltas(
        deltas: List[Dict[str, Any]],
        manifest_author: Optional[Dict[str, Any]] = None,
        stream_position: Optional[int] = None,
        properties: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Merges the input list of deltas into a single delta. All input deltas to
    merge must belong to the same partition, share the same delta type, and
    have non-empty manifests.

    Manifest content type and content encoding will be set to None in the event
    of conflicting types or encodings between individual manifest entries.
    Missing record counts, content lengths, and source content lengths will be
    coalesced to 0. Delta properties, stream position, and manifest author
    will be set to None unless explicitly specified.

    Input delta manifest entry order will be preserved in the merged delta
    returned. That is, if manifest entry A preceded manifest entry B
    in the input delta list, then manifest entry A will precede manifest entry
    B in the merged delta.
    """
    if not deltas:
        raise ValueError("No deltas given to merge.")
    manifests = [get_manifest(d) for d in deltas]
    if any(not m for m in manifests):
        raise ValueError(f"Deltas to merge must all have non-empty manifests.")
    distinct_storage_types = set([get_storage_type(d) for d in deltas])
    if len(distinct_storage_types) > 1:
        raise NotImplementedError(
            f"Deltas to merge must all share the same storage type "
            f"(found {len(distinct_storage_types)} storage types.")
    pl_digest_set = set([pl.digest(get_partition_locator(d)) for d in deltas])
    if len(pl_digest_set) > 1:
        raise ValueError(
            f"Deltas to merge must all belong to the same partition (found "
            f"{len(pl_digest_set)} partitions).")
    distinct_delta_types = set([get_delta_type(m) for m in deltas])
    if len(distinct_delta_types) > 1:
        raise ValueError(f"Deltas to merge must all share the same delta type "
                         f"(found {len(distinct_delta_types)} delta types).")
    merged_manifest = rsm.merge_manifests(
        manifests,
        manifest_author,
    )
    partition_locator = get_partition_locator(deltas[0])
    return of(
        dl.of(partition_locator, stream_position),
        distinct_delta_types.pop(),
        rsm.get_meta(merged_manifest),
        properties,
        merged_manifest,
    )


def get_manifest(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return delta.get("manifest")


def set_manifest(
        delta: Dict[str, Any],
        manifest: Optional[Dict[str, Any]]) -> None:

    delta["manifest"] = manifest


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

    return delta.get("deltaLocator")


def set_delta_locator(
        delta: Dict[str, Any],
        delta_locator: Optional[Dict[str, Any]]) -> None:

    delta["deltaLocator"] = delta_locator


def get_namespace_locator(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_namespace_locator(delta_locator)
    return None


def get_table_locator(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_table_locator(delta_locator)
    return None


def get_table_version_locator(delta: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_table_version_locator(delta_locator)
    return None


def get_stream_locator(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_stream_locator(delta_locator)
    return None


def get_partition_locator(delta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_partition_locator(delta_locator)
    return None


def get_storage_type(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_storage_type(delta_locator)
    return None


def get_namespace(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_namespace(delta_locator)
    return None


def get_table_name(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_table_name(delta_locator)
    return None


def get_table_version(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_table_version(delta_locator)
    return None


def get_stream_id(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_stream_id(delta_locator)
    return None


def get_partition_id(delta: Dict[str, Any]) -> Optional[str]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_partition_id(delta_locator)
    return None


def get_partition_values(delta: Dict[str, Any]) -> Optional[List[Any]]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_partition_values(delta_locator)
    return None


def get_stream_position(delta: Dict[str, Any]) -> Optional[int]:
    delta_locator = get_delta_locator(delta)
    if delta_locator:
        return dl.get_stream_position(delta_locator)
    return None
