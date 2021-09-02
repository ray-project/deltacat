from deltacat.storage.model import delta_manifest as dm, delta_locator as dl, \
    partition_locator as pl, stream_locator as sl
from deltacat.aws.redshift.model import manifest as rsm
from deltacat.storage.model.types import DeltaType
from typing import Any, Dict, List, Optional


def of(
        delta_locator: Optional[Dict[str, Any]],
        delta_type: Optional[DeltaType],
        manifest: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Creates a Delta Manifest model for the given Delta Locator, Delta Type,
    and Manifest. Note that this is a more heavyweight model than the Delta
    metadata model, since a Manifest includes metadata for every file in the
    Delta. Thus, use-cases that require a small memory footprint and/or
    low-latency exchange of Delta information should consider using the
    Delta metadata model to lazily load the associated manifest via this model.
    However, this model is ideal for use-cases whose latency may otherwise
    suffer via repeated Manifest downloads, or that require merging multiple
    Manifests (e.g. from incrementally staged Deltas) into a single Delta.
    """
    return {
        "manifest": manifest,
        "type": delta_type,
        "locator": delta_locator,
    }


def merge_delta_manifests(
        delta_manifests: List[Dict[str, Any]],
        author: Optional[Dict[str, Any]] = None,
        stream_position: Optional[int] = None) -> Dict[str, Any]:
    """
    Merges the input list of delta manifests into a single delta manifest,
    optionally with a new manifest author. All input delta manifests to merge
    must belong to the same partition and share the same delta type.
    Manifest content type and content encoding will be set to None in the event
    of conflicting types or encodings between individual manifest entries.
    Missing record counts, content lengths, and source content lengths will be
    coalesced to 0.

    Input delta manifest entry order will will be preserved in the merged delta
    manifest returned. That is, if manifest entry A preceded manifest entry B
    in the input delta manifest list, then manifest entry A will precede
    manifest entry B in the merged delta manifest.
    """
    if not delta_manifests:
        raise ValueError("No delta manifests given to merge.")
    distinct_storage_types = set([
        sl.get_storage_type(
            pl.get_stream_locator(
                dl.get_partition_locator(
                    dm.get_delta_locator(m)
                )
            )
        ) for m in delta_manifests
    ])
    if len(distinct_storage_types > 1):
        raise NotImplementedError("Delta manifest to merge must all share the "
                                  "same storage type.")
    distinct_partition_locators = set([
        pl.hexdigest(
            dl.get_partition_locator(
                dm.get_delta_locator(m)
            )
        ) for m in delta_manifests
    ])
    if len(distinct_partition_locators) > 1:
        raise ValueError("Delta manifests to merge must all belong to the same "
                         "partition.")
    distinct_delta_types = set([dm.get_delta_type(m) for m in delta_manifests])
    if len(distinct_delta_types) > 1:
        raise ValueError("Delta manifests to merge must all share the same "
                         "delta type.")
    merged_manifest = rsm.merge_manifests(
        [dm.get_manifest(m) for m in delta_manifests],
        author,
    )
    return dm.of(
        dl.of(distinct_partition_locators.pop(), stream_position),
        distinct_delta_types.pop(),
        merged_manifest,
    )


def get_manifest(delta_manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return delta_manifest.get("manifest")


def set_manifest(
        delta_manifest: Dict[str, Any],
        manifest: Optional[Dict[str, Any]]):

    delta_manifest["manifest"] = manifest


def get_delta_type(delta_manifest: Dict[str, Any]) -> Optional[DeltaType]:
    delta_type = delta_manifest.get("type")
    return None if delta_type is None else DeltaType(delta_type)


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
