import logging
from deltacat import logs
from deltacat.storage.model import delta_locator as dl, delta_manifest as dm
from deltacat.aws.redshift.model import manifest as rsm, \
    manifest_entry as rsme, manifest_meta as rsmm
from deltacat.storage.model.types import DeltaType
from typing import Any, Dict, List, Optional, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def from_delta_manifest(delta_manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns an annotated delta manifest built from the input delta manifest,
    which saves all delta manifest properties for each manifest entry. All
    operations performed on the annotated delta manifest by this module will
    preserve a mapping back to the original delta manifest entry indices and
    properties.
    """
    delta_manifest_annotated = {}
    delta_manifest_annotated.update(delta_manifest)
    entries = rsm.get_entries(dm.get_manifest(delta_manifest))
    annotations = []
    if entries:
        dtype = dm.get_delta_type(delta_manifest)
        pos = dl.get_stream_position(dm.get_delta_locator(delta_manifest))
        annotations = [_annotation(i, dtype, pos) for i in range(len(entries))]
    set_annotations(delta_manifest_annotated, annotations)
    return delta_manifest_annotated


def size_limited_groups(
        annotated_delta_manifests: List[Dict[str, Any]],
        min_sublist_bytes) -> List[Dict[str, Any]]:
    """
    Simple greedy algorithm to group 1 or more annotated delta manifests into
    size-limited annotated delta manifests. All ordered entries in the input
    annotated delta manifests are appended to an annotated delta manifest group
    until group_size_bytes >= min_sublist_bytes, then a new group is started.
    Note that byte size is measured in terms of manifest entry content length,
    which is expected to be equal to the number of bytes at rest in S3 for the
    associated object. Returns the list of annotated delta manifest groups.
    """
    groups = []
    dma_group = {}
    dma_group_bytes = 0
    dma_group_entry_count = 0
    for src_dma in annotated_delta_manifests:
        src_dma_annotations = get_annotations(src_dma)
        src_dma_entries = rsm.get_entries(dm.get_manifest(src_dma))
        assert(len(src_dma_annotations) == len(src_dma_entries),
               f"Unexpected Error: Length of delta manifest annotations "
               f"({len(src_dma_annotations)}) doesn't mach the length of delta "
               f"manifest entries ({len(src_dma_entries)}).")
        for i, src_entry in enumerate(src_dma_entries):
            _append_annotated_entry(
                src_dma,
                dma_group,
                src_entry,
                src_dma_annotations[i])
            # TODO: Fetch s3_obj["Size"] if entry content length undefined?
            dma_group_bytes += rsmm.get_content_length(rsme.get_meta(src_entry))
            dma_group_entry_count += 1
            if dma_group_bytes >= min_sublist_bytes:
                logger.info(
                    f"Appending group of {dma_group_entry_count} elements and "
                    f"{dma_group_bytes} bytes.")
                groups.append(dma_group)
                dma_group = {}
                dma_group_bytes = 0
                dma_group_entry_count = 0
    if dma_group:
        groups.append(dma_group)
    return groups


def get_annotations(delta_manifest_annotated: Dict[str, Any]) \
        -> List[Tuple[int, Optional[DeltaType], Optional[int]]]:

    return delta_manifest_annotated["annotations"]


def set_annotations(
        delta_manifest_annotated: Dict[str, Any],
        annotations: List[Tuple[int, Optional[DeltaType], Optional[int]]]):

    delta_manifest_annotated["annotations"] = annotations


def get_annotation_file_index(
        annotation: Tuple[int, Optional[DeltaType], Optional[int]]) -> int:

    return annotation[0]


def get_annotation_delta_type(
        annotation: Tuple[int, Optional[DeltaType], Optional[int]]) \
        -> Optional[DeltaType]:

    return annotation[1]


def get_annotation_stream_position(
        annotation: Tuple[int, Optional[DeltaType], Optional[int]]) \
        -> Optional[int]:

    return annotation[2]


def _annotation(
        entry_index: int,
        delta_type: Optional[DeltaType],
        stream_position: Optional[int]) \
        -> Tuple[int, Optional[DeltaType], Optional[int]]:
    """
    Creates a delta manifest annotation as a tuple of
    (entry_index, delta_type, stream_position)
    """
    return entry_index, delta_type, stream_position


def _append_annotated_entry(
        src_dma: Dict[str, Any],
        dst_dma: Dict[str, Any],
        src_entry: Dict[str, Any],
        src_annotation: Tuple[int, Optional[DeltaType], Optional[int]]):

    if not dst_dma:
        # copy all extended properties from the source delta manifest impl
        dst_dma.update(src_dma)
        dm.set_manifest(dst_dma, rsm.of([src_entry]))
        set_annotations(dst_dma, [src_annotation])
    else:
        entries = rsm.get_entries(dm.get_manifest(dst_dma))
        src_delta_locator = dm.get_delta_locator(src_dma)
        dst_delta_locator = dm.get_delta_locator(dst_dma)
        # remove delta type and stream position if there is a conflict
        if dm.get_delta_type(src_dma) \
                != dm.get_delta_type(dst_dma):
            dm.set_delta_type(dst_dma, None)
        if dl.get_stream_position(src_delta_locator) \
                != dl.get_stream_position(dst_delta_locator):
            dl.set_stream_position(dst_delta_locator, None)
        entries.append(src_entry)
        get_annotations(dst_dma).append(src_annotation)
