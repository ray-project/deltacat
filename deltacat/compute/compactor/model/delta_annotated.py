import logging
from deltacat import logs
from deltacat.storage.model import delta_locator as dl, delta as dc_delta
from deltacat.aws.redshift.model import manifest as rsm, \
    manifest_entry as rsme, manifest_meta as rsmm
from deltacat.storage.model.types import DeltaType
from typing import Any, Dict, List, Optional, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def from_delta(delta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns an annotated delta built from the input delta, which saves all
    properties for each delta manifest entry. All operations performed on the
    annotated delta by this module will preserve a mapping back to the original
    delta manifest entry indices and properties.
    """
    delta_annotated = {}
    delta_annotated.update(delta)
    entries = rsm.get_entries(dc_delta.get_manifest(delta))
    annotations = []
    if entries:
        dtype = dc_delta.get_delta_type(delta)
        pos = dc_delta.get_stream_position(delta)
        annotations = [_annotation(i, dtype, pos) for i in range(len(entries))]
    set_annotations(delta_annotated, annotations)
    return delta_annotated


def rebatch(
        annotated_deltas: List[Dict[str, Any]],
        min_delta_bytes) -> List[Dict[str, Any]]:
    """
    Simple greedy algorithm to split/merge 1 or more annotated deltas into
    size-limited annotated deltas. All ordered manifest entries in the input
    annotated deltas are appended to an annotated delta until
    delta_size_bytes >= min_delta_bytes, then a new delta is started. Note
    that byte size is measured in terms of manifest entry content length,
    which is expected to be equal to the number of bytes at rest for the
    associated object. Returns the list of annotated delta groups.
    """
    groups = []
    new_da = {}
    new_da_bytes = 0
    da_group_entry_count = 0
    for src_da in annotated_deltas:
        src_da_annotations = get_annotations(src_da)
        src_da_entries = rsm.get_entries(dc_delta.get_manifest(src_da))
        assert(len(src_da_annotations) == len(src_da_entries),
               f"Unexpected Error: Length of delta annotations "
               f"({len(src_da_annotations)}) doesn't mach the length of delta "
               f"manifest entries ({len(src_da_entries)}).")
        for i, src_entry in enumerate(src_da_entries):
            _append_annotated_entry(
                src_da,
                new_da,
                src_entry,
                src_da_annotations[i])
            # TODO: Fetch s3_obj["Size"] if entry content length undefined?
            new_da_bytes += rsmm.get_content_length(rsme.get_meta(src_entry))
            da_group_entry_count += 1
            if new_da_bytes >= min_delta_bytes:
                logger.info(
                    f"Appending group of {da_group_entry_count} elements and "
                    f"{new_da_bytes} bytes.")
                groups.append(new_da)
                new_da = {}
                new_da_bytes = 0
                da_group_entry_count = 0
    if new_da:
        groups.append(new_da)
    return groups


def get_annotations(delta_annotated: Dict[str, Any]) \
        -> List[Tuple[int, Optional[DeltaType], Optional[int]]]:

    return delta_annotated["annotations"]


def set_annotations(
        delta_annotated: Dict[str, Any],
        annotations: List[Tuple[int, Optional[DeltaType], Optional[int]]]):

    delta_annotated["annotations"] = annotations


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
        src_da: Dict[str, Any],
        dst_da: Dict[str, Any],
        src_entry: Dict[str, Any],
        src_annotation: Tuple[int, Optional[DeltaType], Optional[int]]):

    if not dst_da:
        # copy all extended properties from the source delta manifest impl
        dst_da.update(src_da)
        dc_delta.set_manifest(dst_da, rsm.of([src_entry]))
        set_annotations(dst_da, [src_annotation])
    else:
        entries = rsm.get_entries(dc_delta.get_manifest(dst_da))
        src_dl = dc_delta.get_delta_locator(src_da)
        dst_dl = dc_delta.get_delta_locator(dst_da)
        # remove delta type and stream position if there is a conflict
        if dc_delta.get_delta_type(src_da) != dc_delta.get_delta_type(dst_da):
            dc_delta.set_delta_type(dst_da, None)
        if dl.get_stream_position(src_dl) != dl.get_stream_position(dst_dl):
            dl.set_stream_position(dst_dl, None)
        entries.append(src_entry)
        get_annotations(dst_da).append(src_annotation)
