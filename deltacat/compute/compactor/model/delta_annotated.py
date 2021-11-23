# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from deltacat import logs
from deltacat.storage import Delta, DeltaType, Manifest, ManifestEntry, \
    ManifestEntryList
from typing import List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DeltaAnnotation(tuple):
    @staticmethod
    def of(file_index: int,
           delta_type: DeltaType,
           stream_position: int) -> DeltaAnnotation:
        return DeltaAnnotation((file_index, delta_type, stream_position))

    @property
    def annotation_file_index(self) -> int:
        return self[0]

    @property
    def annotation_delta_type(self) -> Optional[DeltaType]:
        return self[1]

    @property
    def annotation_stream_position(self) -> Optional[int]:
        return self[2]


class DeltaAnnotated(Delta):
    @staticmethod
    def of(delta: Delta) -> DeltaAnnotated:
        """
        Creates an annotated delta built from the input delta, which saves all
        properties for each delta manifest entry. All operations performed on
        the annotated delta by this module will preserve a mapping back to the
        original delta manifest entry indices and properties.
        """
        delta_annotated = DeltaAnnotated()
        delta_annotated.update(delta)
        entries = delta.manifest.entries
        _annotations = []
        if entries:
            dtype = delta.type
            pos = delta.stream_position
            _annotations = [DeltaAnnotation.of(i, dtype, pos) for i in
                            range(len(entries))]
        delta_annotated.annotations = _annotations
        return delta_annotated

    @staticmethod
    def rebatch(
            annotated_deltas: List[DeltaAnnotated],
            min_delta_bytes) -> List[DeltaAnnotated]:
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
        new_da = DeltaAnnotated()
        new_da_bytes = 0
        da_group_entry_count = 0
        for src_da in annotated_deltas:
            src_da_annotations = src_da.annotations
            src_da_entries = src_da.manifest.entries
            assert(len(src_da_annotations) == len(src_da_entries),
                   f"Unexpected Error: Length of delta annotations "
                   f"({len(src_da_annotations)}) doesn't mach the length of "
                   f"delta manifest entries ({len(src_da_entries)}).")
            for i, src_entry in enumerate(src_da_entries):
                DeltaAnnotated._append_annotated_entry(
                    src_da,
                    new_da,
                    src_entry,
                    src_da_annotations[i])
                # TODO: Fetch s3_obj["Size"] if entry content length undefined?
                new_da_bytes += src_entry.meta.content_length
                da_group_entry_count += 1
                if new_da_bytes >= min_delta_bytes:
                    logger.info(
                        f"Appending group of {da_group_entry_count} elements "
                        f"and {new_da_bytes} bytes.")
                    groups.append(new_da)
                    new_da = DeltaAnnotated()
                    new_da_bytes = 0
                    da_group_entry_count = 0
        if new_da:
            groups.append(new_da)
        return groups

    @property
    def annotations(self) -> List[DeltaAnnotation]:
        return self["annotations"]

    @annotations.setter
    def annotations(self, _annotations: List[DeltaAnnotation]):
        self["annotations"] = _annotations

    @staticmethod
    def _append_annotated_entry(
            src_da: DeltaAnnotated,
            dst_da: DeltaAnnotated,
            src_entry: ManifestEntry,
            src_annotation: DeltaAnnotation):

        if not dst_da:
            # copy all extended properties from the source delta manifest impl
            dst_da.update(src_da)
            dst_da.manifest = Manifest.of(ManifestEntryList([src_entry]))
            dst_da.annotations = [src_annotation]
        else:
            entries = dst_da.manifest.entries
            src_dl = src_da.locator
            dst_dl = dst_da.locator
            # remove delta type and stream position if there is a conflict
            if src_da.type != dst_da.type:
                dst_da.type = None
            if src_dl.stream_position != dst_dl.stream_position:
                dst_dl.stream_position = None
            entries.append(src_entry)
            dst_da.annotations.append(src_annotation)
