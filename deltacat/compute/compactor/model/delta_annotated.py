# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from types import FunctionType
from typing import Callable, List, Optional, Union

from deltacat import logs
from deltacat.storage import (
    Delta,
    DeltaType,
    Manifest,
    ManifestEntry,
    ManifestEntryList,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DeltaAnnotation(tuple):
    @staticmethod
    def of(
        file_index: int, delta_type: DeltaType, stream_position: int
    ) -> DeltaAnnotation:
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
            _annotations = [
                DeltaAnnotation.of(i, dtype, pos) for i in range(len(entries))
            ]
        delta_annotated.annotations = _annotations
        return delta_annotated

    @staticmethod
    def rebatch(
        annotated_deltas: List[DeltaAnnotated],
        min_delta_bytes: float,
        min_file_counts: Optional[Union[int, float]] = float("inf"),
        estimation_function: Optional[Callable] = None,
    ) -> List[DeltaAnnotated]:
        """
        Simple greedy algorithm to split/merge 1 or more annotated deltas into
        size-limited annotated deltas. All ordered manifest entries in the input
        annotated deltas are appended to an annotated delta until
        delta_size_bytes >= min_delta_bytes OR delta_file_count >= min_file_counts,
        then a new delta is started. Note that byte size is measured in terms of
        manifest entry content length, which is expected to be equal to the number
        of bytes at rest for the associated object. Returns the list of annotated
        delta groups.
        """
        groups = []
        new_da = DeltaAnnotated()
        new_da_bytes = 0
        da_group_entry_count = 0
        for src_da in annotated_deltas:
            src_da_annotations = src_da.annotations
            src_da_entries = src_da.manifest.entries
            assert (
                len(src_da_annotations) == len(src_da_entries),
                f"Unexpected Error: Length of delta annotations "
                f"({len(src_da_annotations)}) doesn't mach the length of "
                f"delta manifest entries ({len(src_da_entries)}).",
            )
            for i, src_entry in enumerate(src_da_entries):
                # create a new da group if src and dest has different delta locator
                # (i.e. the previous compaction round ran a rebase)
                if new_da and src_da.locator != new_da.locator:
                    groups.append(new_da)
                    logger.info(
                        f"Due to different delta locator, Appending group of {da_group_entry_count} elements "
                        f"and {new_da_bytes} bytes"
                    )
                    new_da = DeltaAnnotated()
                    new_da_bytes = 0
                    da_group_entry_count = 0
                DeltaAnnotated._append_annotated_entry(
                    src_da, new_da, src_entry, src_da_annotations[i]
                )
                # TODO: Fetch s3_obj["Size"] if entry content length undefined?
                estimated_new_da_bytes = (
                    estimation_function(src_entry.meta.content_length)
                    if type(estimation_function) is FunctionType
                    else src_entry.meta.content_length
                )
                new_da_bytes += estimated_new_da_bytes
                da_group_entry_count += 1
                if (
                    new_da_bytes >= min_delta_bytes
                    or da_group_entry_count >= min_file_counts
                ):
                    if new_da_bytes >= min_delta_bytes:
                        logger.info(
                            f"Appending group of {da_group_entry_count} elements "
                            f"and {new_da_bytes} bytes to meet file size limit"
                        )
                    if da_group_entry_count >= min_file_counts:
                        logger.info(
                            f"Appending group of {da_group_entry_count} elements "
                            f"and {da_group_entry_count} files to meet file count limit"
                        )
                    groups.append(new_da)
                    new_da = DeltaAnnotated()
                    new_da_bytes = 0
                    da_group_entry_count = 0
        if new_da:
            groups.append(new_da)
        return groups

    @staticmethod
    def split(src_da: DeltaAnnotated, pieces: int) -> List[DeltaAnnotated]:
        groups = []
        new_da = DeltaAnnotated()
        da_group_entry_count = 0
        src_da_annotations = src_da.annotations
        src_da_entries = src_da.manifest.entries
        assert (
            len(src_da_annotations) == len(src_da_entries),
            f"Unexpected Error: Length of delta annotations "
            f"({len(src_da_annotations)}) doesn't mach the length of "
            f"delta manifest entries ({len(src_da_entries)}).",
        )
        src_da_entries_length = len(src_da_entries)
        equal_length = src_da_entries_length // pieces
        for i in range(len(src_da_entries)):
            DeltaAnnotated._append_annotated_entry(
                src_da, new_da, src_da_entries[i], src_da_annotations[i]
            )
            # TODO: Fetch s3_obj["Size"] if entry content length undefined?
            da_group_entry_count += 1
            if da_group_entry_count >= equal_length and i < equal_length * (pieces - 1):
                logger.info(
                    f"Splitting {da_group_entry_count} manifest files "
                    f"to {pieces} pieces of {equal_length} size."
                )
                groups.append(new_da)
                new_da = DeltaAnnotated()
                da_group_entry_count = 0
            if i == len(src_da_entries) - 1:
                groups.append(new_da)
                logger.info(
                    f"Splitting {da_group_entry_count} manifest files "
                    f"to {pieces} pieces of {equal_length} size."
                )
                new_da = DeltaAnnotated()
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
        src_annotation: DeltaAnnotation,
    ):

        if not dst_da:
            # copy all extended properties from the source delta manifest impl
            dst_da.update(src_da)
            dst_da.manifest = Manifest.of(ManifestEntryList([src_entry]))
            dst_da.annotations = [src_annotation]
        else:
            entries = dst_da.manifest.entries
            src_dl = src_da.locator
            dst_dl = dst_da.locator
            assert (
                src_dl == dst_dl
            ), f"Delta locator should be same when grouping entries"
            # remove delta type if there is a conflict
            if src_da.type != dst_da.type:
                dst_da.type = None
            entries.append(src_entry)
            dst_da.annotations.append(src_annotation)
