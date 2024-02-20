# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict

from ray.types import ObjectRef

from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups

from deltacat.compute.compactor_v2.utils.primary_key_index import (
    hash_group_index_to_hash_bucket_indices,
)

from deltacat.io.object_store import IObjectStore

from deltacat import logs

from deltacat.compute.compactor import DeltaFileEnvelope

from typing import List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MergeFileGroup(dict):
    @staticmethod
    def of(dfe_groups: List[List[DeltaFileEnvelope]], hb_index: Optional[int] = None):
        """
        Creates a container with delta file envelope groupings and other
        additional properties used primarily for the merging step.

        Args:
            dfe_groups: A list of delta file envelope groups.
            hb_index: If present, this signifies the hash bucket index corresponding to the envelope delta file groups.

        Returns:
            A dict

        """
        d = MergeFileGroup()
        d["dfe_groups"] = dfe_groups
        d["hb_index"] = hb_index
        return d

    @property
    def dfe_groups(self) -> List[List[DeltaFileEnvelope]]:
        return self["dfe_groups"]

    @property
    def hb_index(self) -> Optional[int]:
        return self["hb_index"]


class MergeFileGroupsFactory(ABC):
    @abstractmethod
    def create(self) -> List[MergeFileGroup]:
        """
        Creates a list of merge file groups.

        Returns: a list of merge file groups.

        """
        raise NotImplementedError("Method not implemented")


class LocalMergeFileGroupsFactory(MergeFileGroupsFactory):
    """
    A factory class for producing merge file groups given local delta file envelopes.
    """

    def __init__(self, delta_file_envelopes: List[DeltaFileEnvelope]):
        self._dfe_groups = (
            [delta_file_envelopes] if len(delta_file_envelopes) > 0 else None
        )
        self.copy_by_reference_ids = []

    def create(self) -> List[MergeFileGroup]:
        if not self._dfe_groups:
            return []

        return [MergeFileGroup.of(self._dfe_groups)]


class RemoteMergeFileGroupsFactory(MergeFileGroupsFactory):
    """
    A factory class for producing merge file groups given delta file envelope object refs
        and hash bucketing parameters. Delta file envelopes are pulled from the object store
        remotely and loaded with in-memory pyarrow tables.
    """

    def __init__(
        self,
        hash_group_index: int,
        dfe_groups_refs: List[ObjectRef[DeltaFileEnvelopeGroups]],
        hash_bucket_count: int,
        num_hash_groups: int,
        object_store: IObjectStore,
    ):
        self.hash_group_index = hash_group_index
        self.hash_bucket_count = hash_bucket_count
        self.num_hash_groups = num_hash_groups
        self.object_store = object_store
        self.hb_index_to_delta_file_envelopes_list = None
        self.dfe_groups_list = []
        self._dfe_groups_refs = dfe_groups_refs
        self._hb_index_copy_by_reference_ids = []
        self._loaded_from_object_store = False

    def _load_deltas_from_object_store(self):
        delta_file_envelope_groups_list = self.object_store.get_many(
            self._dfe_groups_refs
        )
        hb_index_to_delta_file_envelopes_list = defaultdict(list)
        for delta_file_envelope_groups in delta_file_envelope_groups_list:
            assert self.hash_bucket_count == len(delta_file_envelope_groups), (
                f"The hash bucket count must match the dfe size as {self.hash_bucket_count}"
                f" != {len(delta_file_envelope_groups)}"
            )

            for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                if dfes:
                    hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)
        self.hb_index_to_delta_file_envelopes_list = (
            hb_index_to_delta_file_envelopes_list
        )

        valid_hb_indices_iterable = hash_group_index_to_hash_bucket_indices(
            self.hash_group_index, self.hash_bucket_count, self.num_hash_groups
        )

        dfe_list_groups = []
        hb_index_copy_by_reference = []
        for hb_idx in valid_hb_indices_iterable:
            dfe_list = hb_index_to_delta_file_envelopes_list.get(hb_idx)
            if dfe_list:
                dfe_list_groups.append(MergeFileGroup.of(dfe_list, hb_idx))
            else:
                hb_index_copy_by_reference.append(hb_idx)

        self.dfe_groups_list = dfe_list_groups
        self._hb_index_copy_by_reference_ids = hb_index_copy_by_reference
        self._loaded_from_object_store = True

    def create(self) -> List[MergeFileGroup]:
        if not self._loaded_from_object_store:
            self._load_deltas_from_object_store()

        return self.dfe_groups_list

    @property
    def hb_index_copy_by_reference_ids(self) -> List[int]:
        if not self._loaded_from_object_store:
            self._load_deltas_from_object_store()

        return self._hb_index_copy_by_reference_ids
