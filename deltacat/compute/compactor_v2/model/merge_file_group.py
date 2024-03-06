# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict

from deltacat.utils.common import ReadKwargsProvider
from ray.types import ObjectRef

from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor_v2.utils.delta import read_delta_file_envelopes

from deltacat.compute.compactor_v2.utils.primary_key_index import (
    hash_group_index_to_hash_bucket_indices,
)

from deltacat.storage import interface as unimplemented_deltacat_storage

from deltacat.io.object_store import IObjectStore

from deltacat import logs

from deltacat.compute.compactor import DeltaFileEnvelope, DeltaAnnotated

from typing import List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MergeFileGroup(dict):
    @staticmethod
    def of(hb_index: int, dfe_groups: Optional[List[List[DeltaFileEnvelope]]] = None):
        """
        Creates a container with delta file envelope groupings and other
        additional properties used primarily for the merging step.

        Args:
            hb_index: This signifies the hash bucket index corresponding to the envelope delta file groups.
            dfe_groups: A list of delta file envelope groups.
                If not present, the provided hash bucket index is a copy by reference candidate during the merge step.

        Returns:
            A dict

        """
        d = MergeFileGroup()
        d["hb_index"] = hb_index
        d["dfe_groups"] = dfe_groups
        return d

    @property
    def dfe_groups(self) -> Optional[List[List[DeltaFileEnvelope]]]:
        return self["dfe_groups"]

    @property
    def hb_index(self) -> int:
        return self["hb_index"]


class MergeFileGroupsProvider(ABC):
    @abstractmethod
    def create(self) -> List[MergeFileGroup]:
        """
        Creates a list of merge file groups.

        Returns: a list of merge file groups.

        """
        raise NotImplementedError("Method not implemented")

    @property
    @abstractmethod
    def hash_group_index(self):
        raise NotImplementedError("Method not implemented")


class LocalMergeFileGroupsProvider(MergeFileGroupsProvider):
    """
    A factory class for producing merge file groups given local delta file envelopes.
    """

    LOCAL_HASH_BUCKET_INDEX = 0
    LOCAL_HASH_GROUP_INDEX = 0

    def __init__(
        self,
        uniform_deltas: List[DeltaAnnotated],
        read_kwargs_provider: Optional[ReadKwargsProvider],
        deltacat_storage=unimplemented_deltacat_storage,
        deltacat_storage_kwargs: Optional[dict] = None,
    ):
        self._deltas = uniform_deltas
        self._read_kwargs_provider = read_kwargs_provider
        self._deltacat_storage = deltacat_storage
        self._deltacat_storage_kwargs = deltacat_storage_kwargs
        self._loaded_deltas = False

    def _read_deltas_locally(self):
        local_dfe_list = []
        input_records_count = 0
        uniform_deltas = self._deltas
        logger.info(f"Getting {len(uniform_deltas)} DFE Tasks.")
        dfe_start = time.monotonic()
        for annotated_delta in uniform_deltas:
            (
                delta_file_envelopes,
                total_record_count,
                total_size_bytes,
            ) = read_delta_file_envelopes(
                annotated_delta,
                self._read_kwargs_provider,
                self._deltacat_storage,
                self._deltacat_storage_kwargs,
            )
            if delta_file_envelopes:
                local_dfe_list.extend(delta_file_envelopes)
                input_records_count += total_record_count
        dfe_end = time.monotonic()
        logger.info(
            f"Retrieved {len(local_dfe_list)} DFE Tasks in {dfe_end - dfe_start}s."
        )

        self._dfe_groups = [local_dfe_list] if len(local_dfe_list) > 0 else None
        self._loaded_deltas = True

    def create(self) -> List[MergeFileGroup]:
        if not self._loaded_deltas:
            self._read_deltas_locally()

        # Since hash bucketing is skipped for local merges, we use a fixed index here.
        return [
            MergeFileGroup.of(
                hb_index=LocalMergeFileGroupsProvider.LOCAL_HASH_BUCKET_INDEX,
                dfe_groups=self._dfe_groups,
            )
        ]

    @property
    def hash_group_index(self):
        return LocalMergeFileGroupsProvider.LOCAL_HASH_GROUP_INDEX


class RemoteMergeFileGroupsProvider(MergeFileGroupsProvider):
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
        self.hash_bucket_count = hash_bucket_count
        self.num_hash_groups = num_hash_groups
        self.object_store = object_store
        self._hash_group_index = hash_group_index
        self._dfe_groups_refs = dfe_groups_refs
        self._dfe_groups = []
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
        valid_hb_indices_iterable = hash_group_index_to_hash_bucket_indices(
            self.hash_group_index, self.hash_bucket_count, self.num_hash_groups
        )

        total_dfes_found = 0
        dfe_list_groups = []
        for hb_idx in valid_hb_indices_iterable:
            dfe_list = hb_index_to_delta_file_envelopes_list.get(hb_idx)
            if dfe_list:
                total_dfes_found += 1
                dfe_list_groups.append(
                    MergeFileGroup.of(hb_index=hb_idx, dfe_groups=dfe_list)
                )
            else:
                dfe_list_groups.append(MergeFileGroup.of(hb_index=hb_idx))

        assert total_dfes_found == len(hb_index_to_delta_file_envelopes_list), (
            "The total dfe list does not match the input dfes from hash bucket as "
            f"{total_dfes_found} != {len(hb_index_to_delta_file_envelopes_list)}"
        )
        self._dfe_groups = dfe_list_groups
        self._loaded_from_object_store = True

    def create(self) -> List[MergeFileGroup]:
        if not self._loaded_from_object_store:
            self._load_deltas_from_object_store()

        return self._dfe_groups

    @property
    def hash_group_index(self):
        return self._hash_group_index
