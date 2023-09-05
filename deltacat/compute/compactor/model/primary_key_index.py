# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

from deltacat.storage.model.sort_key import SortKey
from deltacat.storage import Locator, PartitionLocator
from deltacat.utils.common import sha1_hexdigest


class PrimaryKeyIndexLocator(Locator, dict):
    @staticmethod
    def of(primary_key_index_meta: PrimaryKeyIndexMeta) -> PrimaryKeyIndexLocator:
        """
        Creates a Primary Key Index Locator from the given Primary Key
        Index Metadata. A Primary Key Index Locator consists of a Primary Key
        Index Root Path common to all versions. This Primary Key Index Root
        Path is generated deterministically from Primary Key Index Metadata.
        """
        pki_root_path = PrimaryKeyIndexLocator._root_path(
            primary_key_index_meta.compacted_partition_locator,
            primary_key_index_meta.primary_keys,
            primary_key_index_meta.sort_keys,
            primary_key_index_meta.primary_key_index_algorithm_version,
        )
        pkil = PrimaryKeyIndexLocator()
        pkil["primaryKeyIndexMeta"] = primary_key_index_meta
        pkil["primaryKeyIndexRootPath"] = pki_root_path
        return pkil

    @staticmethod
    def _root_path(
        compacted_partition_locator: PartitionLocator,
        primary_keys: List[str],
        sort_keys: List[SortKey],
        primary_key_index_algorithm_version: str,
    ) -> str:
        pl_hexdigest = compacted_partition_locator.hexdigest()
        pki_version_str = (
            f"{pl_hexdigest}|{primary_keys}|{sort_keys}|"
            f"{primary_key_index_algorithm_version}"
        )
        return sha1_hexdigest(pki_version_str.encode("utf-8"))

    @property
    def primary_key_index_meta(self) -> PrimaryKeyIndexMeta:
        val: Dict[str, Any] = self.get("primaryKeyIndexMeta")
        if val is not None and not isinstance(val, PrimaryKeyIndexMeta):
            self["primaryKeyIndexMeta"] = val = PrimaryKeyIndexMeta(val)
        return val

    @property
    def primary_key_index_root_path(self) -> str:
        """
        Gets the root path for all sibling versions of the given Primary Key
        Index Locator.
        """
        return self["primaryKeyIndexRootPath"]

    def get_primary_key_index_s3_url_base(self, s3_bucket: str) -> str:
        """
        Gets the base S3 URL for all sibling versions of the given Primary Key
        Index Locator.
        """
        pki_root_path = self.primary_key_index_root_path
        return f"s3://{s3_bucket}/{pki_root_path}"

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        return self.primary_key_index_root_path


class PrimaryKeyIndexMeta(dict):
    @staticmethod
    def of(
        compacted_partition_locator: PartitionLocator,
        primary_keys: List[str],
        sort_keys: List[SortKey],
        primary_key_index_algo_version: str,
    ) -> PrimaryKeyIndexMeta:
        """
        Creates Primary Key Index Metadata from the given compacted
        Partition Locator, primary keys, sort keys, and primary key index
        algorithm version.
        """
        pkim = PrimaryKeyIndexMeta()
        pkim["compactedPartitionLocator"] = compacted_partition_locator
        pkim["primaryKeys"] = primary_keys
        pkim["sortKeys"] = sort_keys
        pkim["primaryKeyIndexAlgorithmVersion"] = primary_key_index_algo_version
        return pkim

    @property
    def compacted_partition_locator(self) -> PartitionLocator:
        val: Dict[str, Any] = self.get("compactedPartitionLocator")
        if val is not None and not isinstance(val, PartitionLocator):
            self["compactedPartitionLocator"] = val = PartitionLocator(val)
        return val

    @property
    def primary_keys(self) -> List[str]:
        return self["primaryKeys"]

    @property
    def sort_keys(self) -> List[SortKey]:
        return self["sortKeys"]

    @property
    def primary_key_index_algorithm_version(self) -> str:
        return self["primaryKeyIndexAlgorithmVersion"]


class PrimaryKeyIndexVersionLocator(Locator, dict):
    @staticmethod
    def of(
        primary_key_index_version_meta: PrimaryKeyIndexVersionMeta,
        pki_version_root_path: str,
    ) -> PrimaryKeyIndexVersionLocator:
        """
        Creates a primary key index version locator from the given primary key
        index version metadata and version root path. Note that, while this is
        useful for constructing a locator pointing to an existing primary key
        index version, the `generate` function should be used to create unique
        locators for all new primary key index versions.
        """
        pkivl = PrimaryKeyIndexVersionLocator()
        pkivl["primaryKeyIndexVersionMeta"] = primary_key_index_version_meta
        pkivl["primaryKeyIndexVersionRootPath"] = pki_version_root_path
        return pkivl

    @staticmethod
    def generate(
        pki_version_meta: PrimaryKeyIndexVersionMeta,
    ) -> PrimaryKeyIndexVersionLocator:
        """
        Creates a new primary key index version locator from the given primary
        key index version metadata. A primary key index version locator
        consists of a primary key index root path common to all versions, and
        a primary key index version root path common to all hash buckets for
        that version. The primary key index version root path is generated
        non-deterministically from the number of hash buckets used for this
        version and a new UUID. The primary key index root path is generated
        deterministically from the compacted partition locator, primary keys,
        sort keys, and primary key index algorithm version.
        """
        pki_version_root_path = (
            PrimaryKeyIndexVersionLocator._generate_version_root_path(
                PrimaryKeyIndexVersionLocator._pki_root_path(pki_version_meta),
                pki_version_meta.hash_bucket_count,
            )
        )
        pkivl = PrimaryKeyIndexVersionLocator()
        pkivl["primaryKeyIndexVersionMeta"] = pki_version_meta
        pkivl["primaryKeyIndexVersionRootPath"] = pki_version_root_path
        return pkivl

    @staticmethod
    def _pki_root_path(pki_version_meta: PrimaryKeyIndexVersionMeta) -> str:
        pki_meta = pki_version_meta.primary_key_index_meta
        pki_locator = PrimaryKeyIndexLocator.of(pki_meta)
        return pki_locator.primary_key_index_root_path

    @staticmethod
    def _generate_version_root_path(pki_root_path: str, hash_bucket_count: int) -> str:
        return f"{pki_root_path}/{hash_bucket_count}/{str(uuid4())}"

    @property
    def primary_key_index_version_meta(self) -> PrimaryKeyIndexVersionMeta:
        val: Dict[str, Any] = self.get("primaryKeyIndexVersionMeta")
        if val is not None and not isinstance(val, PrimaryKeyIndexVersionMeta):
            self["primaryKeyIndexVersionMeta"] = val = PrimaryKeyIndexVersionMeta(val)
        return val

    @property
    def primary_key_index_root_path(self) -> str:
        """
        Gets the root path for all sibling versions of the given primary key
        index version locator.
        """
        return PrimaryKeyIndexVersionLocator._pki_root_path(
            self.primary_key_index_version_meta
        )

    @property
    def primary_key_index_version_root_path(self) -> str:
        """
        Gets the root path for the primary key index version associated with
        the given primary key index version locator.
        """
        return self["primaryKeyIndexVersionRootPath"]

    def get_primary_key_index_version_s3_url_base(self, s3_bucket: str) -> str:
        """
        Gets the base S3 URL for the primary key index version associated with
        the given primary key index version locator.
        """
        pkiv_root_path = self.primary_key_index_version_root_path
        return f"s3://{s3_bucket}/{pkiv_root_path}"

    def get_pkiv_hb_index_root_path(self, hb_index: int) -> str:
        """
        Gets the root path of a single hash bucket of the given primary key
        index version locator.
        """
        pkiv_root_path = self.primary_key_index_version_root_path
        return f"{pkiv_root_path}/{hb_index}"

    def get_pkiv_hb_index_s3_url_base(
        self, s3_bucket: str, hash_bucket_index: int
    ) -> str:
        """
        Gets the base S3 URL of a single hash bucket of the given primary key
        index version locator.
        """
        hbi_root_path = self.get_pkiv_hb_index_root_path(hash_bucket_index)
        return f"s3://{s3_bucket}/{hbi_root_path}"

    def get_pkiv_hb_index_manifest_s3_url(
        self, s3_bucket: str, hash_bucket_index: int
    ) -> str:
        """
        Gets the S3 URL of the manifest for a single primary key index version
        hash bucket.
        """
        pkiv_hb_index_s3_url_base = self.get_pkiv_hb_index_s3_url_base(
            s3_bucket,
            hash_bucket_index,
        )
        return f"{pkiv_hb_index_s3_url_base}.json"

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        return self.primary_key_index_version_root_path


class PrimaryKeyIndexVersionMeta(dict):
    @staticmethod
    def of(
        primary_key_index_meta: PrimaryKeyIndexMeta, hash_bucket_count: int
    ) -> PrimaryKeyIndexVersionMeta:
        """
        Creates Primary Key Index Version Metadata from the given Primary Key
        Index Metadata and hash bucket count.
        """
        pkivm = PrimaryKeyIndexVersionMeta()
        pkivm["primaryKeyIndexMeta"] = primary_key_index_meta
        pkivm["hashBucketCount"] = hash_bucket_count
        return pkivm

    @property
    def primary_key_index_meta(self) -> PrimaryKeyIndexMeta:
        val: Dict[str, Any] = self.get("primaryKeyIndexMeta")
        if val is not None and not isinstance(val, PrimaryKeyIndexMeta):
            self["primaryKeyIndexMeta"] = val = PrimaryKeyIndexMeta(val)
        return val

    @property
    def hash_bucket_count(self) -> int:
        return self["hashBucketCount"]
