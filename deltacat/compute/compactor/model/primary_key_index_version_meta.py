from typing import Any, Dict


def of(
        primary_key_index_meta: Dict[str, Any],
        hash_bucket_count: int) -> Dict[str, Any]:
    """
    Creates Primary Key Index Version Metadata from the given Primary Key Index
    Metadata and hash bucket count.
    """
    return {
        "primaryKeyIndexMeta": primary_key_index_meta,
        "hashBucketCount": hash_bucket_count,
    }


def get_primary_key_index_meta(pkiv_meta: Dict[str, Any]) \
        -> Dict[str, Any]:
    return pkiv_meta["primaryKeyIndexMeta"]


def get_hash_bucket_count(pkiv_meta: Dict[str, Any]) -> int:
    return pkiv_meta["hashBucketCount"]
