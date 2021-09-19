from deltacat.compute.compactor.model import primary_key_index_meta as pkim
from deltacat.storage.model import partition_locator as pl
from deltacat.utils.common import sha1_digest, sha1_hexdigest
from typing import Any, Dict, List, Tuple


def of(primary_key_index_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a Primary Key Index Locator from the given Primary Key
    Index Metadata. A Primary Key Index Locator consists of a Primary Key Index
    Root Path common to all versions. This Primary Key Index Root Path is
    generated deterministically from Primary Key Index Metadata.
    """
    pki_root_path = _root_path(
        pkim.get_compacted_partition_locator(primary_key_index_meta),
        pkim.get_primary_keys(primary_key_index_meta),
        pkim.get_sort_keys(primary_key_index_meta),
        pkim.get_primary_key_index_algorithm_version(primary_key_index_meta),
    )
    return {
        "primaryKeyIndexMeta": primary_key_index_meta,
        "primaryKeyIndexRootPath": pki_root_path,
    }


def _root_path(
        compacted_partition_locator: Dict[str, Any],
        primary_keys: List[str],
        sort_keys: List[Tuple[str, str]],
        primary_key_index_algorithm_version: str) -> str:

    pl_hexdigest = pl.hexdigest(compacted_partition_locator)
    pki_version_str = f"{pl_hexdigest}|{primary_keys}|{sort_keys}|" \
                      f"{primary_key_index_algorithm_version}"
    return sha1_hexdigest(pki_version_str.encode("utf-8"))


def get_primary_key_index_meta(pki_locator: Dict[str, Any]) -> Dict[str, Any]:
    return pki_locator["primaryKeyIndexMeta"]


def get_primary_key_index_root_path(pki_locator: Dict[str, Any]) -> str:
    """
    Gets the root path for all sibling versions of the given Primary Key Index
    Locator.
    """
    return pki_locator["primaryKeyIndexRootPath"]


def get_primary_key_index_s3_url_base(
        pki_locator: Dict[str, Any],
        s3_bucket: str) -> str:
    """
    Gets the base S3 URL for all sibling versions of the given Primary Key
    Index Locator.
    """
    pki_root_path = get_primary_key_index_root_path(pki_locator)
    return f"s3://{s3_bucket}/{pki_root_path}"


def canonical_string(pki_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    return get_primary_key_index_root_path(pki_locator)


def digest(pki_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(pki_locator).encode("utf-8"))


def hexdigest(pki_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(pki_locator).encode("utf-8"))
