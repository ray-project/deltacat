from deltacat.compute.compactor.model import \
    primary_key_index_version_meta as pkivm, primary_key_index_locator as pkil
from deltacat.utils.common import sha1_hexdigest
from typing import Any, Dict
from uuid import uuid4


def of(
        primary_key_index_version_meta: Dict[str, Any],
        primary_key_index_version_root_path: str) -> Dict[str, Any]:
    """
    Creates a primary key index version locator from the given primary key
    index version metadata and version root path. Note that, while this is
    useful for constructing a locator pointing to an existing primary key index
    version, the `generate` function should be used to create unique locators
    for all new primary key index versions.
    """
    return {
        "primaryKeyIndexVersionMeta": primary_key_index_version_meta,
        "primaryKeyIndexVersionRootPath": primary_key_index_version_root_path,
    }


def generate(pki_version_meta: Dict[str, Any]) \
        -> Dict[str, Any]:
    """
    Creates a new primary key index version locator from the given primary key
    index version metadata. A primary key index version locator consists of a
    primary key index root path common to all versions, and a primary key index
    version root path common to all hash buckets for that version. The primary
    key index version root path is generated non-deterministically from the
    number of hash buckets used for this version and a new UUID. The primary
    key index root path is generated deterministically from the compacted
    partition locator, primary keys, sort keys, and primary key index
    algorithm version.
    """
    pki_version_root_path = _generate_version_root_path(
        _pki_root_path(pki_version_meta),
        pkivm.get_hash_bucket_count(pki_version_meta),
    )
    return {
        "primaryKeyIndexVersionMeta": pki_version_meta,
        "primaryKeyIndexVersionRootPath": pki_version_root_path,
    }


def _pki_root_path(pki_version_meta: Dict[str, Any]):
    pki_meta = pkivm.get_primary_key_index_meta(pki_version_meta)
    pki_locator = pkil.of(pki_meta)
    return pkil.get_primary_key_index_root_path(pki_locator)


def _generate_version_root_path(
        pki_root_path: str,
        hash_bucket_count: int) -> str:

    return f"{pki_root_path}/{hash_bucket_count}/{str(uuid4())}"


def get_primary_key_index_version_meta(pkiv_locator: Dict[str, Any]) \
        -> Dict[str, Any]:
    return pkiv_locator["primaryKeyIndexVersionMeta"]


def get_primary_key_index_root_path(pkiv_locator: Dict[str, Any]) -> str:
    """
    Gets the root path for all sibling versions of the given primary key index
    version locator.
    """
    return _pki_root_path(get_primary_key_index_version_meta(pkiv_locator))


def get_primary_key_index_version_root_path(pkiv_locator: Dict[str, Any]) \
        -> str:
    """
    Gets the root path for the primary key index version associated with the
    given primary key index version locator.
    """
    return pkiv_locator["primaryKeyIndexVersionRootPath"]


def get_primary_key_index_version_s3_url_base(
        pkiv_locator: Dict[str, Any],
        s3_bucket: str) -> str:
    """
    Gets the base S3 URL for the primary key index version associated with the
    given primary key index version locator.
    """
    pkiv_root_path = get_primary_key_index_version_root_path(pkiv_locator)
    return f"s3://{s3_bucket}/{pkiv_root_path}"


def get_pkiv_hb_index_root_path(
        pkiv_locator: Dict[str, Any],
        hb_index: int) -> str:
    """
    Gets the root path of a single hash bucket of the given primary key index
    version locator.
    """
    pkiv_root_path = get_primary_key_index_version_root_path(pkiv_locator)
    return f"{pkiv_root_path}/{hb_index}"


def get_pkiv_hb_index_s3_url_base(
        pkiv_locator: Dict[str, Any],
        s3_bucket: str,
        hash_bucket_index: int) -> str:
    """
    Gets the base S3 URL of a single hash bucket of the given primary key index
    version locator.
    """
    hbi_root_path = get_pkiv_hb_index_root_path(pkiv_locator, hash_bucket_index)
    return f"s3://{s3_bucket}/{hbi_root_path}"


def get_pkiv_hb_index_manifest_s3_url(
        pkiv_locator: Dict[str, Any],
        s3_bucket: str,
        hash_bucket_index: int) -> str:
    """
    Gets the S3 URL of the manifest for a single primary key index version hash
    bucket.
    """
    pkiv_hb_index_s3_url_base = get_pkiv_hb_index_s3_url_base(
        pkiv_locator,
        s3_bucket,
        hash_bucket_index,
    )
    return f"{pkiv_hb_index_s3_url_base}.json"


def hexdigest(pkiv_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given primary key index version locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    pkiv_root_path = get_primary_key_index_version_root_path(pkiv_locator)
    return sha1_hexdigest(pkiv_root_path.encode("utf-8"))
