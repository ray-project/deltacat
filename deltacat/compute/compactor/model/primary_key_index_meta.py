from typing import Any, Dict, Tuple, List


def of(
        compacted_partition_locator: Dict[str, Any],
        primary_keys: List[str],
        sort_keys: List[Tuple[str, str]],
        primary_key_index_algorithm_version: str) -> Dict[str, Any]:
    """
    Creates Primary Key Index Metadata from the given compacted
    Partition Locator, primary keys, sort keys, and primary key index
    algorithm version.
    """
    return {
        "compactedPartitionLocator": compacted_partition_locator,
        "primaryKeys": primary_keys,
        "sortKeys": sort_keys,
        "primaryKeyIndexAlgorithmVersion": primary_key_index_algorithm_version
    }


def get_compacted_partition_locator(pki_meta: Dict[str, Any]) \
        -> Dict[str, Any]:
    return pki_meta["compactedPartitionLocator"]


def get_primary_keys(pki_meta: Dict[str, Any]) -> List[str]:
    return pki_meta["primaryKeys"]


def get_sort_keys(pki_meta: Dict[str, Any]) -> List[Tuple[str, str]]:
    return pki_meta["sortKeys"]


def get_primary_key_index_algorithm_version(pki_meta: Dict[str, Any]) -> str:
    return pki_meta["primaryKeyIndexAlgorithmVersion"]
