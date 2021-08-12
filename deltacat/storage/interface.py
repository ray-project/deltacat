import pyarrow as pa
import pandas as pd
import numpy as np
from deltacat.types.media import ContentType
from deltacat.storage.model.types import DeltaType, LifecycleState
from deltacat.types.media import TableType
from typing import Any, Dict, List, Optional, Union


def list_namespaces(
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    raise NotImplementedError("list_namespaces not implemented")


def list_tables(
        namespace: str,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists a page of tables for the given table namespace. Tables are returned as
    list result items.
    """
    raise NotImplementedError("list_tables not implemented")


def list_table_versions(
        namespace: str,
        table_name: str,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists a page of table versions for the given table. Table versions are
    returned as list result items.
    """
    raise NotImplementedError("list_table_versions not implemented")


def list_partitions(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists a page of partitions for the given table version. Partitions are
    returned as list result items. Table version resolves to the latest active
    table version if not specified.
    """
    raise NotImplementedError("list_partitions not implemented")


def list_partitions_pending_commit(
        partition_staging_area: Dict[str, Any],
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists all partitions that will be included in a commit of the current
    partition staging area to a new stream (i.e. all partitions currently
    committed to the partition staging area).
    """
    raise NotImplementedError("list_partitions_pending_commit not implemented")


def list_deltas(
        namespace: str,
        table_name: str,
        partition_values: Optional[List[Any]] = None,
        table_version: Optional[str] = None,
        first_stream_position: Optional[int] = None,
        last_stream_position: Optional[int] = None,
        ascending_order: Optional[bool] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists a page of deltas for the given table version and partition. Deltas are
    returned as list result items. Deltas returned can optionally be limited to
    inclusive first and last stream positions. Deltas are returned by
    descending stream position by default. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables.
    """
    raise NotImplementedError("list_deltas not implemented")


def list_deltas_pending_commit(
        delta_staging_area: Dict[str, Any],
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Lists all deltas that will be included in a commit of the current delta
    staging area to a new partition (i.e. all deltas currently committed to the
    delta staging area).
    """
    raise NotImplementedError("list_deltas_pending_commit not implemented")


def latest_delta(
        namespace: str,
        table_name: str,
        partition_values: Optional[List[Any]],
        table_version: Optional[str] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Gets the latest delta (i.e. the delta with the greatest stream position) for
    the given table version and partition. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables.
    """
    raise NotImplementedError("latest_delta not implemented")


def download_delta(
        delta: Dict[str, Any],
        table_type: TableType = TableType.PYARROW,
        max_parallelism: int = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs) -> List[Union[pa.Table, pd.DataFrame, np.ndarray]]:
    """
    Download a delta into 1 or more instances of the specified table type.
    """
    raise NotImplementedError("download_delta not implemented")


def get_manifest(
        delta_locator: Dict[str, Any],
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Get the manifest associated for the given delta locator.
    """
    raise NotImplementedError("get_manifest not implemented")


def download_manifest_entry(
        delta_locator: Dict[str, Any],
        manifest: Dict[str, Any],
        entry_index: int,
        table_type: TableType = TableType.PYARROW,
        file_reader_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs) -> Union[pa.Table, pd.DataFrame, np.ndarray]:
    """
    Downloads a single manifest entry into the specified table type.
    """
    raise NotImplementedError("download_manifest_entry not implemented")


def get_delta_manifest(
        delta: Dict[str, Any],
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Get the delta manifest associated with this delta.
    """
    raise NotImplementedError("get_delta_manifest not implemented")


def download_delta_manifest(
        delta_manifest: Dict[str, Any],
        table_type: TableType = TableType.PYARROW,
        max_parallelism: int = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs) -> List[Union[pa.Table, pd.DataFrame, np.ndarray]]:
    """
    Download a delta manifest to 1 or more instances of the specified table
    type. While the main purpose of this function is to download staged deltas,
    it can also be used to download delta manifests previously retrieved from
    committed deltas, or constructed by merging/splitting multiple delta
    manifests.
    """
    raise NotImplementedError("download_delta_manifest not implemented")


def create_namespace(
        namespace: str,
        permissions: Dict[str, Any],
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Creates a table namespace with the given name and permissions.
    """
    raise NotImplementedError("create_namespace not implemented")


def update_namespace(
        namespace: str,
        permissions: Optional[Dict[str, Any]] = None,
        new_namespace: Optional[str] = None,
        *args,
        **kwargs):
    """
    Updates a table namespace's name and/or permissions.
    """
    raise NotImplementedError("update_namespace not implemented")


def create_table_version(
        namespace: str,
        table_name: str,
        schema: Optional[Union[pa.Schema, str, bytes]] = None,
        partition_keys: Optional[List[Dict[str, Any]]] = None,
        primary_key_column_names: Optional[List[str]] = None,
        table_version_description: Optional[str] = None,
        table_version_properties: Optional[Dict[str, str]] = None,
        table_permissions: Optional[Dict[str, Any]] = None,
        table_description: Optional[str] = None,
        table_properties: Optional[Dict[str, str]] = None,
        supported_content_types: Optional[List[ContentType]] = None,
        *args,
        **kwargs):
    """
    Create a table version with an unreleased lifecycle state and an empty delta
    stream. Table versions may be schemaless and unpartitioned, or partitioned
    according to a list of partition key names and types. Note that partition
    keys are not required to exist in the table's schema, and can thus still be
    used with schemaless tables. This can be useful for creating logical shards
    of a delta stream where partition keys are known but not projected onto each
    row of the table (e.g. all rows of a customer orders table are known to
    correspond to a given order day, even if this column doesn't exist in the
    table). Primary keys must exist within the table's schema. Permissions
    specified at the table level override any conflicting permissions specified
    at the table namespace level.
    """
    raise NotImplementedError("create_table_version not implemented")


def update_table(
        namespace: str,
        table_name: str,
        permissions: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        new_table_name: Optional[str] = None):
    """
    Update table metadata describing the table versions it contains. By default,
    a table's properties are empty, and its description and permissions are
    equal to those given when its first table version was created.
    """
    raise NotImplementedError("update_table not implemented")


def update_table_version(
        namespace: str,
        table_name: str,
        table_version: str,
        lifecycle_state: Optional[LifecycleState] = None,
        schema: Optional[str] = None,
        partition_keys: Optional[List[Dict[str, Any]]] = None,
        primary_key_column_names: Optional[List[str]] = None,
        description: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        *args,
        **kwargs):
    """
    Update a table version. Notably, updating an unreleased table version's
    lifecycle state to 'active' telegraphs that it is ready for external
    consumption, and causes all calls made to consume/produce streams,
    partitions, or deltas from/to its parent table to automatically resolve to
    this table version by default (i.e. when the client does not explicitly
    specify a different table version).
    """
    raise NotImplementedError("update_table_version not implemented")


def stage_stream(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Stages a new stream for the given table version. Resolves to the latest
    active table version if no table version is given. Returns the partition
    staging area for the staged stream.
    """
    raise NotImplementedError("stage_stream not implemented")


def commit_stream(
        partition_staging_area,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Registers a stream with a target table version, replacing any previous
    stream registered for the same table version. Returns stream metadata
    describing the registered stream.
    """
    raise NotImplementedError("commit_stream not implemented")


def delete_stream(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        *args,
        **kwargs):
    """
    Deletes the stream currently registered with the given table version.
    Resolves to the latest active table version if no table version is given.
    """
    raise NotImplementedError("delete_stream not implemented")


def get_partition_staging_area(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets the partition staging area for the most recently committed stream of
    the given table version and partition key values. Resolves to the latest
    active table version if no table version is given. Returns None if no
    stream has been committed for the given table version and partition key
    values.
    """
    raise NotImplementedError("get_partition_staging_area not implemented")


def stage_partition(
        partition_staging_area: Dict[str, Any],
        partition_values: Optional[List[Any]] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Stages a new partition for the given partition staging area and
    key values. Returns the delta staging area for the staged partition.
    Partition keys should not be specified for unpartitioned tables.
    """
    raise NotImplementedError("stage_partition not implemented")


def commit_partition(
        delta_staging_area: Dict[str, Any],
        previous_stream_position: Optional[int] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Registers the given delta staging area as a new partition within its
    associated table version stream, replacing any previous partition registered
    for the same table version stream and partition keys. Returns the registered
    partition. If previous stream position is specified, then the commit will be
    rejected if it does not match the actual previous stream position of the
    partition being replaced.
    """
    raise NotImplementedError("commit_partition not implemented")


def delete_partition(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        partition_values: Optional[List[Any]] = None,
        *args,
        **kwargs):
    """
    Deletes the given partition from the specified table version. Resolves to
    the latest active table version if no table version is given. Partition
    values should not be specified for unpartitioned tables.
    """
    raise NotImplementedError("delete_partition not implemented")


def get_delta_staging_area(
        partition_staging_area: Dict[str, Any],
        partition_values: Optional[List[Any]] = None,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets the delta staging area for the most recently committed partition of the
    given table version and partition key values. Returns None if no partition
    has been committed for the given table version and/or partition key values.
    Partition should not be specified for unpartitioned tables.
    """
    raise NotImplementedError("get_delta_staging_area not implemented")


def stage_delta(
        table_to_stage: Union[pa.Table, pd.DataFrame, np.ndarray],
        delta_staging_area: Dict[str, Any],
        delta_type: DeltaType = DeltaType.UPSERT,
        max_records_per_entry: Optional[int] = None,
        author: Optional[Dict[str, Any]] = None,
        s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
        content_type: ContentType = ContentType.PARQUET,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Writes the given table to 1 or more S3 files. Returns an unregistered
    delta manifest whose entries point to the uploaded files.
    """
    raise NotImplementedError("stage_delta not implemented")


def commit_delta(
        delta_manifest: Dict[str, Any],
        properties: Optional[Dict[str, str]] = None,
        stream_position: Optional[int] = None,
        previous_stream_position: Optional[int] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """
    Registers a delta manifest as a new delta with its associated target table
    version and partition. Returns the registered delta. If previous stream
    position is specified, then the commit will be rejected if it does not match
    the target partition's actual previous stream position.
    """
    raise NotImplementedError("commit_delta not implemented")


def get_namespace(
        namespace: str,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets table namespace metadata for the specified table namespace.
    """
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(
        namespace: str,
        *args,
        **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    raise NotImplementedError("namespace_exists not implemented")


def get_table(
        namespace: str,
        table_name: str,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets table metadata for the specified table.
    """
    raise NotImplementedError("get_table not implemented")


def table_exists(
        namespace: str,
        table_name: str,
        *args,
        **kwargs) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    raise NotImplementedError("table_exists not implemented")


def get_table_version(
        namespace: str,
        table_name: str,
        table_version: str,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets table version metadata for the specified table version.
    """
    raise NotImplementedError("get_table_version not implemented")


def get_latest_table_version(
        namespace: str,
        table_name: str,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets table version metadata for the latest version of the specified table.
    """
    raise NotImplementedError("get_latest_table_version not implemented")


def get_latest_active_table_version(
        namespace: str,
        table_name: str,
        *args,
        **kwargs) -> Optional[Dict[str, Any]]:
    """
    Gets table version metadata for the latest active version of the specified
    table.
    """
    raise NotImplementedError("get_latest_active_table_version not implemented")


def get_table_version_column_names(
        namespace: str,
        table_name: str,
        table_version: Optional[str] = None,
        *args,
        **kwargs) -> Optional[List[str]]:
    """
    Gets a list of column names for the specified table version, or for the
    latest active table version if none is specified. The index of each
    column name returned represents its ordinal position in a delimited text
    file or other row-oriented content type files appended to the table.
    """
    raise NotImplementedError("get_table_version_column_names not implemented")


def table_version_exists(
        namespace: str,
        table_name: str,
        table_version: str,
        *args,
        **kwargs) -> bool:
    """
    Returns True if the given table version exists, False if not.
    """
    raise NotImplementedError("table_version_exists not implemented")
