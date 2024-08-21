from typing import Any, Callable, Dict, List, Optional, Set, Union

import pyarrow as pa

from deltacat.storage import (
    DeleteParameters,
    Delta,
    DeltaLocator,
    DeltaType,
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Manifest,
    ManifestAuthor,
    Namespace,
    Partition,
    SchemaConsistencyType,
    Stream,
    StreamLocator,
    Table,
    TableVersion,
    SortKey,
    PartitionLocator,
    PartitionFilter,
    PartitionValues,
    DeltaPartitionSpec,
    StreamPartitionSpec,
    NamespaceLocator,
    TableLocator,
    TableVersionLocator,
    SortOrder,
)
from deltacat.types.media import (
    ContentType,
    StorageType,
    TableType,
    DistributedDatasetType,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.aws.clients import client_cache
from deltacat.storage.glue.schema import glue_columns_to_arrow_schema
from deltacat.storage.glue.exceptions import EntityNotFound


def _get_client_from_kwargs(**kwargs):
    glue = client_cache("glue", kwargs.get("region", "us-east-1"))
    lf = client_cache("lakeformation", kwargs.get("region", "us-east-1"))
    return glue, lf


def _extract_pagination_params(**kwargs):
    result = {}
    if "NextToken" in kwargs and kwargs.get("NextToken") is not None:
        result["NextToken"] = kwargs["NextToken"]
    if "MaxResults" in kwargs and kwargs.get("MaxResults") is not None:
        result["MaxResults"] = kwargs["MaxResults"]
    return result


def _merge_dict(dict1, dict2):
    result = {}
    for key, value in dict1.items():
        if value is not None:
            result[key] = value
    for key, value in dict2.items():
        if value is not None:
            result[key] = value
    return result


def _build_namespace_locator_from_glue(namespace: str) -> NamespaceLocator:
    return NamespaceLocator.of(namespace)


def _build_table_from_glue(
    table: Dict[Any, Any], lf_permissions: Dict[Any, Any]
) -> Table:
    namespace_locator = _build_namespace_locator_from_glue(table["DatabaseName"])
    table_locator = TableLocator.of(namespace_locator, table["Name"])
    return Table.of(
        table_locator,
        permissions=lf_permissions["PrincipalResourcePermissions"],
        description=table.get("Description"),
    )


def _build_table_version_from_glue(
    glue_tv: Dict[Any, Any], lf_permissions: Dict[Any, Any]
) -> TableVersion:
    table = _build_table_from_glue(glue_tv["Table"], lf_permissions=lf_permissions)
    glue_table = glue_tv["Table"]
    tv_locator = TableVersionLocator.of(table.locator, glue_tv["VersionId"])
    schema = None
    if glue_table.get("StorageDescriptor") and glue_table["StorageDescriptor"].get(
        "Columns"
    ):
        schema = glue_columns_to_arrow_schema(
            glue_table["StorageDescriptor"]["Columns"]
        )

    partition_keys = []
    if glue_table.get("PartitionKeys"):
        for partition_key in glue_table["PartitionKeys"]:
            partition_keys.append(partition_key["Name"])

    sort_keys = []
    sort_order_lookup = {1: SortOrder.ASCENDING, 0: SortOrder.DESCENDING}
    for sort_key in glue_table.get("StorageDescriptor", {}).get("SortColumns", []):
        sort_keys.append(
            SortKey.of(
                sort_key["Column"],
                sort_order_lookup.get(sort_key["SortOrder"], SortOrder.ASCENDING),
            )
        )

    # TODO: Add content types
    return TableVersion.of(
        tv_locator,
        schema=schema,
        partition_keys=partition_keys,
        description=glue_table.get("Description"),
        content_types=[ContentType.PARQUET],
        sort_keys=sort_keys,
    )


def _build_namespace_from_glue(
    namespace: str, lf_permissions: Dict[Any, Any]
) -> Namespace:
    locator = _build_namespace_locator_from_glue(namespace)
    return Namespace.of(
        locator, permissions=lf_permissions.get("PrincipalResourcePermissions", {})
    )


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of glue databasesas as list result items.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)
    params = _extract_pagination_params(**kwargs)

    response = glue.get_databases(**params)
    if "DatabaseList" not in response:
        return ListResult.of([])

    result = []
    for db in response["DatabaseList"]:
        namespace = get_namespace(db["Name"])
        result.append(namespace)

    return ListResult.of(
        result,
        response.get("NextToken"),
        lambda token: list_namespaces(
            NextToken=token, MaxResults=params.get("MaxResults")
        ),
    )


def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    """
    Lists all the tables in a namespace.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)
    params = {"DatabaseName": namespace}
    pagination_params = _extract_pagination_params(**kwargs)
    params = _merge_dict(params, pagination_params)

    response = glue.get_tables(**params)

    if "TableList" not in response:
        return ListResult.of([])

    result = []
    for table in response["TableList"]:
        table_result = get_table(namespace, table["Name"])
        result.append(table_result)

    return ListResult.of(
        result,
        response.get("NextToken"),
        lambda token: list_tables(NextToken=token, MaxResults=params.get("MaxResults")),
    )


def list_table_versions(
    namespace: str, table_name: str, *args, **kwargs
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table. Table versions are
    returned as list result items. Raises an error if the given table does not
    exist.
    """
    if table_exists(namespace, table_name, *args, **kwargs):
        glue, lf = _get_client_from_kwargs(**kwargs)
        params = {"DatabaseName": namespace, "TableName": table_name}
        pagination_params = _extract_pagination_params(**kwargs)
        params = _merge_dict(params, pagination_params)
        response = glue.get_table_versions(**params)

        lf_permissions = lf.list_permissions(
            ResourceType="TABLE",
            Resource={"Table": {"Name": table_name, "DatabaseName": namespace}},
        )

        result = []
        for glue_tv in response.get("TableVersions", []):
            tv = _build_table_version_from_glue(glue_tv, lf_permissions=lf_permissions)
            result.append(tv)

        if not result:
            return ListResult.of([])

        return ListResult.of(
            result,
            response.get("NextToken"),
            lambda token: list_tables(
                NextToken=token, MaxResults=params.get("MaxResults")
            ),
        )
    else:
        raise EntityNotFound(f"Table {namespace}.{table_name} does not exist")


def list_partitions(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> ListResult[Partition]:
    """
    Lists a page of partitions for the given table version. Partitions are
    returned as list result items. Table version resolves to the latest active
    table version if not specified. Raises an error if the table version does
    not exist.
    """
    raise NotImplementedError("list_partitions not implemented")


def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    """
    Lists all partitions committed to the given stream.
    """
    raise NotImplementedError("list_stream_partitions not implemented")


def list_deltas(
    namespace: str,
    table_name: str,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: Optional[bool] = None,
    include_manifest: bool = False,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> ListResult[Delta]:
    """
    Lists a page of deltas for the given table version and committed partition.
    Deltas are returned as list result items. Deltas returned can optionally be
    limited to inclusive first and last stream positions. Deltas are returned by
    descending stream position by default. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Raises an error if the given table
    version or partition does not exist.

    To conserve memory, the deltas returned do not include manifests by
    default. The manifests can either be optionally retrieved as part of this
    call or lazily loaded via subsequent calls to `get_delta_manifest`.

    Note: partition_values is deprecated and will be removed in future releases.
    Use partition_filter instead.
    """
    raise NotImplementedError("list_deltas not implemented")


def list_partition_deltas(
    partition_like: Union[Partition, PartitionLocator],
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: bool = False,
    include_manifest: bool = False,
    *args,
    **kwargs,
) -> ListResult[Delta]:
    """
    Lists a page of deltas committed to the given partition.

    To conserve memory, the deltas returned do not include manifests by
    default. The manifests can either be optionally retrieved as part of this
    call or lazily loaded via subsequent calls to `get_delta_manifest`.
    """
    raise NotImplementedError("list_partition_deltas not implemented")


def get_delta(
    namespace: str,
    table_name: str,
    stream_position: int,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the delta for the given table version, partition, and stream position.
    Table version resolves to the latest active table version if not specified.
    Partition values should not be specified for unpartitioned tables. Raises
    an error if the given table version or partition does not exist.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.

    Note: partition_values is deprecated and will be removed in future releases.
    Use partition_filter instead.
    """
    raise NotImplementedError("get_delta not implemented")


def get_latest_delta(
    namespace: str,
    table_name: str,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the latest delta (i.e. the delta with the greatest stream position) for
    the given table version and partition. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Raises an error if the given table
    version or partition does not exist.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.

    Note: partition_values is deprecated and will be removed in future releases.
    Use partition_filter instead.
    """
    raise NotImplementedError("get_latest_delta not implemented")


def download_delta(
    delta_like: Union[Delta, DeltaLocator],
    table_type: TableType = TableType.PYARROW,
    storage_type: StorageType = StorageType.DISTRIBUTED,
    max_parallelism: Optional[int] = None,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: DistributedDatasetType = DistributedDatasetType.RAY_DATASET,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Union[LocalDataset, DistributedDataset]:  # type: ignore
    """
    Download the given delta or delta locator into either a list of
    tables resident in the local node's memory, or into a dataset distributed
    across this Ray cluster's object store memory. Ordered table N of a local
    table list, or ordered block N of a distributed dataset, always contain
    the contents of ordered delta manifest entry N.

    partition_filter is an optional parameter which determines which files to
    download from the delta manifest. A delta manifest contains all the data files
    for a given delta.
    """
    raise NotImplementedError("download_delta not implemented")


def download_delta_manifest_entry(
    delta_like: Union[Delta, DeltaLocator],
    entry_index: int,
    table_type: TableType = TableType.PYARROW,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    **kwargs,
) -> LocalTable:
    """
    Downloads a single manifest entry into the specified table type for the
    given delta or delta locator. If a delta is provided with a non-empty
    manifest, then the entry is downloaded from this manifest. Otherwise, the
    manifest is first retrieved then the given entry index downloaded.

    NOTE: The entry will be downloaded in the current node's memory.
    """
    raise NotImplementedError("download_delta_manifest_entry not implemented")


def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator], *args, **kwargs
) -> Manifest:
    """
    Get the manifest associated with the given delta or delta locator. This
    always retrieves the authoritative remote copy of the delta manifest, and
    never the local manifest defined for any input delta.
    """
    raise NotImplementedError("get_delta_manifest not implemented")


def create_namespace(
    namespace: str, permissions: Dict[str, Any], *args, **kwargs
) -> Namespace:
    """
    Creates a table namespace with the given name and permissions. Returns
    the created namespace.
    """
    raise NotImplementedError("create_namespace not implemented")


def update_namespace(
    namespace: str,
    permissions: Optional[Dict[str, Any]] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Updates a table namespace's name and/or permissions. Raises an error if the
    given namespace does not exist.
    """
    raise NotImplementedError("update_namespace not implemented")


def create_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    schema: Optional[Union[pa.Schema, str, bytes]] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    partition_keys: Optional[List[Dict[str, Any]]] = None,
    primary_key_column_names: Optional[Set[str]] = None,
    sort_keys: Optional[List[SortKey]] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[Dict[str, str]] = None,
    table_permissions: Optional[Dict[str, Any]] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    partition_spec: Optional[StreamPartitionSpec] = None,
    *args,
    **kwargs,
) -> Stream:
    """
    Create a table version with an unreleased lifecycle state and an empty delta
    stream. Table versions may be schemaless and unpartitioned, or partitioned
    according to a list of partition key names and types. Note that partition
    keys are not required to exist in the table's schema, and can thus still be
    used with schemaless tables. This can be useful for creating logical shards
    of a delta stream where partition keys are known but not projected onto each
    row of the table (e.g. all rows of a customer orders table are known to
    correspond to a given order day, even if this column doesn't exist in the
    table). Primary and sort keys must exist within the table's schema.
    Permissions specified at the table level override any conflicting
    permissions specified at the table namespace level. Returns the stream
    for the created table version. Raises an error if the given namespace does
    not exist.

    Schemas are optional for DeltaCAT tables and can be used to inform the data
    consistency checks run for each field. If a schema is present, it can be
    used to enforce the following column-level data consistency policies at
    table load time:

    None: No consistency checks are run. May be mixed with the below two
    policies by specifying column names to pass through together with
    column names to coerce/validate.

    Coerce: Coerce fields to fit the schema whenever possible. An explicit
    subset of column names to coerce may optionally be specified.

    Validate: Raise an error for any fields that don't fit the schema. An
    explicit subset of column names to validate may optionally be specified.

    Either partition_keys or partition_spec must be specified but not both.
    """
    raise NotImplementedError("create_table_version not implemented")


def update_table(
    namespace: str,
    table_name: str,
    permissions: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    new_table_name: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Update table metadata describing the table versions it contains. By default,
    a table's properties are empty, and its description and permissions are
    equal to those given when its first table version was created. Raises an
    error if the given table does not exist.
    """
    raise NotImplementedError("update_table not implemented")


def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Union[pa.Schema, str, bytes]] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    *args,
    **kwargs,
) -> None:
    """
    Update a table version. Notably, updating an unreleased table version's
    lifecycle state to 'active' telegraphs that it is ready for external
    consumption, and causes all calls made to consume/produce streams,
    partitions, or deltas from/to its parent table to automatically resolve to
    this table version by default (i.e. when the client does not explicitly
    specify a different table version). Raises an error if the given table
    version does not exist.
    """
    raise NotImplementedError("update_table_version not implemented")


def stage_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Stream:
    """
    Stages a new delta stream for the given table version. Resolves to the
    latest active table version if no table version is given. Returns the
    staged stream. Raises an error if the table version does not exist.
    """
    raise NotImplementedError("stage_stream not implemented")


def commit_stream(stream: Stream, *args, **kwargs) -> Stream:
    """
    Registers a delta stream with a target table version, replacing any
    previous stream registered for the same table version. Returns the
    committed stream.
    """
    raise NotImplementedError("commit_stream not implemented")


def delete_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the delta stream currently registered with the given table version.
    Resolves to the latest active table version if no table version is given.
    Raises an error if the table version does not exist.
    """
    raise NotImplementedError("delete_stream not implemented")


def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Stream]:
    """
    Gets the most recently committed stream for the given table version and
    partition key values. Resolves to the latest active table version if no
    table version is given. Returns None if the table version does not exist.
    """
    raise NotImplementedError("get_stream not implemented")


def stage_partition(
    stream: Stream, partition_values: Optional[PartitionValues] = None, *args, **kwargs
) -> Partition:
    """
    Stages a new partition for the given stream and partition values. Returns
    the staged partition. If this partition will replace another partition
    with the same partition values, then it will have its previous partition ID
    set to the ID of the partition being replaced. Partition keys should not be
    specified for unpartitioned tables.

    The partition_values must represents the results of transforms in a partition
    spec specified in the stream.
    """
    raise NotImplementedError("stage_partition not implemented")


def commit_partition(
    partition: Partition,
    previous_partition: Optional[Partition] = None,
    *args,
    **kwargs,
) -> Partition:
    """
    Commits the given partition to its associated table version stream,
    replacing any previous partition (i.e., "partition being replaced") registered for the same stream and
    partition values.
    If the previous_partition is passed as an argument, the specified previous_partition will be the partition being replaced, otherwise it will be retrieved.
    Returns the registered partition. If the partition's
    previous delta stream position is specified, then the commit will
    be rejected if it does not match the actual previous stream position of
    the partition being replaced. If the partition's previous partition ID is
    specified, then the commit will be rejected if it does not match the actual
    ID of the partition being replaced.
    """
    raise NotImplementedError("commit_partition not implemented")


def delete_partition(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the given partition from the specified table version. Resolves to
    the latest active table version if no table version is given. Partition
    values should not be specified for unpartitioned tables. Raises an error
    if the table version or partition does not exist.
    """
    raise NotImplementedError("delete_partition not implemented")


def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the most recently committed partition for the given stream locator and
    partition key values. Returns None if no partition has been committed for
    the given table version and/or partition key values. Partition values
    should not be specified for unpartitioned tables.
    """
    raise NotImplementedError("get_partition not implemented")


def stage_delta(
    data: Union[LocalTable, LocalDataset, DistributedDataset, Manifest],
    partition: Partition,
    delta_type: DeltaType = DeltaType.UPSERT,
    max_records_per_entry: Optional[int] = None,
    author: Optional[ManifestAuthor] = None,
    properties: Optional[Dict[str, str]] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    delete_parameters: Optional[DeleteParameters] = None,
    partition_spec: Optional[DeltaPartitionSpec] = None,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> Delta:
    """
    Writes the given table to 1 or more S3 files. Returns an unregistered
    delta whose manifest entries point to the uploaded files. Applies any
    schema consistency policies configured for the parent table version.

    The partition spec will be used to split the input table into
    multiple files. Optionally, partition_values can be provided to avoid
    this method to recompute partition_values from the provided data.

    Raises an error if the provided data does not conform to a unique ordered
    list of partition_values
    """
    raise NotImplementedError("stage_delta not implemented")


def commit_delta(delta: Delta, *args, **kwargs) -> Delta:
    """
    Registers a new delta with its associated target table version and
    partition. Returns the registered delta. If the delta's previous stream
    position is specified, then the commit will be rejected if it does not match
    the target partition's actual previous stream position. If the delta's
    stream position is specified, it must be greater than the latest stream
    position in the target partition.
    """
    raise NotImplementedError("commit_delta not implemented")


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """
    Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)
    if namespace_exists(namespace=namespace, *args, **kwargs):
        lf_permissions = lf.list_permissions(
            ResourceType="DATABASE", Resource={"Database": {"Name": namespace}}
        )
        return _build_namespace_from_glue(namespace, lf_permissions=lf_permissions)
    return None


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)
    try:
        glue.get_database(Name=namespace)
    except glue.exceptions.EntityNotFoundException:
        return False
    return True


def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if the given
    table does not exist.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)

    if table_exists(namespace=namespace, table_name=table_name, *args, **kwargs):
        namespace_obj = get_namespace(namespace=namespace)
        table_locator = TableLocator.of(namespace_obj.locator, table_name=table_name)
        lf_permissions = lf.list_permissions(
            ResourceType="TABLE",
            Resource={"Table": {"Name": table_name, "DatabaseName": namespace}},
        )
        return Table.of(
            table_locator, permissions=lf_permissions["PrincipalResourcePermissions"]
        )
    else:
        return None


def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    result = namespace_exists(namespace=namespace, *args, **kwargs)

    if result is False:
        return False

    glue, lf = _get_client_from_kwargs(**kwargs)
    try:
        glue.get_table(DatabaseName=namespace, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        return False

    return True


def get_table_version(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the specified table version. Returns None
    if the given table version does not exist.
    """
    glue, lf = _get_client_from_kwargs(**kwargs)

    try:
        response = glue.get_table_version(
            DatabaseName=namespace, TableName=table_name, VersionId=table_version
        )
        if "TableVersion" not in response:
            return None

        lf_permissions = lf.list_permissions(
            ResourceType="TABLE",
            Resource={"Table": {"Name": table_name, "DatabaseName": namespace}},
        )

        return _build_table_version_from_glue(
            response["TableVersion"], lf_permissions=lf_permissions
        )

    except glue.exceptions.EntityNotFoundException:
        return None


def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest version of the specified table.
    Returns None if no table version exists for the given table.
    """
    raise NotImplementedError("get_latest_table_version not implemented")


def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest active version of the specified
    table. Returns None if no active table version exists for the given table.
    """
    raise NotImplementedError("get_latest_active_table_version not implemented")


def get_table_version_column_names(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[List[str]]:
    """
    Gets a list of column names for the specified table version, or for the
    latest active table version if none is specified. The index of each
    column name returned represents its ordinal position in a delimited text
    file or other row-oriented content type files appended to the table.
    Returns None for schemaless tables. Raises an error if the table version
    does not exist.
    """
    raise NotImplementedError("get_table_version_column_names not implemented")


def get_table_version_schema(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Union[pa.Schema, str, bytes]]:
    """
    Gets the schema for the specified table version, or for the latest active
    table version if none is specified. Returns None if the table version is
    schemaless. Raises an error if the table version does not exist.
    """
    raise NotImplementedError("get_table_version_schema not implemented")


def table_version_exists(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> bool:
    """
    Returns True if the given table version exists, False if not.
    """
    raise NotImplementedError("table_version_exists not implemented")


def can_categorize(e: BaseException, *args, **kwargs) -> bool:
    """
    Return whether input error is from storage implementation layer.
    """
    raise NotImplementedError


def raise_categorized_error(e: BaseException, *args, **kwargs):
    """
    Raise and handle storage implementation layer specific errors.
    """
    raise NotImplementedError


__all__ = [
    "exceptions",
]
