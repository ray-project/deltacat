import logging
import uuid
import posixpath

from typing import Any, Callable, Dict, List, Optional, Union, Tuple

from deltacat.catalog.model.properties import get_catalog_properties
from deltacat.constants import (
    DEFAULT_TABLE_VERSION,
    DATA_FILE_DIR_NAME,
    UNSIGNED_INT32_MAX_VALUE,
)
from deltacat.exceptions import (
    TableNotFoundError,
    TableVersionNotFoundError,
    DeltaCatError,
    UnclassifiedDeltaCatError,
    SchemaValidationError,
    StreamNotFoundError,
    PartitionNotFoundError,
    DeltaNotFoundError,
    NamespaceNotFoundError,
    TableValidationError,
    ConcurrentModificationError,
    ObjectAlreadyExistsError,
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
    TableVersionAlreadyExistsError,
    ObjectNotFoundError,
    StreamAlreadyExistsError,
    PartitionAlreadyExistsError,
    DeltaAlreadyExistsError,
)
from deltacat.storage.model.manifest import (
    EntryParams,
    EntryType,
    ManifestAuthor,
    ManifestEntryList,
    ManifestEntry,
)
from deltacat.storage.model.delta import (
    Delta,
    DeltaLocator,
    DeltaProperties,
    DeltaType,
)
from deltacat.storage.model.transaction import setup_transaction
from deltacat.storage.model.types import (
    CommitState,
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    TransactionOperationType,
    StreamFormat,
)
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import (
    Namespace,
    NamespaceLocator,
    NamespaceProperties,
)
from deltacat.storage.model.partition import (
    Partition,
    PartitionLocator,
    PartitionScheme,
    PartitionValues,
    UNPARTITIONED_SCHEME,
    UNPARTITIONED_SCHEME_ID,
)
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.sort_key import (
    SortScheme,
    UNSORTED_SCHEME,
)
from deltacat.storage.model.stream import (
    Stream,
    StreamLocator,
)
from deltacat.storage.model.table import (
    Table,
    TableProperties,
    TableLocator,
)
from deltacat.storage.model.table_version import (
    TableVersion,
    TableVersionProperties,
    TableVersionLocator,
)
from deltacat.storage.model.metafile import (
    Metafile,
)
from deltacat.storage.model.transaction import (
    TransactionOperation,
    Transaction,
)
from deltacat.storage.model.manifest import Manifest
from deltacat.types.media import (
    ContentType,
    DatasetType,
    DistributedDatasetType,
    StorageType,
    ContentEncoding,
)
from deltacat.utils.common import ReadKwargsProvider
import pyarrow as pa

from deltacat.types.tables import (
    TableProperty,
    get_table_writer,
    get_table_slicer,
    write_sliced_table,
    download_manifest_entries,
    download_manifest_entries_distributed,
    download_manifest_entry,
    reconstruct_manifest_entry_url,
)
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _normalize_partition_values(
    partition_values: Optional[PartitionValues],
) -> Optional[PartitionValues]:
    """
    Normalize partition values to ensure consistent representation of unpartitioned data.

    Both None and empty list [] represent unpartitioned data, but they should be
    normalized to None for consistent lookup and validation.

    Args:
        partition_values: The partition values to normalize

    Returns:
        None for unpartitioned data (both None and [] inputs),
        original value for partitioned data
    """
    if partition_values is None or (
        isinstance(partition_values, list) and len(partition_values) == 0
    ):
        return None
    return partition_values


def _list(
    metafile: Metafile,
    txn_op_type: TransactionOperationType,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> ListResult[Metafile]:
    catalog_properties = get_catalog_properties(**kwargs)
    limit = kwargs.get("limit") or None

    operation = TransactionOperation.of(
        operation_type=txn_op_type,
        dest_metafile=metafile,
        read_limit=limit,
    )

    if transaction is not None:
        # Add the read operation to the existing transaction and return the result
        return transaction.step(operation)
    else:
        # Create and commit a new transaction (legacy behavior)
        new_transaction = Transaction.of([operation])
        list_results_per_op = new_transaction.commit(
            catalog_root_dir=catalog_properties.root,
            filesystem=catalog_properties.filesystem,
        )
        return list_results_per_op[0]


def _latest(
    metafile: Metafile,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Metafile]:
    list_results = _list(
        metafile=metafile,
        txn_op_type=TransactionOperationType.READ_LATEST,
        transaction=transaction,
        *args,
        **kwargs,
    )
    results = list_results.all_items()
    return results[0] if results else None


def _exists(
    metafile: Metafile,
    *args,
    **kwargs,
) -> Optional[bool]:
    list_results = _list(
        metafile=metafile,
        txn_op_type=TransactionOperationType.READ_EXISTS,
        *args,
        **kwargs,
    )
    results = list_results.all_items()
    return True if results else False


def _resolve_latest_active_table_version_id(
    namespace: str,
    table_name: str,
    *args,
    fail_if_no_active_table_version: bool = True,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[str]:
    table = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not table:
        raise TableNotFoundError(f"Table does not exist: {namespace}.{table_name}")
    if fail_if_no_active_table_version and not table.latest_active_table_version:
        raise TableVersionNotFoundError(
            f"Table has no active table version: {namespace}.{table_name}"
        )
    return table.latest_active_table_version


def _resolve_latest_table_version_id(
    namespace: str,
    table_name: str,
    fail_if_no_active_table_version: True,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[str]:
    table = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not table:
        raise TableNotFoundError(f"Table does not exist: {namespace}.{table_name}")
    if fail_if_no_active_table_version and not table.latest_table_version:
        raise TableVersionNotFoundError(
            f"Table has no table version: {namespace}.{table_name}"
        )
    return table.latest_table_version


def _validate_schemes_against_schema(
    schema: Optional[Schema],
    partition_scheme: Optional[PartitionScheme],
    sort_scheme: Optional[SortScheme],
) -> None:
    """
    Validates partition and sort schemes against a schema, ensuring all referenced fields exist.
    If schema is None, validation is skipped.
    """
    if schema is None:
        return

    schema_fields = set(field.name for field in schema.arrow)

    # Validate partition scheme
    if partition_scheme is not None and partition_scheme.keys is not None:
        for key in partition_scheme.keys:
            if key.key[0] not in schema_fields:
                raise SchemaValidationError(
                    f"Partition key field '{key.key[0]}' not found in schema"
                )

    # Validate sort scheme
    if sort_scheme is not None and sort_scheme.keys is not None:
        for key in sort_scheme.keys:
            if key.key[0] not in schema_fields:
                raise SchemaValidationError(
                    f"Sort key field '{key.key[0]}' not found in schema"
                )


def _validate_partition_values_against_scheme(
    partition_values: Optional[PartitionValues],
    partition_scheme: PartitionScheme,
    schema: Optional[Schema],
) -> None:
    """
    Validates that partition values match the data types of the partition key fields in the schema.

    Args:
        partition_values: List of partition values to validate
        partition_scheme: The partition scheme containing the keys to validate against
        schema: The schema containing the field types to validate against

    Raises:
        TableValidationError: If validation fails
    """
    if not partition_values:
        raise TableValidationError("Partition values cannot be empty")

    if not schema:
        raise TableValidationError(
            "Table version must have a schema to validate partition values"
        )

    if len(partition_values) != len(partition_scheme.keys):
        raise TableValidationError(
            f"Number of partition values ({len(partition_values)}) does not match "
            f"number of partition keys ({len(partition_scheme.keys)})"
        )

    # Validate each partition value against its corresponding field type
    for i in range(len(partition_scheme.keys)):
        field_type = partition_scheme.keys[i].transform.return_type
        partition_value = partition_values[i]
        if field_type is None:
            # the transform returns the same type as the source schema type
            # (which also implies that it is a single-key transform)
            field_type = schema.field(partition_scheme.keys[i].key[0]).arrow.type
        try:
            # Try to convert the value to PyArrow to validate its type
            pa.array([partition_value], type=field_type)
            # If successful, the type is valid
        except (pa.lib.ArrowInvalid, pa.lib.ArrowTypeError) as e:
            raise TableValidationError(
                f"Partition value {partition_value} (type {type(partition_value)}) "
                f"incompatible with partition transform return type {field_type}"
            ) from e


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    return _list(
        metafile=Namespace.of(NamespaceLocator.of("placeholder")),
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        *args,
        **kwargs,
    )


def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    """
    Lists a page of tables for the given table namespace. Tables are returned as
    list result items. Raises an error if the given namespace does not exist.
    """
    locator = TableLocator.at(namespace=namespace, table_name="placeholder")
    try:
        return _list(
            metafile=Table.of(locator=locator),
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise NamespaceNotFoundError(f"Namespace {namespace} not found") from e


def list_table_versions(
    namespace: str,
    table_name: str,
    *args,
    **kwargs,
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table. Table versions are
    returned as list result items. Raises an error if the given table does not
    exist.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version="placeholder.0",
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    try:
        return _list(
            metafile=table_version,
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise TableNotFoundError(f"Table {namespace}.{table_name} not found") from e


def list_streams(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> ListResult[Stream]:
    """
    Lists a page of streams for the given table version.
    Raises an error if the table version does not exist.
    """
    # TODO(pdames): Support listing uncommitted streams.
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id="placeholder",
        stream_format=None,
    )
    stream = Stream.of(
        locator=locator,
        partition_scheme=None,
    )
    try:
        return _list(
            stream,
            TransactionOperationType.READ_SIBLINGS,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise TableVersionNotFoundError(
            f"Table version {namespace}.{table_name}.{table_version} not found"
        ) from e


def list_partitions(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> ListResult[Partition]:
    """
    Lists a page of partitions for the given table version. Partitions are
    returned as list result items. Table version resolves to the latest active
    table version if not specified. Raises an error if the table version does
    not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if not namespace:
        raise ValueError("Namespace cannot be empty.")
    if not table_name:
        raise ValueError("Table name cannot be empty.")
    # resolve default deltacat stream for the given namespace, table name, and table version
    # TODO(pdames): debug why this doesn't work when only the table_version is provided
    #   and PartitionLocator.stream_format is hard-coded to deltacat (we should be able
    #   to resolve the default deltacat stream automatically)
    stream = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not stream:
        raise StreamNotFoundError(
            f"Default stream for {namespace}.{table_name}.{table_version} not found."
        )
    locator = PartitionLocator.of(
        stream_locator=stream.locator,
        partition_values=["placeholder"],
        partition_id="placeholder",
    )
    partition = Partition.of(
        locator=locator,
        content_types=None,
    )
    try:
        result = _list(
            metafile=partition,
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            transaction=transaction,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise StreamNotFoundError(f"Stream {stream.locator} not found") from e

    if commit_transaction:
        transaction.seal()
    return result


def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    """
    Lists all partitions committed to the given stream.
    """
    # TODO(pdames): Support listing uncommitted partitions.
    if stream.stream_format != StreamFormat.DELTACAT:
        raise ValueError(
            f"Unsupported stream format: {stream.stream_format}"
            f"Expected stream format: {StreamFormat.DELTACAT}"
        )
    locator = PartitionLocator.of(
        stream_locator=stream.locator,
        partition_values=["placeholder"],
        partition_id="placeholder",
    )
    partition = Partition.of(
        locator=locator,
        content_types=None,
    )
    try:
        return _list(
            metafile=partition,
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise StreamNotFoundError(f"Stream {stream.locator} not found") from e


def list_deltas(
    namespace: str,
    table_name: str,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: Optional[bool] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> ListResult[Delta]:
    """
    Lists a page of deltas for the given table version and committed partition.
    Deltas are returned as list result items. Deltas returned can optionally be
    limited to inclusive first and last stream positions. Deltas are returned by
    descending stream position by default. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Partition scheme ID resolves to the
    table version's current partition scheme by default. Raises an error if the
    given table version or partition does not exist.

    To conserve memory, the deltas returned do not include manifests by
    default. The manifests can either be optionally retrieved as part of this
    call or lazily loaded via subsequent calls to `get_delta_manifest`.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    # TODO(pdames): Delta listing should ideally either use an efficient
    #  range-limited dir listing of partition children between start and end
    #  positions, or should traverse using Partition.stream_position (to
    #  resolve last stream position) and Delta.previous_stream_position
    #  (down to first stream position).

    if first_stream_position is not None and last_stream_position is not None:
        if first_stream_position > last_stream_position:
            raise ValueError(
                f"first_stream_position must be less than or equal to last_stream_position. "
                f"first_stream_position: {first_stream_position}, last_stream_position: {last_stream_position}"
            )

    # First get the stream to resolve proper table version and stream locator
    stream = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not stream:
        raise StreamNotFoundError(
            f"Failed to resolve stream for "
            f"`{namespace}.{table_name}` at table version "
            f"`{table_version or 'latest'}` (no stream found)."
        )

    # Then get the actual partition to ensure we have the real partition locator with ID
    partition = get_partition(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not partition:
        raise PartitionNotFoundError(
            f"Failed to find partition for stream {stream.locator} "
            f"with partition_values={partition_values} and "
            f"partition_scheme_id={partition_scheme_id}"
        )
    # Use the actual partition locator (with partition ID) for listing deltas
    locator = DeltaLocator.of(partition_locator=partition.locator)
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    try:
        all_deltas_list_result: ListResult[Delta] = _list(
            metafile=delta,
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            transaction=transaction,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise PartitionNotFoundError(f"Partition {partition.locator} not found") from e
    all_deltas = all_deltas_list_result.all_items()
    filtered_deltas = [
        delta
        for delta in all_deltas
        if (
            first_stream_position is None
            or first_stream_position <= delta.stream_position
        )
        and (
            last_stream_position is None
            or delta.stream_position <= last_stream_position
        )
    ]
    # Sort deltas by stream position in the requested order
    filtered_deltas.sort(reverse=(not ascending_order), key=lambda d: d.stream_position)

    if commit_transaction:
        transaction.seal()

    return ListResult.of(
        items=filtered_deltas,
        pagination_key=None,
        next_page_provider=None,
    )


def _get_partition_delta(
    partition_like: Union[Partition, PartitionLocator],
    stream_position: int,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Delta:
    locator = DeltaLocator.of(
        partition_locator=partition_like
        if isinstance(partition_like, PartitionLocator)
        else partition_like.locator,
        stream_position=stream_position,
    )
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    return _latest(
        metafile=delta,
        transaction=transaction,
        *args,
        **kwargs,
    )


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
    # TODO(pdames): Delta listing should ideally either use an efficient
    #  range-limited dir listing of partition children between start and end
    #  positions, or should traverse using Partition.stream_position (to
    #  resolve last stream position) and Delta.previous_stream_position
    #  (down to first stream position).
    if first_stream_position is not None and last_stream_position is not None:
        if first_stream_position > last_stream_position:
            raise ValueError(
                f"first_stream_position must be less than or equal to last_stream_position. "
                f"first_stream_position: {first_stream_position}, last_stream_position: {last_stream_position}"
            )
    locator = DeltaLocator.of(
        partition_locator=partition_like
        if isinstance(partition_like, PartitionLocator)
        else partition_like.locator,
        stream_position=None,
    )
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    try:
        all_deltas_list_result: ListResult[Delta] = _list(
            metafile=delta,
            txn_op_type=TransactionOperationType.READ_SIBLINGS,
            *args,
            **kwargs,
        )
    except ObjectNotFoundError as e:
        raise PartitionNotFoundError(f"Partition {partition_like} not found") from e
    all_deltas = all_deltas_list_result.all_items()
    filtered_deltas = [
        delta
        for delta in all_deltas
        if (
            first_stream_position is None
            or first_stream_position <= delta.stream_position
        )
        and (
            last_stream_position is None
            or delta.stream_position <= last_stream_position
        )
    ]
    # Sort deltas by stream position in the requested order
    filtered_deltas.sort(reverse=(not ascending_order), key=lambda d: d.stream_position)
    return ListResult.of(
        items=filtered_deltas,
        pagination_key=None,
        next_page_provider=None,
    )


def get_delta(
    namespace: str,
    table_name: str,
    stream_position: int,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the delta for the given table version, partition, and stream position.
    Table version resolves to the latest active table version if not specified.
    Partition values should not be specified for unpartitioned tables. Partition
    scheme ID resolves to the table version's current partition scheme by
    default. Raises an error if the given table version or partition does not
    exist.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    # TODO(pdames): Honor `include_manifest` param.

    # First get the stream to resolve proper table version and stream locator
    stream = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not stream:
        raise StreamNotFoundError(
            f"Failed to resolve stream for "
            f"`{namespace}.{table_name}` at table version "
            f"`{table_version or 'latest'}` (no stream found)."
        )

    # Then get the actual partition to ensure we have the real partition locator with ID
    partition = get_partition(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not partition:
        raise PartitionNotFoundError(
            f"Failed to find partition for stream {stream.locator} "
            f"with partition_values={partition_values} and "
            f"partition_scheme_id={partition_scheme_id}"
        )

    # Use the actual partition locator (with partition ID) for getting the delta
    result = _get_partition_delta(
        partition.locator,
        stream_position,
        transaction=transaction,
        *args,
        **kwargs,
    )

    # TODO(pdames): Honor the include_manifest parameter during retrieval from _latest, since
    #   the point is to avoid loading the manifest into memory if it's not needed.
    if result and not include_manifest:
        result.manifest = None

    if commit_transaction:
        transaction.seal()
    return result


def get_latest_delta(
    namespace: str,
    table_name: str,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the latest ordered delta for the given table version and partition. Table version
    resolves to the latest active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Partition scheme ID resolves to the table version's
    current partition scheme by default. Raises an error if the given table version or partition
    does not exist. Unordered deltas will not be returned.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    stream = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not stream:
        raise StreamNotFoundError(
            f"Failed to find stream for "
            f"`{namespace}.{table_name}` at table version "
            f"`{table_version or 'latest'}`."
        )
    partition = get_partition(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not partition:
        raise PartitionNotFoundError(
            f"Failed to find partition for stream {stream.locator} "
            f"with partition_values={partition_values} and "
            f"partition_scheme_id={partition_scheme_id}"
        )
    if partition.stream_position:
        result = _get_partition_delta(
            partition.locator,
            partition.stream_position,
            transaction=transaction,
            *args,
            **kwargs,
        )
        # TODO(pdames): Honor the include_manifest parameter during retrieval from _latest, since
        #   the point is to also avoid even downloading the manifest if it's not needed.
        if result and not include_manifest:
            result.manifest = None
    else:
        # no ordered deltas in the partition
        result = None

    if commit_transaction:
        transaction.seal()
    return result


def _download_delta_distributed(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: Optional[
        DistributedDatasetType
    ] = DistributedDatasetType.RAY_DATASET,
    **kwargs,
) -> DistributedDataset:

    distributed_dataset: DistributedDataset = download_manifest_entries_distributed(
        manifest=manifest,
        table_type=table_type,
        max_parallelism=max_parallelism,
        column_names=column_names,
        include_columns=include_columns,
        file_reader_kwargs_provider=file_reader_kwargs_provider,
        ray_options_provider=ray_options_provider,
        distributed_dataset_type=distributed_dataset_type,
        *args,
        **kwargs,
    )

    return distributed_dataset


def _download_delta_local(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    **kwargs,
) -> LocalDataset:
    tables: LocalDataset = download_manifest_entries(
        manifest,
        table_type,
        max_parallelism if max_parallelism else 1,
        column_names,
        include_columns,
        file_reader_kwargs_provider,
        **kwargs,
    )
    return tables


def download_delta(
    delta_like: Union[Delta, DeltaLocator],
    table_type: DatasetType = DatasetType.PYARROW,
    storage_type: StorageType = StorageType.DISTRIBUTED,
    max_parallelism: Optional[int] = None,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: DistributedDatasetType = DistributedDatasetType.RAY_DATASET,
    file_path_column: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    all_column_names: Optional[List[str]] = None,
    **kwargs,
) -> Union[LocalDataset, DistributedDataset]:  # type: ignore
    """
    Read the given delta or delta locator into either a list of
    tables resident in the local node's memory, or into a dataset distributed
    across this Ray cluster's object store memory. Ordered table N of a local
    table list, or ordered block N of a distributed dataset, always contain
    the contents of ordered delta manifest entry N.
    """
    # TODO (pdames): Cast delimited text types to the table's schema types
    # TODO (pdames): Deprecate this method and replace with `read_delta`
    # TODO (pdames): Replace dependence on TableType, StorageType, and DistributedDatasetType
    #   with DatasetType

    # if all column names are provided, then this is a pure manifest entry download (no transaction needed)
    commit_transaction = False
    if not all_column_names:
        transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    storage_type_to_download_func = {
        StorageType.LOCAL: _download_delta_local,
        StorageType.DISTRIBUTED: _download_delta_distributed,
    }

    is_delta = isinstance(delta_like, Delta)
    is_delta_locator = isinstance(delta_like, DeltaLocator)

    delta_locator: Optional[DeltaLocator] = None
    if is_delta_locator:
        delta_locator = delta_like
    elif is_delta:
        delta_locator = Delta(delta_like).locator
    if not delta_locator:
        raise ValueError(
            f"Expected delta_like to be a Delta or DeltaLocator, but found "
            f"{type(delta_like)}."
        )

    # Get manifest - if delta_like is a Delta with a manifest, use it, otherwise fetch from storage
    if is_delta and delta_like.manifest:
        manifest = delta_like.manifest
    elif all_column_names:
        raise ValueError(
            "All column names can only be specified with a delta with an inline manifest."
        )
    else:
        manifest = get_delta_manifest(
            delta_locator,
            transaction=transaction,
            *args,
            **kwargs,
        )
    all_column_names = all_column_names or None
    if not all_column_names:
        table_version_schema = get_table_version_schema(
            delta_locator.namespace,
            delta_locator.table_name,
            delta_locator.table_version,
            transaction=transaction,
            *args,
            **kwargs,
        )
        if table_version_schema and table_version_schema.arrow:
            all_column_names = [field.name for field in table_version_schema.arrow]
            if distributed_dataset_type == DatasetType.DAFT:
                # Daft needs the latest table version schema to properly handle schema evolution
                kwargs["table_version_schema"] = table_version_schema.arrow
    elif distributed_dataset_type == DatasetType.DAFT:
        raise ValueError("All column names canot be specified with Daft.")
    if columns:
        # Extract file_path_column since it's appended after reading each file
        columns_to_validate = (
            [col for col in columns if col != file_path_column]
            if file_path_column
            else columns
        )

        # Only validate columns if we have schema information (all_column_names is not None)
        if all_column_names is not None:
            if not all(
                col in [col_name.lower() for col_name in all_column_names]
                for col in columns_to_validate
            ):
                raise SchemaValidationError(
                    f"One or more columns in {columns_to_validate} are not present in table "
                    f"version columns {all_column_names}"
                )
        columns = [column.lower() for column in columns]
    logger.debug(
        f"Reading {columns or 'all'} columns from table version column "
        f"names: {all_column_names}. "
    )

    # Filter out parameters that are already passed as positional/keyword arguments
    # to avoid "multiple values for argument" errors
    filtered_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k
        not in [
            "manifest",
            "table_type",
            "max_parallelism",
            "column_names",
            "include_columns",
            "file_reader_kwargs_provider",
            "ray_options_provider",
            "distributed_dataset_type",
        ]
    }

    dataset = storage_type_to_download_func[storage_type](
        manifest,
        table_type,
        max_parallelism,
        all_column_names,
        columns,
        file_reader_kwargs_provider,
        ray_options_provider=ray_options_provider,
        distributed_dataset_type=distributed_dataset_type,
        file_path_column=file_path_column,
        **filtered_kwargs,
    )
    if commit_transaction:
        transaction.seal()
    return dataset


def _download_manifest_entry(
    manifest_entry: ManifestEntry,
    table_type: DatasetType = DatasetType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    content_type: Optional[ContentType] = None,
    content_encoding: Optional[ContentEncoding] = None,
    filesystem: Optional[pa.fs.FileSystem] = None,
) -> LocalTable:

    return download_manifest_entry(
        manifest_entry,
        table_type,
        column_names,
        include_columns,
        file_reader_kwargs_provider,
        content_type,
        content_encoding,
        filesystem,
    )


def download_delta_manifest_entry(
    delta_like: Union[Delta, DeltaLocator],
    entry_index: int,
    table_type: DatasetType = DatasetType.PYARROW,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    transaction: Optional[Transaction] = None,
    all_column_names: Optional[List[str]] = None,
    **kwargs,
) -> LocalTable:
    """
    Reads a single manifest entry into the specified table type for the
    given delta or delta locator. If a delta is provided with a non-empty
    manifest, then the entry is read from this manifest. Otherwise, the
    manifest is first retrieved then the given entry index read.

    NOTE: The entry will be read in the current node's memory.
    """
    # if all column names are provided, then this is a pure manifest entry download (no transaction needed)
    commit_transaction = False
    if not all_column_names:
        transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    is_delta = isinstance(delta_like, Delta)
    is_delta_locator = isinstance(delta_like, DeltaLocator)

    delta_locator: Optional[DeltaLocator] = None
    if is_delta_locator:
        delta_locator = delta_like
    elif is_delta:
        delta_locator = Delta(delta_like).locator
    if not delta_locator:
        raise ValueError(
            f"Expected delta_like to be a Delta or DeltaLocator, but found "
            f"{type(delta_like)}."
        )

    if is_delta and delta_like.manifest:
        manifest = delta_like.manifest
    elif all_column_names:
        raise ValueError(
            "All column names can only be specified with a delta with an inline manifest."
        )
    else:
        manifest = get_delta_manifest(
            delta_locator,
            transaction=transaction,
            *args,
            **kwargs,
        )
    # TODO(pdames): Cache table version column names and only invoke when
    #  needed.
    all_column_names = all_column_names or get_table_version_column_names(
        delta_locator.namespace,
        delta_locator.table_name,
        delta_locator.table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if columns:
        if not all(
            col in [col_name.lower() for col_name in all_column_names]
            for col in columns
        ):
            raise SchemaValidationError(
                f"One or more columns in {columns} are not present in table "
                f"version columns {all_column_names}"
            )
        columns = [column.lower() for column in columns]
    logger.debug(
        f"Reading {columns or 'all'} columns from table version column "
        f"names: {all_column_names}. "
    )
    catalog_properties = get_catalog_properties(**kwargs)
    manifest_entry = _download_manifest_entry(
        reconstruct_manifest_entry_url(manifest.entries[entry_index], **kwargs),
        table_type,
        all_column_names,
        columns,
        file_reader_kwargs_provider,
        filesystem=catalog_properties.filesystem,
    )
    if commit_transaction:
        transaction.seal()
    return manifest_entry


def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator],
    *args,
    **kwargs,
) -> Manifest:
    """
    Get the manifest associated with the given delta or delta locator. This
    always retrieves the authoritative durable copy of the delta manifest, and
    never the local manifest defined for any input delta. Raises an error if
    the delta can't be found, or if it doesn't contain a manifest.
    """
    if isinstance(delta_like, Delta):
        delta_locator = delta_like.locator
    elif isinstance(delta_like, DeltaLocator):
        delta_locator = delta_like
    else:
        raise ValueError(
            f"Expected delta or delta locator, but got: {type(delta_like)}"
        )
    delta = Delta.of(
        locator=delta_locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    latest_delta: Delta = _latest(
        metafile=delta,
        *args,
        **kwargs,
    )
    if not latest_delta:
        raise DeltaNotFoundError(f"No delta found for locator: {delta_locator}")
    elif not latest_delta.manifest:
        raise DeltaNotFoundError(f"No manifest found for delta: {latest_delta}")
    return latest_delta.manifest


def create_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Namespace:
    """
    Creates a table namespace with the given name and properties. Returns
    the created namespace.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    namespace = Namespace.of(
        locator=NamespaceLocator.of(namespace=namespace),
        properties=properties,
    )

    # Add the operation to the transaction
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=namespace,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise NamespaceAlreadyExistsError(
            f"Namespace {namespace} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()
    return namespace


def update_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """
    Updates a table namespace's name and/or properties. Raises an error if the
    given namespace does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    # Check if the namespace exists
    old_namespace_meta = get_namespace(
        namespace=namespace,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not old_namespace_meta:
        raise NamespaceNotFoundError(f"Namespace {namespace} does not exist")

    # Create new namespace metadata
    new_namespace_meta: Namespace = Metafile.update_for(old_namespace_meta)
    if new_namespace:
        new_namespace_meta.locator.namespace = new_namespace
    if properties is not None:
        new_namespace_meta.properties = properties

    # Add the update operation to the transaction
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_namespace_meta,
                src_metafile=old_namespace_meta,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise NamespaceAlreadyExistsError(
            f"Namespace {namespace} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()


def create_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = LifecycleState.CREATED,
    schema: Optional[Schema] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Tuple[Table, TableVersion, Stream]:
    """
    Create a table version with the given or CREATED lifecycle state and an empty delta
    stream. Table versions may be schemaless and unpartitioned to improve write
    performance, or have their writes governed by a schema and partition scheme
    to improve data consistency and read performance.

    Returns a tuple containing the created/updated table, table version, and
    stream (respectively).

    Raises an error if the given namespace does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if not namespace_exists(
        namespace=namespace,
        transaction=transaction,
        *args,
        **kwargs,
    ):
        raise NamespaceNotFoundError(f"Namespace {namespace} does not exist")

    # Validate schemes against schema
    _validate_schemes_against_schema(schema, partition_scheme, sort_keys)

    # coerce unspecified partition schemes to the unpartitioned scheme
    partition_scheme = partition_scheme or UNPARTITIONED_SCHEME
    # coerce unspecified sort schemes to the unsorted scheme
    sort_keys = sort_keys or UNSORTED_SCHEME
    # check if a parent table and/or previous table version already exist
    prev_table_version = None
    prev_table = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not prev_table:
        # no parent table exists, so we'll create it in this transaction
        table_txn_op_type = TransactionOperationType.CREATE
        prev_table = None
        new_table = Table.of(
            locator=TableLocator.at(namespace=namespace, table_name=table_name),
        )
        table_version = table_version or DEFAULT_TABLE_VERSION
    else:
        # the parent table exists, so we'll update it in this transaction
        table_txn_op_type = TransactionOperationType.UPDATE
        new_table: Table = Metafile.update_for(prev_table)
        prev_table_version = prev_table.latest_table_version
        if not table_version:
            # generate the next table version ID
            table_version = TableVersion.next_version(prev_table_version)
        else:
            # ensure that the given table version number matches expectations
            expected_table_version = TableVersion.next_version(prev_table_version)
            _, version_number = TableVersion.parse_table_version(
                table_version,
            )
            _, expected_version_number = TableVersion.parse_table_version(
                expected_table_version,
            )
            if version_number != expected_version_number:
                raise TableVersionAlreadyExistsError(
                    f"Table {namespace}.{table_name} version {table_version} already exists."
                )
    if table_description is not None:
        new_table.description = table_description
    if table_properties is not None:
        new_table.properties = table_properties
    new_table.latest_table_version = table_version
    new_table.latest_active_table_version = (
        table_version if lifecycle_state == LifecycleState.ACTIVE else None
    )
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=schema,
        partition_scheme=partition_scheme,
        description=table_version_description,
        properties=table_version_properties,
        content_types=supported_content_types,
        sort_scheme=sort_keys,
        watermark=None,
        lifecycle_state=lifecycle_state,
        schemas=[schema] if schema else None,
        partition_schemes=[partition_scheme],
        sort_schemes=[sort_keys],
        previous_table_version=prev_table_version,
    )
    # create the table version's default deltacat stream in this transaction
    stream_locator = StreamLocator.of(
        table_version_locator=locator,
        stream_id=str(uuid.uuid4()),
        stream_format=StreamFormat.DELTACAT,
    )
    stream = Stream.of(
        locator=stream_locator,
        partition_scheme=partition_scheme,
        state=CommitState.COMMITTED,
        previous_stream_id=None,
        watermark=None,
    )
    # Add operations to the transaction
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=table_txn_op_type,
                dest_metafile=new_table,
                src_metafile=prev_table,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise TableAlreadyExistsError(
            f"Table {namespace}.{table_name} already exists"
        ) from e
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=table_version,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise TableVersionAlreadyExistsError(
            f"Table version {namespace}.{table_name}.{table_version} already exists"
        ) from e

    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=stream,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise StreamAlreadyExistsError(
            f"Stream {namespace}.{table_name}.{table_version}.{stream_locator.stream_id} already exists"
        ) from e
    if commit_transaction:
        transaction.seal()
    return new_table, table_version, stream


def create_table(
    namespace: str,
    table_name: str,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Table:
    """
    Create a new table. Raises an error if the given table already exists.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    new_table: Table = Table.of(
        locator=TableLocator.at(namespace=namespace, table_name=table_name),
        description=description,
        properties=properties,
    )
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=new_table,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise TableAlreadyExistsError(
            f"Table {namespace}.{table_name} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()
    return new_table


def update_table(
    namespace: str,
    table_name: str,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    new_table_name: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Table:
    """
    Update table metadata describing the table versions it contains. By default,
    a table's properties are empty, and its description is equal to that given
    when its first table version was created. Raises an error if the given
    table does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    old_table = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not old_table:
        raise TableNotFoundError(f"Table `{namespace}.{table_name}` does not exist.")
    new_table: Table = Metafile.update_for(old_table)
    new_table.description = description or old_table.description
    new_table.properties = properties or old_table.properties
    new_table.table_name = new_table_name or old_table.table_name

    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_table,
                src_metafile=old_table,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise TableAlreadyExistsError(
            f"Table {namespace}.{table_name} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()
    return new_table


def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
    description: Optional[str] = None,
    properties: Optional[TableVersionProperties] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Tuple[Optional[Table], TableVersion, Optional[Stream]]:
    """
    Update a table version. Notably, updating an unreleased table version's
    lifecycle state to 'ACTIVE' telegraphs that it is ready for external
    consumption, and causes all calls made to consume/produce streams,
    partitions, or deltas from/to its parent table to automatically resolve to
    this table version by default (i.e., when the client does not explicitly
    specify a different table version). Raises an error if the given table
    version does not exist.

    Note that, to transition a table version from partitioned to unpartitioned,
    partition_scheme must be explicitly set to UNPARTITIONED_SCHEME. Similarly
    to transition a table version from sorted to unsorted, sort_keys must be
    explicitly set to UNSORTED_SCHEME.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    old_table_version = get_table_version(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not old_table_version:
        raise TableVersionNotFoundError(
            f"Table version `{table_version}` does not exist for "
            f"table `{namespace}.{table_name}`."
        )

    # If schema is not provided but partition_scheme or sort_keys are,
    # validate against the existing schema
    schema_to_validate = schema or old_table_version.schema
    _validate_schemes_against_schema(schema_to_validate, partition_scheme, sort_keys)

    new_table_version: TableVersion = Metafile.update_for(old_table_version)
    new_table_version.state = lifecycle_state or old_table_version.state

    # Caller is expected to do all necessary backwards compatibility schema checks
    update_schema = schema and not schema.equivalent_to(
        old_table_version.schema,
        True,
    )
    if update_schema and schema.id in [s.id for s in old_table_version.schemas]:
        raise TableValidationError(
            f"Schema ID `{schema.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.schema = schema if update_schema else old_table_version.schema
    new_table_version.schemas = (
        old_table_version.schemas + [schema]
        if update_schema
        else old_table_version.schemas
    )
    new_table_version.description = (
        description if description is not None else old_table_version.description
    )
    new_table_version.properties = (
        properties if properties is not None else old_table_version.properties
    )
    new_supported_reader_types = new_table_version.read_table_property(
        TableProperty.SUPPORTED_READER_TYPES
    )
    if new_supported_reader_types:
        old_supported_reader_types = (
            old_table_version.read_table_property(TableProperty.SUPPORTED_READER_TYPES)
            or {}
        )
        added_supported_reader_types = set(new_supported_reader_types) - set(
            old_supported_reader_types
        )
        if added_supported_reader_types:
            raise TableValidationError(
                f"Cannot add new supported reader types: {added_supported_reader_types}"
            )
    new_table_version.partition_scheme = (
        partition_scheme or old_table_version.partition_scheme
    )
    # TODO(pdames): Check for backwards incompatible partition scheme changes.
    update_partition_scheme = partition_scheme and not partition_scheme.equivalent_to(
        old_table_version.partition_scheme,
        True,
    )
    if update_partition_scheme and partition_scheme.id in [
        ps.id for ps in old_table_version.partition_schemes
    ]:
        raise TableValidationError(
            f"Partition scheme ID `{partition_scheme.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.partition_schemes = (
        old_table_version.partition_schemes + [partition_scheme]
        if update_partition_scheme
        else old_table_version.partition_schemes
    )
    # TODO(pdames): Check for backwards incompatible sort scheme changes.
    update_sort_scheme = sort_keys and not sort_keys.equivalent_to(
        old_table_version.sort_scheme,
        True,
    )
    if update_sort_scheme and sort_keys.id in [
        sk.id for sk in old_table_version.sort_schemes
    ]:
        raise TableValidationError(
            f"Sort scheme ID `{sort_keys.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.sort_scheme = sort_keys or old_table_version.sort_scheme
    new_table_version.sort_schemes = (
        old_table_version.sort_schemes + [sort_keys]
        if update_sort_scheme
        else old_table_version.sort_schemes
    )
    old_table = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )
    new_table: Table = None
    if (
        lifecycle_state == LifecycleState.ACTIVE
        and old_table_version.state != LifecycleState.ACTIVE
    ):
        _, old_version_number = (
            TableVersion.parse_table_version(
                old_table.latest_active_table_version,
            )
            if old_table.latest_active_table_version
            else (None, None)
        )
        _, new_version_number = TableVersion.parse_table_version(table_version)
        if old_version_number is None or old_version_number < new_version_number:
            # update the table's latest table version
            new_table = Metafile.update_for(old_table)
            new_table.latest_active_table_version = table_version
            transaction.step(
                TransactionOperation.of(
                    operation_type=TransactionOperationType.UPDATE,
                    dest_metafile=new_table,
                    src_metafile=old_table,
                ),
            )
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_table_version,
                src_metafile=old_table_version,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise TableVersionAlreadyExistsError(
            f"Table version {namespace}.{table_name}.{table_version} already exists"
        ) from e

    # TODO(pdames): Push changes down to non-deltacat streams via sync module.
    #   Also copy sort scheme changes down to deltacat child stream?
    new_stream: Stream = None
    if partition_scheme:
        old_stream = get_stream(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            transaction=transaction,
            *args,
            **kwargs,
        )
        new_stream = Metafile.update_for(old_stream)
        new_stream.partition_scheme = partition_scheme
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_stream,
                src_metafile=old_stream,
            ),
        )
    if commit_transaction:
        transaction.seal()
    return new_table, new_table_version, new_stream


def stage_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Stream:
    """
    Stages a new delta stream for the given table version. Resolves to the
    latest active table version if no table version is given. Resolves to the
    DeltaCAT stream format if no stream format is given. If this stream
    will replace another stream with the same format and scheme, then it will
    have its previous stream ID set to the ID of the stream being replaced.
    Returns the staged stream. Raises an error if the table version does not
    exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            namespace=namespace,
            table_name=table_name,
            transaction=transaction,
            *args,
            **kwargs,
        )
    table_version_meta = get_table_version(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not table_version_meta:
        raise TableVersionNotFoundError(
            f"Table version not found: {namespace}.{table_name}.{table_version}."
        )
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=str(uuid.uuid4()),
        stream_format=stream_format or StreamFormat.DELTACAT,
    )
    stream = Stream.of(
        locator=locator,
        partition_scheme=table_version_meta.partition_scheme,
        state=CommitState.STAGED,
        previous_stream_id=None,
        watermark=None,
    )
    prev_stream = get_stream(
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=stream.stream_format,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if prev_stream:
        if prev_stream.stream_id == stream.stream_id:
            raise TableValidationError(
                f"Stream to stage has the same ID as existing stream: {prev_stream}."
            )
        stream.previous_stream_id = prev_stream.stream_id
    # Add the operation to the transaction
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=stream,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise StreamAlreadyExistsError(
            f"Stream {namespace}.{table_name}.{table_version}.{stream.stream_id} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()
    return stream


def commit_stream(
    stream: Stream,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Stream:
    """
    Registers a staged delta stream with a target table version, replacing any
    previous stream registered for the same table version. Returns the
    committed stream.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if not stream.stream_id:
        raise ValueError("Stream ID to commit must be set to a staged stream ID.")
    if not stream.table_version_locator:
        raise ValueError(
            "Stream to commit must have its table version locator "
            "set to the parent of its staged stream ID."
        )
    prev_staged_stream = get_stream_by_id(
        table_version_locator=stream.table_version_locator,
        stream_id=stream.stream_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not prev_staged_stream:
        raise StreamNotFoundError(
            f"Stream at table version {stream.table_version_locator} with ID "
            f"{stream.stream_id} not found."
        )
    if prev_staged_stream.state != CommitState.STAGED:
        raise TableValidationError(
            f"Expected to find a `{CommitState.STAGED}` stream at table version "
            f"{stream.table_version_locator} with ID {stream.stream_id},"
            f"but found a `{prev_staged_stream.state}` partition."
        )
    stream: Stream = Metafile.update_for(prev_staged_stream)
    stream.state = CommitState.COMMITTED
    prev_committed_stream = get_stream(
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=stream.stream_format,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if prev_committed_stream:
        # there's a previously committed stream, so update the transaction
        # type to overwrite the previously committed stream
        txn_op_type = TransactionOperationType.REPLACE
    else:
        txn_op_type = TransactionOperationType.UPDATE

    # the first transaction operation updates the staged stream commit state
    transaction.step(
        TransactionOperation.of(
            operation_type=txn_op_type,
            dest_metafile=stream,
            src_metafile=prev_staged_stream,
        ),
    )
    if prev_committed_stream:
        if prev_committed_stream.stream_id != stream.previous_stream_id:
            raise ConcurrentModificationError(
                f"Previous stream ID mismatch Expected "
                f"{stream.previous_stream_id} but found "
                f"{prev_committed_stream.stream_id}."
            )
        if prev_committed_stream.stream_id == stream.stream_id:
            raise TableValidationError(
                f"Stream to commit has the same ID as existing stream: {prev_committed_stream}."
            )
        # add another transaction operation to replace the previously committed stream
        # with the staged stream
        transaction.step(
            TransactionOperation.of(
                operation_type=txn_op_type,
                dest_metafile=stream,
                src_metafile=prev_committed_stream,
            ),
        )
    if commit_transaction:
        transaction.seal()
    return stream


def delete_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """
    Deletes the delta stream currently registered with the given table version.
    Resolves to the latest active table version if no table version is given.
    Resolves to the deltacat stream format if no stream format is given.
    Raises an error if the stream does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            namespace=namespace,
            table_name=table_name,
            transaction=transaction,
            *args,
            **kwargs,
        )
    stream_to_delete = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_format=stream_format,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not stream_to_delete:
        raise StreamNotFoundError(
            f"Stream to delete not found: {namespace}.{table_name}"
            f".{table_version}.{stream_format}."
        )
    else:
        stream_to_delete.state = CommitState.DEPRECATED

    transaction.step(
        TransactionOperation.of(
            operation_type=TransactionOperationType.DELETE,
            dest_metafile=stream_to_delete,
        ),
    )

    if commit_transaction:
        transaction.seal()


def delete_table(
    namespace: str,
    table_name: str,
    purge: bool = False,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """
    Drops the given table from the catalog. If purge is True, also removes
    all data files associated with the table. Raises an error if the given table
    does not exist.
    """
    if purge:
        raise NotImplementedError("Purge flag is not currently supported.")
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    table: Optional[Table] = get_table(
        namespace=namespace,
        table_name=table_name,
        transaction=transaction,
        *args,
        **kwargs,
    )

    if not table:
        # TODO(pdames): Refactor this so that it doesn't initialize Ray
        raise TableNotFoundError(f"Table `{namespace}.{table_name}` does not exist.")

    transaction.step(
        TransactionOperation.of(
            operation_type=TransactionOperationType.DELETE,
            dest_metafile=table,
        ),
    )

    if commit_transaction:
        transaction.seal()


def delete_namespace(
    namespace: str,
    purge: bool = False,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """
    Drops the given namespace from the catalog. If purge is True, also removes
    all data files associated with the namespace. Raises an error if the given
    namespace does not exist.
    """
    if purge:
        raise NotImplementedError("Purge flag is not currently supported.")
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    namespace_obj: Optional[Namespace] = get_namespace(
        namespace=namespace,
        transaction=transaction,
        *args,
        **kwargs,
    )

    if not namespace_obj:
        raise NamespaceNotFoundError(f"Namespace `{namespace}` does not exist.")

    transaction.step(
        TransactionOperation.of(
            operation_type=TransactionOperationType.DELETE,
            dest_metafile=namespace_obj,
        ),
    )

    if commit_transaction:
        transaction.seal()


def get_stream_by_id(
    table_version_locator: TableVersionLocator,
    stream_id: str,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the stream for the given table version locator and stream ID.
    Returns None if the stream does not exist. Raises an error if the given
    table version locator does not exist.
    """
    locator = StreamLocator.of(
        table_version_locator=table_version_locator,
        stream_id=stream_id,
        stream_format=None,
    )
    return _latest(
        metafile=Stream.of(locator=locator, partition_scheme=None),
        *args,
        **kwargs,
    )


def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Stream]:
    """
    Gets the most recently committed stream for the given table version.
    Resolves to the latest active table version if no table version is given.
    Resolves to the DeltaCAT stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            namespace=namespace,
            table_name=table_name,
            fail_if_no_active_table_version=False,
            transaction=transaction,
            *args,
            **kwargs,
        )
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=None,
        stream_format=stream_format,
    )
    stream = _latest(
        metafile=Stream.of(
            locator=locator,
            partition_scheme=None,
            state=CommitState.COMMITTED,
        ),
        transaction=transaction,
        *args,
        **kwargs,
    )
    if commit_transaction:
        transaction.seal()
    return stream


def stream_exists(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Stream]:
    """
    Returns True if the given Stream exists, False if not.
    Resolves to the latest active table version if no table version is given.
    Resolves to the DeltaCAT stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            namespace=namespace,
            table_name=table_name,
            fail_if_no_active_table_version=False,
            transaction=transaction,
            *args,
            **kwargs,
        )

    # Try with the provided table name first
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=None,
        stream_format=stream_format,
    )
    exists = _exists(
        metafile=Stream.of(
            locator=locator,
            partition_scheme=None,
            state=CommitState.COMMITTED,
        ),
        transaction=transaction,
        *args,
        **kwargs,
    )
    if commit_transaction:
        transaction.seal()
    return exists


def stage_partition(
    stream: Stream,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Partition:
    """
    Stages a new partition for the given stream and partition values. Returns
    the staged partition. If this partition will replace another partition
    with the same partition values and scheme, then it will have its previous
    partition ID set to the ID of the partition being replaced. Partition values
    should not be specified for unpartitioned tables.

    The partition_values must represent the results of transforms in a partition
    spec specified in the stream.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    # TODO(pdames): Cache last retrieved metafile revisions in memory to resolve
    #   potentially high cost of staging many partitions.
    table_version = get_table_version(
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not table_version:
        raise TableVersionNotFoundError(
            f"Table version not found: {stream.namespace}.{stream.table_name}."
            f"{stream.table_version}."
        )
    # Set partition_scheme_id to UNPARTITIONED_SCHEME_ID when partition_values
    # is None or empty
    if not partition_values:
        partition_scheme_id = UNPARTITIONED_SCHEME_ID
    # Use stream's partition scheme ID if none provided and partition_values
    # are specified
    elif partition_scheme_id is None:
        partition_scheme_id = stream.partition_scheme.id
    if not table_version.partition_schemes or partition_scheme_id not in [
        ps.id for ps in table_version.partition_schemes
    ]:
        raise TableValidationError(
            f"Invalid partition scheme ID `{partition_scheme_id}` (not found "
            f"in parent table version `{stream.namespace}.{stream.table_name}"
            f".{table_version.table_version}` partition scheme IDs)."
        )
    if stream.partition_scheme.id not in [
        ps.id for ps in table_version.partition_schemes
    ]:
        # this should never happen, but just in case
        raise TableValidationError(
            f"Invalid stream partition scheme ID `{stream.partition_scheme.id}`"
            f" (not found in parent table version "
            f"`{stream.namespace}.{stream.table_name}"
            f".{table_version.table_version}` partition scheme IDs)."
        )

    if partition_values:
        if partition_scheme_id == UNPARTITIONED_SCHEME_ID:
            raise TableValidationError(
                "Partition values cannot be specified for unpartitioned tables"
            )
        # Validate partition values against partition scheme
        partition_scheme = next(
            ps for ps in table_version.partition_schemes if ps.id == partition_scheme_id
        )
        _validate_partition_values_against_scheme(
            partition_values=partition_values,
            partition_scheme=partition_scheme,
            schema=table_version.schema,
        )

    locator = PartitionLocator.of(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_id=str(uuid.uuid4()),
    )
    partition = Partition.of(
        locator=locator,
        content_types=table_version.content_types,
        state=CommitState.STAGED,
        previous_stream_position=None,
        previous_partition_id=None,
        stream_position=None,
        partition_scheme_id=partition_scheme_id,
    )
    prev_partition = get_partition(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    prev_partition_id = prev_partition.partition_id if prev_partition else None

    # TODO(pdames): Check all historic partitions for the same partition ID
    if prev_partition_id == partition.partition_id:
        raise TableValidationError(
            f"Partition to stage has the same ID as previous partition: {prev_partition_id}."
        )
    partition.previous_partition_id = prev_partition_id

    # Add the operation to the transaction
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=partition,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise PartitionAlreadyExistsError(
            f"Partition {stream.namespace}.{stream.table_name}.{stream.table_version}.{partition.partition_id} already exists"
        ) from e

    if commit_transaction:
        transaction.seal()
    return partition


def commit_partition(
    partition: Partition,
    previous_partition: Optional[Partition] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Partition:
    """
    Commits the staged partition to its associated table version stream,
    replacing any previous partition registered for the same stream and
    partition values. All values set on the input partition except compaction
    round completion info will be overwritten with the values stored in the
    staged partition.

    If previous partition is given then it will be replaced with its deltas
    prepended to the new partition being committed. Otherwise the latest
    committed partition with the same keys and partition scheme ID will be
    retrieved.

    Returns the registered partition. If the partition's
    previous delta stream position is specified, then the commit will
    be rejected if it does not match the actual previous stream position of
    the partition being replaced. If the partition's previous partition ID is
    specified, then the commit will be rejected if it does not match the actual
    ID of the partition being replaced.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    if previous_partition:
        raise NotImplementedError(
            f"delta prepending from previous partition {previous_partition} "
            f"is not yet implemented"
        )
    if not partition.partition_id:
        raise ValueError("Partition ID to commit must be set to a staged partition ID.")
    if not partition.stream_locator:
        raise ValueError(
            "Partition to commit must have its stream locator "
            "set to the parent of its staged partition ID."
        )

    # Start a single multi-step transaction for all operations (both read and write)
    # Step 1: Get the staged partition using transaction
    prev_staged_partition = get_partition_by_id(
        stream_locator=partition.stream_locator,
        partition_id=partition.partition_id,
        transaction=transaction,
        *args,
        **kwargs,
    )

    # Validate staged partition
    if not prev_staged_partition:
        raise PartitionNotFoundError(
            f"Partition at stream {partition.stream_locator} with ID "
            f"{partition.partition_id} not found."
        )
    if prev_staged_partition.state != CommitState.STAGED:
        raise TableValidationError(
            f"Expected to find a `{CommitState.STAGED}` partition at stream "
            f"{partition.stream_locator} with ID {partition.partition_id},"
            f"but found a `{prev_staged_partition.state}` partition."
        )

    # Step 2: Check for existing committed partition
    prev_committed_partition = None
    if partition.previous_partition_id is not None:
        prev_committed_partition = get_partition(
            stream_locator=partition.stream_locator,
            partition_values=partition.partition_values,
            partition_scheme_id=partition.partition_scheme_id,
            transaction=transaction,
            *args,
            **kwargs,
        )

    # Validate expected previous partition ID for race condition detection
    if prev_committed_partition:
        logger.info(
            f"Checking previous committed partition for conflicts: {prev_committed_partition}"
        )
        if prev_committed_partition.partition_id != partition.previous_partition_id:
            raise ConcurrentModificationError(
                f"Concurrent modification detected: Expected committed partition "
                f"{partition.previous_partition_id} but found "
                f"{prev_committed_partition.partition_id}."
            )

    if prev_committed_partition:
        # Update transaction type based on what we found
        txn_op_type = TransactionOperationType.REPLACE
        if prev_committed_partition.partition_id == partition.partition_id:
            raise TableValidationError(
                f"Partition to commit has the same ID as existing partition: "
                f"{prev_committed_partition}."
            )
    else:
        txn_op_type = TransactionOperationType.UPDATE

    # Prepare the committed partition based on the staged partition
    # Compaction round completion info (if any) is not set on the staged partition,
    # so we need to save it from the input partition to commit.
    input_partition_rci = partition.compaction_round_completion_info
    partition: Partition = Metafile.update_for(prev_staged_partition)
    partition.state = CommitState.COMMITTED
    # Restore compaction round completion info (if any) from the input partition.
    if input_partition_rci is not None:
        partition.compaction_round_completion_info = input_partition_rci

    # Step 4: Add write operations to the same transaction
    # Always UPDATE the staged partition to committed state
    transaction.step(
        TransactionOperation.of(
            operation_type=txn_op_type,
            dest_metafile=partition,
            src_metafile=prev_staged_partition,
        ),
    )

    # If there's a previously committed partition, we need to replace it too
    if prev_committed_partition:
        transaction.step(
            TransactionOperation.of(
                operation_type=txn_op_type,
                dest_metafile=partition,
                src_metafile=prev_committed_partition,
            ),
        )

    if commit_transaction:
        transaction.seal()

    return partition


def delete_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """
    Deletes the given partition from the specified stream. Partition
    values should not be specified for unpartitioned tables. Raises an error
    if the partition does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    partition_to_delete = get_partition(
        stream_locator=stream_locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        transaction=transaction,
        *args,
        **kwargs,
    )
    if not partition_to_delete:
        raise PartitionNotFoundError(
            f"Partition with values {partition_values} and scheme "
            f"{partition_scheme_id} not found in stream: {stream_locator}"
        )
    else:
        partition_to_delete.state = CommitState.DEPRECATED

    transaction.step(
        TransactionOperation.of(
            operation_type=TransactionOperationType.DELETE,
            dest_metafile=partition_to_delete,
        ),
    )

    if commit_transaction:
        transaction.seal()


def get_partition_by_id(
    stream_locator: StreamLocator,
    partition_id: str,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the partition for the given stream locator and partition ID.
    Returns None if the partition does not exist. Raises an error if the
    given stream locator does not exist.
    """
    locator = PartitionLocator.of(
        stream_locator=stream_locator,
        partition_values=None,
        partition_id=partition_id,
    )
    return _latest(
        metafile=Partition.of(
            locator=locator,
            content_types=None,
        ),
        *args,
        **kwargs,
    )


def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the most recently committed partition for the given stream locator and
    partition key values. Returns None if no partition has been committed for
    the given table version and/or partition key values. Partition values
    should not be specified for unpartitioned tables. Partition scheme ID
    resolves to the table version's current partition scheme by default.
    Raises an error if the given stream locator does not exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    if not partition_scheme_id or not stream_locator.stream_id:
        # resolve latest partition scheme from the current
        # revision of its `deltacat` stream
        stream = get_stream(
            namespace=stream_locator.namespace,
            table_name=stream_locator.table_name,
            table_version=stream_locator.table_version,
            transaction=transaction,
            *args,
            **kwargs,
        )
        if not stream:
            raise StreamNotFoundError(f"Stream {stream_locator} not found.")
        partition_scheme_id = stream.partition_scheme.id
        # ensure that we always use a fully qualified stream locator
        stream_locator = stream.locator
    locator = PartitionLocator.of(
        stream_locator=stream_locator,
        partition_values=partition_values,
        partition_id=None,
    )
    partition = _latest(
        metafile=Partition.of(
            locator=locator,
            content_types=None,
            state=CommitState.COMMITTED,
            partition_scheme_id=partition_scheme_id,
        ),
        transaction=transaction,
        *args,
        **kwargs,
    )
    if commit_transaction:
        transaction.seal()
    return partition


def _write_table_slices(
    table: Union[LocalTable, LocalDataset, DistributedDataset],
    partition_id: str,
    max_records_per_entry: Optional[int],
    table_writer_fn: Callable,
    table_slicer_fn: Callable,
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
    table_writer_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> ManifestEntryList:
    catalog_properties = get_catalog_properties(**kwargs)
    manifest_entries = ManifestEntryList()
    # LocalDataset is a special case to upload iteratively
    tables = [t for t in table] if isinstance(table, list) else [table]
    filesystem = catalog_properties.filesystem
    data_dir_path = posixpath.join(
        catalog_properties.root,
        DATA_FILE_DIR_NAME,
        partition_id,
    )
    filesystem.create_dir(data_dir_path, recursive=True)
    # Add catalog properties to table writer kwargs for manifest path relativization
    table_writer_kwargs["catalog_properties"] = catalog_properties
    for t in tables:
        manifest_entries.extend(
            write_sliced_table(
                t,
                data_dir_path,
                filesystem,
                max_records_per_entry,
                table_writer_fn,
                table_slicer_fn,
                table_writer_kwargs,
                content_type,
                entry_params,
                entry_type,
            )
        )
    return manifest_entries


def _write_table(
    partition_id: str,
    table: Union[LocalTable, LocalDataset, DistributedDataset],
    max_records_per_entry: Optional[int] = None,
    author: Optional[ManifestAuthor] = None,
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
    write_table_slices_fn: Optional[Callable] = _write_table_slices,
    table_writer_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Manifest:
    """
    Writes the given table to 1 or more files and returns a
    Redshift manifest pointing to the uploaded files.
    """
    table_writer_fn = get_table_writer(table)
    table_slicer_fn = get_table_slicer(table)

    manifest_entries = write_table_slices_fn(
        table,
        partition_id,
        max_records_per_entry,
        table_writer_fn,
        table_slicer_fn,
        content_type,
        entry_params,
        entry_type,
        table_writer_kwargs,
        **kwargs,
    )
    manifest = Manifest.of(
        entries=manifest_entries,
        author=author,
        uuid=str(uuid.uuid4()),
        entry_type=entry_type,
        entry_params=entry_params,
    )
    return manifest


def stage_delta(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    partition: Partition,
    delta_type: DeltaType = DeltaType.UPSERT,
    max_records_per_entry: Optional[int] = None,
    author: Optional[ManifestAuthor] = None,
    properties: Optional[DeltaProperties] = None,
    table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
    write_table_slices_fn: Optional[Callable] = _write_table_slices,
    schema: Optional[Schema] = None,
    sort_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> Delta:
    """
    Writes the given dataset to 1 or more files. Returns an unregistered
    delta whose manifest entries point to the uploaded files. Applies any
    schema consistency policies configured for the parent table version.
    """
    # TODO(pdames): Validate that equality delete entry types either have
    #  entry params specified, or are being added to a table with merge keys.
    if not partition.is_supported_content_type(content_type):
        raise TableValidationError(
            f"Content type {content_type} is not supported by "
            f"partition: {partition}"
        )
    if partition.state == CommitState.DEPRECATED:
        raise TableValidationError(
            f"Cannot stage delta to {partition.state} partition: {partition}",
        )
    previous_stream_position: Optional[int] = (
        partition.stream_position if delta_type != DeltaType.ADD else None
    )

    # Handle schema parameter and add to table_writer_kwargs if available
    table_writer_kwargs = table_writer_kwargs or {}

    # Extract schema_id from the schema if it's a DeltaCAT Schema
    schema_id = None
    if isinstance(schema, Schema):
        schema_id = schema.id
        table_writer_kwargs["schema_id"] = schema_id
        # Add PyArrow schema to table_writer_kwargs if not already present
        if "schema" not in table_writer_kwargs:
            table_writer_kwargs["schema"] = schema.arrow
    elif schema is not None and "schema" not in table_writer_kwargs:
        # For PyArrow schemas or other types, add directly
        table_writer_kwargs["schema"] = schema

    # Add sort_scheme_id to table_writer_kwargs for manifest entry creation
    if sort_scheme_id is not None:
        table_writer_kwargs["sort_scheme_id"] = sort_scheme_id

    manifest: Manifest = _write_table(
        partition.partition_id,
        data,
        max_records_per_entry,
        author,
        content_type,
        entry_params,
        entry_type,
        write_table_slices_fn,
        table_writer_kwargs,
        **kwargs,
    )
    staged_delta: Delta = Delta.of(
        locator=DeltaLocator.of(partition.locator, None),
        delta_type=delta_type,
        meta=manifest.meta,
        properties=properties,
        manifest=manifest,
        previous_stream_position=previous_stream_position,
    )
    return staged_delta


def commit_delta(
    delta: Delta,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Delta:
    """
    Registers a new delta with its associated target table version and
    partition. Returns the registered delta. If the delta's previous stream
    position is specified, then the commit will be rejected if it does not match
    the target partition's actual previous stream position. If the delta's
    stream position is specified, it must be greater than the latest stream
    position in the target partition.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)

    delta: Delta = Metafile.update_for(delta)
    delta_type: Optional[DeltaType] = delta.type
    resolved_delta_type = delta_type if delta_type is not None else DeltaType.UPSERT
    delta.type = resolved_delta_type
    delta.properties = kwargs.get("properties") or delta.properties
    new_parent_partition: Optional[Partition] = None
    # resolve the parent partition
    if delta.partition_id:
        parent_partition = get_partition_by_id(
            stream_locator=delta.stream_locator,
            partition_id=delta.partition_id,
            transaction=transaction,
            *args,
            **kwargs,
        )
    else:
        parent_partition = get_partition(
            stream_locator=delta.stream_locator,
            partition_values=delta.partition_values,
            transaction=transaction,
            *args,
            **kwargs,
        )
    if not parent_partition:
        raise PartitionNotFoundError(f"Partition not found: {delta.locator}")
    # ensure that we always use a fully qualified partition locator
    delta.locator.partition_locator = parent_partition.locator

    # resolve the delta's stream position
    if delta.type != DeltaType.ADD:
        # this is an ordered delta
        delta.previous_stream_position = parent_partition.stream_position or 0
        if delta.stream_position is not None:
            if delta.stream_position <= delta.previous_stream_position:
                # manually specified delta stream positions must be greater than the
                # previous stream position
                raise TableValidationError(
                    f"Delta stream position {delta.stream_position} must be "
                    f"greater than previous stream position "
                    f"{delta.previous_stream_position}"
                )
        else:
            delta.locator.stream_position = delta.previous_stream_position + 1
        # update the parent partition's stream position
        new_parent_partition: Partition = Metafile.update_for(parent_partition)
        new_parent_partition.stream_position = delta.locator.stream_position
    else:
        # this is an unordered delta - use a positive signed 64-bit UUID-based stream position
        delta.locator.stream_position = uuid.uuid4().int & (1 << 63) - 1
        # reserve stream positions <= UINT32_MAX for ordered deltas
        while delta.locator.stream_position <= UNSIGNED_INT32_MAX_VALUE:
            delta.locator.stream_position = uuid.uuid4().int & (1 << 63) - 1

    # Add operations to the transaction
    # the 1st operation creates the delta
    try:
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=delta,
            ),
        )
    except ObjectAlreadyExistsError as e:
        raise DeltaAlreadyExistsError(f"Delta {delta.locator} already exists") from e
    if new_parent_partition:
        # For ordered APPEND deltas,,
        # the 2nd operation alters the latest ordered stream position of the partition
        transaction.step(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_parent_partition,
                src_metafile=parent_partition,
            ),
        )

    if commit_transaction:
        transaction.seal()
    return delta


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """
    Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist.
    """
    return _latest(
        metafile=Namespace.of(NamespaceLocator.of(namespace)),
        *args,
        **kwargs,
    )


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    return _exists(
        metafile=Namespace.of(NamespaceLocator.of(namespace)),
        *args,
        **kwargs,
    )


def get_table(
    namespace: str,
    table_name: str,
    *args,
    **kwargs,
) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if the given
    table does not exist.
    """
    locator = TableLocator.at(namespace=namespace, table_name=table_name)
    return _latest(
        metafile=Table.of(locator=locator),
        *args,
        **kwargs,
    )


def table_exists(
    namespace: str,
    table_name: str,
    *args,
    **kwargs,
) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    locator = TableLocator.at(namespace=namespace, table_name=table_name)
    return _exists(
        metafile=Table.of(locator=locator),
        *args,
        **kwargs,
    )


def get_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the specified table version. Returns None
    if the given table version does not exist.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    return _latest(
        metafile=table_version,
        *args,
        **kwargs,
    )


def get_latest_table_version(
    namespace: str,
    table_name: str,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest version of the specified table.
    Returns None if no table version exists for the given table. Raises
    an error if the given table doesn't exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    table_version_id = _resolve_latest_table_version_id(
        namespace=namespace,
        table_name=table_name,
        fail_if_no_active_table_version=False,
        transaction=transaction,
        *args,
        **kwargs,
    )

    table_version = (
        get_table_version(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version_id,
            transaction=transaction,
            *args,
            **kwargs,
        )
        if table_version_id
        else None
    )
    if commit_transaction:
        transaction.seal()
    return table_version


def get_latest_active_table_version(
    namespace: str,
    table_name: str,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest active version of the specified
    table. Returns None if no active table version exists for the given table.
    Raises an error if the given table doesn't exist.
    """
    transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    table_version_id = _resolve_latest_active_table_version_id(
        namespace=namespace,
        table_name=table_name,
        fail_if_no_active_table_version=False,
        transaction=transaction,
        *args,
        **kwargs,
    )
    table_version = (
        get_table_version(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version_id,
            transaction=transaction,
            *args,
            **kwargs,
        )
        if table_version_id
        else None
    )
    if commit_transaction:
        transaction.seal()
    return table_version


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
    schema = get_table_version_schema(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        *args,
        **kwargs,
    )
    return schema.arrow.names if schema else None


def get_table_version_schema(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Schema]:
    """
    Gets the schema for the specified table version, or for the latest active
    table version if none is specified. Returns None if the table version is
    schemaless. Raises an error if the table version does not exist.
    """
    table_version_meta = (
        get_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            **kwargs,
        )
        if table_version
        else get_latest_active_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            **kwargs,
        )
    )
    return table_version_meta.schema


def table_version_exists(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> bool:
    """
    Returns True if the given table version exists, False if not.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    return _exists(
        *args,
        metafile=table_version,
        **kwargs,
    )


def can_categorize(e: BaseException, *args, **kwargs) -> bool:
    """
    True if the input error originated from the storage
    implementation layer and can be categorized under an
    existing DeltaCatError. The "categorize_errors" decorator
    uses this to determine if an unknown error from the storage
    implementation can be categorized prior to casting it to
    the equivalent DeltaCatError via `raise_categorized_error`
    """

    # DeltaCAT native storage can only categorize DeltaCatError
    # (i.e., this is effectively a no-op for native storage)
    if isinstance(e, DeltaCatError):
        return True
    else:
        return False


def raise_categorized_error(e: BaseException, *args, **kwargs):
    """
    Casts a categorizable error that originaed from the storage
    implementation layer to its equivalent DeltaCatError
    for uniform handling (e.g., determining whether an error
    is retryable or not) via the "categorize_errors" decorator.
    Raises an UnclassifiedDeltaCatError from the input exception
    if the error cannot be categorized.
    """

    # DeltaCAT native storage can only categorize DeltaCatError
    # (i.e., this is effectively a no-op for native storage)
    logger.info(f"Categorizing exception: {e}")
    categorized = None
    if isinstance(categorized, DeltaCatError):
        raise categorized from e

    logger.warning(f"Could not classify {type(e).__name__}: {e}")
    raise UnclassifiedDeltaCatError(
        f"Failed to classify error {type(e).__name__}: {e}"
    ) from e
