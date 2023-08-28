# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
import pyarrow as pa

from deltacat.tests.compute.test_util_constant import (
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    RAY_COMPACTED_NAMESPACE,
    RAY_COMPACTED_NAME_SUFFIX,
    REBASING_NAMESPACE,
    REBASING_NAME_SUFFIX,
)
from deltacat.tests.compute.test_util_common import (
    PartitionKey,
)

from deltacat.storage import (
    DeltaType,
)


def _create_empty_destination_table(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    column_names: List[str],
    arrow_arrays: List[pa.Array],
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Stream

    destination_namespace: str = BASE_TEST_DESTINATION_NAMESPACE
    destination_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    destination_table_version: str = BASE_TEST_DESTINATION_TABLE_VERSION

    ds.create_namespace(destination_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    return destination_table_stream


def _create_base_src_table_w_deltas(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    column_names: List[str],
    input_deltas: List[pa.Array],
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Partition, Stream

    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version: str = BASE_TEST_SOURCE_TABLE_VERSION

    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version: str = BASE_TEST_SOURCE_TABLE_VERSION

    ds.create_namespace(source_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        source_namespace,
        source_table_name,
        source_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    source_table_stream: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    test_table: pa.Table = pa.Table.from_arrays(input_deltas, names=column_names)
    staged_partition: Partition = ds.stage_partition(
        source_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    return source_table_stream_after_committed


def create_src_w_deltas_destination_strategy(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    column_names: List[str],
    input_deltas: List[pa.Array],
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Any, Any, Any]:
    from deltacat.storage import Stream

    source_table_stream_after_committed: Stream = _create_base_src_table_w_deltas(
        primary_keys,
        sort_keys,
        partition_keys,
        column_names,
        input_deltas,
        input_delta_type,
        partition_values,
        ds_mock_kwargs,
    )

    destination_table_stream: Stream = _create_empty_destination_table(
        primary_keys,
        sort_keys,
        partition_keys,
        column_names,
        input_deltas,
        partition_values,
        ds_mock_kwargs,
    )
    return source_table_stream_after_committed, destination_table_stream, None


def create_src_w_deltas_destination_rebase_w_deltas_strategy(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    column_names: List[str],
    input_deltas: List[pa.Array],
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Partition, Stream

    destination_namespace = RAY_COMPACTED_NAMESPACE
    destination_table_name = (
        BASE_TEST_DESTINATION_TABLE_NAME + RAY_COMPACTED_NAME_SUFFIX
    )
    destination_table_version = BASE_TEST_DESTINATION_TABLE_VERSION

    rebasing_namespace = REBASING_NAMESPACE
    rebasing_table_name = BASE_TEST_SOURCE_TABLE_NAME + REBASING_NAME_SUFFIX
    rebasing_table_version = BASE_TEST_SOURCE_TABLE_VERSION

    # create source table
    source_table_stream_after_committed: Stream = _create_base_src_table_w_deltas(
        primary_keys,
        sort_keys,
        partition_keys,
        column_names,
        input_deltas,
        input_delta_type,
        partition_values,
        ds_mock_kwargs,
    )

    # create destination table
    ds.create_namespace(destination_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )

    # create the rebase table
    ds.create_namespace(rebasing_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        rebasing_namespace,
        rebasing_table_name,
        rebasing_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    rebasing_table_stream: Stream = ds.get_stream(
        namespace=rebasing_namespace,
        table_name=rebasing_table_name,
        table_version=rebasing_table_version,
        **ds_mock_kwargs,
    )
    test_table: pa.Table = pa.Table.from_arrays(input_deltas, names=column_names)
    staged_partition: Partition = ds.stage_partition(
        rebasing_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)

    # get streams
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    rebased_stream_after_committed: Stream = ds.get_stream(
        namespace=rebasing_namespace,
        table_name=rebasing_table_name,
        table_version=rebasing_table_version,
        **ds_mock_kwargs,
    )
    return (
        source_table_stream_after_committed,
        destination_table_stream,
        rebased_stream_after_committed,
    )
