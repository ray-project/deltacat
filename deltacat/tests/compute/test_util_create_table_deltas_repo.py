# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
import pyarrow as pa

from deltacat.tests.compute.test_util_common import (
    PartitionKey,
)

from deltacat.storage import (
    Delta,
    DeltaType,
    Partition,
    PartitionLocator,
    Stream,
)
from deltacat.tests.compute.test_util_common import (
    create_src_table,
    create_destination_table,
    create_rebase_table,
)


def create_incremental_deltas_on_source_table(
    source_namespace: str,
    source_table_name: str,
    source_table_version: str,
    source_table_stream: Stream,
    partition_values_param,
    incremental_deltas: pa.Table,
    incremental_delta_type: DeltaType,
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[PartitionLocator, Delta]:
    import deltacat.tests.local_deltacat_storage as ds

    src_partition: Partition = ds.get_partition(
        source_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    new_delta: Delta = ds.commit_delta(
        ds.stage_delta(
            incremental_deltas, src_partition, incremental_delta_type, **ds_mock_kwargs
        ),
        **ds_mock_kwargs,
    )
    src_table_stream_after_committed_delta: Stream = ds.get_stream(
        source_namespace,
        source_table_name,
        source_table_version,
        **ds_mock_kwargs,
    )
    src_partition_after_committed_delta: Partition = ds.get_partition(
        src_table_stream_after_committed_delta.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    return src_partition_after_committed_delta.locator, new_delta


def create_src_w_deltas_destination_plus_destination(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: pa.Table,
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream]]:
    import deltacat.tests.local_deltacat_storage as ds

    source_namespace, source_table_name, source_table_version = create_src_table(
        primary_keys, sort_keys, partition_keys, ds_mock_kwargs
    )

    source_table_stream: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    staged_partition: Partition = ds.stage_partition(
        source_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(
            input_deltas, staged_partition, input_delta_type, **ds_mock_kwargs
        ),
        **ds_mock_kwargs,
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    (
        destination_table_namespace,
        destination_table_name,
        destination_table_version,
    ) = create_destination_table(
        primary_keys, sort_keys, partition_keys, ds_mock_kwargs
    )
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    return source_table_stream_after_committed, destination_table_stream, None


def create_src_w_deltas_destination_rebase_w_deltas_strategy(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: pa.Table,
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream]]:
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.storage import Partition, Stream

    source_namespace, source_table_name, source_table_version = create_src_table(
        primary_keys, sort_keys, partition_keys, ds_mock_kwargs
    )

    source_table_stream: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    staged_partition: Partition = ds.stage_partition(
        source_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(
            input_deltas, staged_partition, input_delta_type, **ds_mock_kwargs
        ),
        **ds_mock_kwargs,
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    # create the destination table
    (
        destination_table_namespace,
        destination_table_name,
        destination_table_version,
    ) = create_destination_table(
        primary_keys, sort_keys, partition_keys, ds_mock_kwargs
    )
    # create the rebase table
    (
        rebase_table_namespace,
        rebase_table_name,
        rebase_table_version,
    ) = create_rebase_table(primary_keys, sort_keys, partition_keys, ds_mock_kwargs)
    rebasing_table_stream: Stream = ds.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    staged_partition: Partition = ds.stage_partition(
        rebasing_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(input_deltas, staged_partition, **ds_mock_kwargs),
        **ds_mock_kwargs,
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)

    # get streams
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    rebased_stream_after_committed: Stream = ds.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    return (
        source_table_stream_after_committed,
        destination_table_stream,
        rebased_stream_after_committed,
    )
