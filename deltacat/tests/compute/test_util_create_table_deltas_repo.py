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
import logging
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _add_deltas_to_partition(
    deltas_ingredients: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    partition: Optional[Partition],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> List[Optional[Delta], int]:
    import deltacat.tests.local_deltacat_storage as ds

    all_deltas_length = 0
    for (delta_data, delta_type, delete_parameters) in deltas_ingredients:
        staged_delta: Delta = ds.stage_delta(
            delta_data,
            partition,
            delta_type,
            delete_parameters=delete_parameters,
            **ds_mock_kwargs,
        )
        incremental_delta = ds.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        all_deltas_length += len(delta_data) if delta_data else 0
    return incremental_delta, all_deltas_length


def add_late_deltas_to_partition(
    late_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    source_partition: Optional[Partition],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> List[Optional[Delta], int]:
    return _add_deltas_to_partition(late_deltas, source_partition, ds_mock_kwargs)


def create_incremental_deltas_on_source_table(
    source_namespace: str,
    source_table_name: str,
    source_table_version: str,
    source_table_stream: Stream,
    partition_values_param,
    incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[PartitionLocator, Delta, int, bool]:
    import deltacat.tests.local_deltacat_storage as ds

    incremental_delta_length = 0
    is_delete = False
    src_partition: Partition = ds.get_partition(
        source_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    for (
        incremental_data,
        incremental_delta_type,
        incremental_delete_parameters,
    ) in incremental_deltas:
        if incremental_delta_type is DeltaType.DELETE:
            is_delete = True
        incremental_delta: Delta = ds.commit_delta(
            ds.stage_delta(
                incremental_data,
                src_partition,
                incremental_delta_type,
                delete_parameters=incremental_delete_parameters,
                **ds_mock_kwargs,
            ),
            **ds_mock_kwargs,
        )
        incremental_delta_length += len(incremental_data) if incremental_data else 0
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
    return (
        src_partition_after_committed_delta.locator,
        incremental_delta,
        incremental_delta_length,
        is_delete,
    )


def create_src_w_deltas_destination_plus_destination(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: pa.Table,
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
    simulate_is_inplace: bool = False,
) -> Tuple[Stream, Stream, Optional[Stream], str, str, str]:
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
    destination_table_namespace: Optional[str] = None
    destination_table_name: Optional[str] = None
    destination_table_version: Optional[str] = None
    if not simulate_is_inplace:
        (
            destination_table_namespace,
            destination_table_name,
            destination_table_version,
        ) = create_destination_table(
            primary_keys, sort_keys, partition_keys, ds_mock_kwargs
        )
    else:
        # not creating a table as in-place
        destination_table_namespace = source_namespace
        destination_table_name = source_table_name
        destination_table_version = source_table_version

    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    return (
        source_table_stream_after_committed,
        destination_table_stream,
        None,
        source_namespace,
        source_table_name,
        source_table_version,
    )


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
    from deltacat.storage import Delta
    from deltacat.utils.common import current_time_ms

    last_stream_position = current_time_ms()
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
    staged_delta: Delta = ds.stage_delta(
        input_deltas, staged_partition, input_delta_type, **ds_mock_kwargs
    )
    staged_delta.locator.stream_position = last_stream_position
    ds.commit_delta(
        staged_delta,
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
    staged_delta: Delta = ds.stage_delta(
        input_deltas, staged_partition, **ds_mock_kwargs
    )
    staged_delta.locator.stream_position = last_stream_position
    ds.commit_delta(
        staged_delta,
        **ds_mock_kwargs,
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)

    # get streams
    # TODO: Add deltas to destination stream
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


def multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream], bool]:
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
    is_delete = False
    input_delta_length = 0
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        if input_delta_type is DeltaType.DELETE:
            is_delete = True
        staged_delta = ds.stage_delta(
            input_delta,
            staged_partition,
            input_delta_type,
            delete_parameters=input_delta_parameters,
            **ds_mock_kwargs,
        )
        ds.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        input_delta_length += len(input_delta) if input_delta else 0
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
    input_delta_length = 0
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        if input_delta_type is DeltaType.DELETE:
            is_delete = True
        staged_delta = ds.stage_delta(
            input_delta,
            staged_partition,
            input_delta_type,
            delete_parameters=input_delta_parameters,
            **ds_mock_kwargs,
        )
        ds.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        input_delta_length += len(input_delta) if input_delta else 0
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
        is_delete,
    )
