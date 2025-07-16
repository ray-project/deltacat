from collections import defaultdict
import logging
from deltacat import logs
import pyarrow
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import (
    parquet_path_to_id_mapping,
    StatisticsCollector,
    MetricModeTypes,
    DataFileStatistics,
    MetricsMode,
    StatsAggregator,
)
from typing import Dict, List, Set, Any, Tuple
from deltacat.compute.converter.utils.iceberg_columns import (
    ICEBERG_RESERVED_FIELD_ID_FOR_FILE_PATH_COLUMN,
    ICEBERG_RESERVED_FIELD_ID_FOR_POS_COLUMN,
)
from pyiceberg.io.pyarrow import (
    compute_statistics_plan,
)
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
)
from pyiceberg.table import _min_sequence_number, _open_manifest, Table
from pyiceberg.utils.concurrent import ExecutorFactory
from itertools import chain
from pyiceberg.typedef import (
    KeyDefaultDict,
)
from pyiceberg.schema import Schema
from pyiceberg.io import FileIO
from deltacat.compute.converter.model.convert_input_files import (
    DataFileList,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def parquet_path_to_id_mapping_override(schema: Schema) -> Dict[str, int]:
    res = parquet_path_to_id_mapping(schema)
    # Override here to insert position delete reserved column field IDs
    res["file_path"] = ICEBERG_RESERVED_FIELD_ID_FOR_FILE_PATH_COLUMN
    res["pos"] = ICEBERG_RESERVED_FIELD_ID_FOR_POS_COLUMN
    return res


def data_file_statistics_from_parquet_metadata(
    parquet_metadata: pq.FileMetaData,
    stats_columns: Dict[int, StatisticsCollector],
    parquet_column_mapping: Dict[str, int],
) -> DataFileStatistics:
    """
    Overrides original Pyiceberg function: Compute and return DataFileStatistics that includes the following.

    - record_count
    - column_sizes
    - value_counts
    - null_value_counts
    - nan_value_counts
    - column_aggregates
    - split_offsets

    Args:
        parquet_metadata (pyarrow.parquet.FileMetaData): A pyarrow metadata object.
        stats_columns (Dict[int, StatisticsCollector]): The statistics gathering plan. It is required to
            set the mode for column metrics collection
        parquet_column_mapping (Dict[str, int]): The mapping of the parquet file name to the field ID
    """
    column_sizes: Dict[int, int] = {}
    value_counts: Dict[int, int] = {}
    split_offsets: List[int] = []

    null_value_counts: Dict[int, int] = {}
    nan_value_counts: Dict[int, int] = {}

    col_aggs = {}

    invalidate_col: Set[int] = set()
    for r in range(parquet_metadata.num_row_groups):
        # References:
        # https://github.com/apache/iceberg/blob/fc381a81a1fdb8f51a0637ca27cd30673bd7aad3/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L232
        # https://github.com/apache/parquet-mr/blob/ac29db4611f86a07cc6877b416aa4b183e09b353/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/metadata/ColumnChunkMetaData.java#L184

        row_group = parquet_metadata.row_group(r)

        data_offset = row_group.column(0).data_page_offset
        dictionary_offset = row_group.column(0).dictionary_page_offset

        if row_group.column(0).has_dictionary_page and dictionary_offset < data_offset:
            split_offsets.append(dictionary_offset)
        else:
            split_offsets.append(data_offset)

        for pos in range(parquet_metadata.num_columns):
            column = row_group.column(pos)
            field_id = parquet_column_mapping[column.path_in_schema]
            if field_id in stats_columns:
                stats_col = stats_columns[field_id]

                column_sizes.setdefault(field_id, 0)
                column_sizes[field_id] += column.total_compressed_size

                if stats_col.mode == MetricsMode(MetricModeTypes.NONE):
                    continue

                value_counts[field_id] = (
                    value_counts.get(field_id, 0) + column.num_values
                )

                if column.is_stats_set:
                    try:
                        statistics = column.statistics

                        if statistics.has_null_count:
                            null_value_counts[field_id] = (
                                null_value_counts.get(field_id, 0)
                                + statistics.null_count
                            )

                        if stats_col.mode == MetricsMode(MetricModeTypes.COUNTS):
                            continue

                        if field_id not in col_aggs:
                            col_aggs[field_id] = StatsAggregator(
                                stats_col.iceberg_type,
                                statistics.physical_type,
                                stats_col.mode.length,
                            )

                        col_aggs[field_id].update_min(statistics.min)
                        col_aggs[field_id].update_max(statistics.max)

                    except pyarrow.lib.ArrowNotImplementedError as e:
                        invalidate_col.add(field_id)
                        logger.warning(e)
            else:
                # Note: Removed original adding columns without stats to invalid column logic here
                logger.warning(
                    "PyArrow statistics missing for column %d when writing file", pos
                )

    split_offsets.sort()

    for field_id in invalidate_col:
        del col_aggs[field_id]
        del null_value_counts[field_id]

    return DataFileStatistics(
        record_count=parquet_metadata.num_rows,
        column_sizes=column_sizes,
        value_counts=value_counts,
        null_value_counts=null_value_counts,
        nan_value_counts=nan_value_counts,
        column_aggregates=col_aggs,
        split_offsets=split_offsets,
    )


def parquet_files_dict_to_iceberg_data_files(
    io: FileIO,
    table_metadata: Any,
    files_dict: Dict[Any, List[str]],
    file_content_type: DataFileContent,
) -> List[DataFile]:
    iceberg_files = []
    schema = table_metadata.schema()
    for partition_value, file_paths in files_dict.items():
        for file_path in file_paths:
            input_file = io.new_input(file_path)
            with input_file.open() as input_stream:
                parquet_metadata = pq.read_metadata(input_stream)

            # Removed _check_pyarrow_schema_compatible() here since reserved columns does not comply to all rules.

            statistics = data_file_statistics_from_parquet_metadata(
                parquet_metadata=parquet_metadata,
                stats_columns=compute_statistics_plan(
                    schema, table_metadata.properties
                ),
                parquet_column_mapping=parquet_path_to_id_mapping_override(schema),
            )

            data_file = DataFile(
                content=file_content_type,
                file_path=file_path,
                file_format=FileFormat.PARQUET,
                partition=partition_value,
                file_size_in_bytes=len(input_file),
                sort_order_id=None,
                spec_id=table_metadata.default_spec_id,
                equality_ids=None,
                key_metadata=None,
                **statistics.to_serialized_dict(),
            )
            iceberg_files.append(data_file)
    return iceberg_files


def fetch_all_bucket_files(
    table: Table,
) -> Tuple[Dict[Any, DataFileList], Dict[Any, DataFileList], Dict[Any, DataFileList]]:
    # step 1: filter manifests using partition summaries
    # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id
    data_scan = table.scan()
    snapshot = data_scan.snapshot()
    if not snapshot:
        return iter([])
    manifest_evaluators = KeyDefaultDict(data_scan._build_manifest_evaluator)

    manifests = [
        manifest_file
        for manifest_file in snapshot.manifests(data_scan.io)
        if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
    ]

    # step 2: filter the data files in each manifest
    # this filter depends on the partition spec used to write the manifest file
    partition_evaluators = KeyDefaultDict(data_scan._build_partition_evaluator)
    residual_evaluators = KeyDefaultDict(data_scan._build_residual_evaluator)
    min_sequence_number = _min_sequence_number(manifests)

    # {"bucket_index": List[DataFile]}
    data_entries = defaultdict(list)
    equality_data_entries = defaultdict(list)
    positional_delete_entries = defaultdict(list)

    executor = ExecutorFactory.get_or_create()
    for manifest_entry in chain(
        *executor.map(
            lambda args: _open_manifest(*args),
            [
                (
                    data_scan.io,
                    manifest,
                    partition_evaluators[manifest.partition_spec_id],
                    residual_evaluators[manifest.partition_spec_id],
                    data_scan._build_metrics_evaluator(),
                )
                for manifest in manifests
                if data_scan._check_sequence_number(min_sequence_number, manifest)
            ],
        )
    ):
        data_file = manifest_entry.data_file
        file_sequence_number = manifest_entry.sequence_number
        data_file_tuple = (file_sequence_number, data_file)
        partition_value = data_file.partition

        if data_file.content == DataFileContent.DATA:
            data_entries[partition_value].append(data_file_tuple)
        elif data_file.content == DataFileContent.POSITION_DELETES:
            positional_delete_entries[partition_value].append(data_file_tuple)
        elif data_file.content == DataFileContent.EQUALITY_DELETES:
            equality_data_entries[partition_value].append(data_file_tuple)
        else:
            logger.warning(
                f"Unknown DataFileContent ({data_file.content}): {manifest_entry}"
            )
    return data_entries, equality_data_entries, positional_delete_entries
