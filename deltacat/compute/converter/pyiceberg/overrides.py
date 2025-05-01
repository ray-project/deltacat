from collections import defaultdict
import logging
from deltacat import logs
import pyarrow.parquet as pq

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def parquet_files_dict_to_iceberg_data_files(io, table_metadata, files_dict_list):
    from pyiceberg.io.pyarrow import (
        _check_pyarrow_schema_compatible,
        data_file_statistics_from_parquet_metadata,
        compute_statistics_plan,
        parquet_path_to_id_mapping,
    )
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )

    data_file_content_type = DataFileContent.POSITION_DELETES
    iceberg_files = []
    schema = table_metadata.schema()
    for files_dict in files_dict_list:
        for partition_value, file_paths in files_dict.items():
            for file_path in file_paths:
                input_file = io.new_input(file_path)
                with input_file.open() as input_stream:
                    parquet_metadata = pq.read_metadata(input_stream)
                _check_pyarrow_schema_compatible(
                    schema, parquet_metadata.schema.to_arrow_schema()
                )

                statistics = data_file_statistics_from_parquet_metadata(
                    parquet_metadata=parquet_metadata,
                    stats_columns=compute_statistics_plan(
                        schema, table_metadata.properties
                    ),
                    parquet_column_mapping=parquet_path_to_id_mapping(schema),
                )

                data_file = DataFile(
                    content=data_file_content_type,
                    file_path=file_path,
                    file_format=FileFormat.PARQUET,
                    partition=partition_value,
                    # partition=Record(**{"pk": "111", "bucket": 2}),
                    file_size_in_bytes=len(input_file),
                    sort_order_id=None,
                    spec_id=table_metadata.default_spec_id,
                    equality_ids=None,
                    key_metadata=None,
                    **statistics.to_serialized_dict(),
                )
                iceberg_files.append(data_file)
    return iceberg_files


def fetch_all_bucket_files(table):
    # step 1: filter manifests using partition summaries
    # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id
    from pyiceberg.typedef import (
        KeyDefaultDict,
    )

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
    from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
    from pyiceberg.types import (
        strtobool,
    )
    from pyiceberg.table import _min_sequence_number, _open_manifest
    from pyiceberg.utils.concurrent import ExecutorFactory
    from itertools import chain
    from pyiceberg.manifest import DataFileContent

    partition_evaluators = KeyDefaultDict(data_scan._build_partition_evaluator)
    metrics_evaluator = _InclusiveMetricsEvaluator(
        data_scan.table_metadata.schema(),
        data_scan.row_filter,
        data_scan.case_sensitive,
        strtobool(data_scan.options.get("include_empty_files", "false")),
    ).eval

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
                    metrics_evaluator,
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
        if data_file.content == DataFileContent.POSITION_DELETES:
            positional_delete_entries[partition_value].append(data_file_tuple)
        elif data_file.content == DataFileContent.EQUALITY_DELETES:
            equality_data_entries[partition_value].append(data_file_tuple)
        else:
            logger.warning(
                f"Unknown DataFileContent ({data_file.content}): {manifest_entry}"
            )
    return data_entries, equality_data_entries, positional_delete_entries
