from typing import List, Optional, Union, Dict, Any
import pyarrow as pa
from deltacat.storage import Delta, Partition, PartitionLocator, DeltaLocator, metastore, Schema
from deltacat.types.media import StorageType, ContentType


def create_delta_from_csv_file(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    table_version: int = 1,
    content_type: ContentType = ContentType.PARQUET,
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
    *args,
    **kwargs,
) -> Delta:
    if ds_mock_kwargs is None:
        ds_mock_kwargs = {}
    
    staged_partition = stage_partition_from_file_paths(
        namespace,
        file_paths,
        *args,
        table_name=table_name,
        table_version=table_version,
        ds_mock_kwargs=ds_mock_kwargs,
        **kwargs,
    )
    committed_delta = commit_delta_to_staged_partition(
        staged_partition, file_paths, content_type=content_type, ds_mock_kwargs=ds_mock_kwargs, *args, **kwargs
    )
    return committed_delta


def stage_partition_from_file_paths(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    table_version: int = 1,
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
    *args,
    **kwargs,
) -> Partition:
    if ds_mock_kwargs is None:
        ds_mock_kwargs = {}
    
    metastore.create_namespace(namespace=namespace, **ds_mock_kwargs)
    if table_name is None:
        table_name = "-".join(file_paths).replace("/", "_")
    
    # Read a sample file to get the schema
    sample_table = pa.csv.read_csv(file_paths[0])
    pa_schema = sample_table.schema
    # Convert PyArrow schema to DeltaCat Schema
    schema = Schema.of(pa_schema)
    
    table, table_version, stream = metastore.create_table_version(
        namespace=namespace, 
        table_name=table_name, 
        table_version=str(table_version), 
        schema=schema,
        *args,
        **ds_mock_kwargs,
        **kwargs,
    )
    staged_partition = metastore.stage_partition(stream, [], **ds_mock_kwargs)
    return staged_partition


def commit_delta_to_staged_partition(
    staged_partition,
    file_paths: List[str] = None,
    pa_table: pa.Table = None,
    content_type: ContentType = ContentType.PARQUET,
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
    *args,
    **kwargs,
) -> Delta:
    if ds_mock_kwargs is None:
        ds_mock_kwargs = {}
    
    committed_delta = commit_delta_to_partition(
        staged_partition,
        *args,
        file_paths=file_paths,
        content_type=content_type,
        pa_table=pa_table,
        ds_mock_kwargs=ds_mock_kwargs,
        **kwargs,
    )
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)
    return committed_delta


def download_delta(delta_like: Union[Delta, DeltaLocator], ds_mock_kwargs: Optional[Dict[str, Any]] = None, *args, **kwargs) -> Delta:
    if ds_mock_kwargs is None:
        ds_mock_kwargs = {}
    
    return pa.concat_tables(
        metastore.download_delta(
            delta_like,
            storage_type=StorageType.LOCAL,
            *args,
            **ds_mock_kwargs,
            **kwargs,
        )
    )


def commit_delta_to_partition(
    partition: Union[Partition, PartitionLocator],
    file_paths: List[str] = None,
    pa_table: pa.Table = None,
    content_type: ContentType = ContentType.PARQUET,
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
    *args,
    **kwargs,
) -> Delta:
    if ds_mock_kwargs is None:
        ds_mock_kwargs = {}

    if isinstance(partition, PartitionLocator):
        partition = metastore.get_partition(
            partition.stream_locator, partition.partition_values, **ds_mock_kwargs
        )
    if pa_table is None:
        assert file_paths is not None, "One of pa_table or file_paths must be passed."
        tables = []
        for file_path in file_paths:
            table = pa.csv.read_csv(file_path)
            tables.append(table)

        pa_table = pa.concat_tables(tables)

    staged_delta = metastore.stage_delta(
        pa_table, partition, content_type=content_type, **ds_mock_kwargs, **kwargs
    )

    return metastore.commit_delta(staged_delta, **ds_mock_kwargs) 