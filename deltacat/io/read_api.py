from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow as pa
import s3fs
from ray.data import read_datasource
from ray.data._internal.arrow_block import ArrowRow

from deltacat import ContentType
from deltacat.io.aws.redshift.redshift_datasource import (
    HivePartitionParser,
    RedshiftDatasource,
    RedshiftUnloadTextArgs,
    S3PathType,
)
from deltacat.io.dataset import DeltacatDataset
from deltacat.utils.common import ReadKwargsProvider


def read_redshift(
    paths: Union[str, List[str]],
    *,
    path_type: S3PathType = S3PathType.MANIFEST,
    filesystem: Optional[Union[pa.fs.S3FileSystem, s3fs.S3FileSystem]] = None,
    columns: Optional[List[str]] = None,
    schema: Optional[pa.Schema] = None,
    unload_text_args: RedshiftUnloadTextArgs = RedshiftUnloadTextArgs(),
    partitioning: HivePartitionParser = None,
    content_type_provider: Callable[[str], ContentType] = lambda p: ContentType.PARQUET
    if p.endswith(".parquet")
    else ContentType.CSV,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **kwargs,
) -> DeltacatDataset[ArrowRow]:
    """Reads Redshift UNLOAD results from either S3 Parquet or delimited text
    files into a Ray Dataset.

    Examples:
        >>> # Read all files contained in a Redshift Manifest:
        >>> import deltacat as dc
        >>> dc.io.read_redshift("/bucket/dir/manifest")

        >>> # Read all files matching the given key prefix. If this prefix
        >>> # refers to multiple files, like s3://bucket/data.parquet,
        >>> # s3://bucket/data.1.csv, etc. then all will be read. The dataset
        >>> # schema will be inferred from the first parquet file and used for
        >>> # explicit type conversion of all CSV files:
        >>> dc.io.read_redshift(
        >>>     "s3://bucket/data.txt",
        >>>     path_type=S3PathType.PREFIX)

        >>> # Read all files matching the given key prefix. If this prefix
        >>> # refers to multiple files or folders, like s3://bucket/dir/,
        >>> # s3://bucket/dir1/, s3://bucket/dir.txt, s3://bucket/dir.txt.1,
        >>> # then all files and subfolder contents will be read.
        >>> dc.io.read_redshift(
        >>>     "/bucket/dir",
        >>>     path_type=S3PathType.PREFIX)

        >>> # Read multiple files and folders:
        >>> dc.io.read_redshift(
        >>>     ["/bucket/file1", "/bucket/folder1/"],
        >>>     path_type=S3PathType.FILES_AND_FOLDERS)

        >>> # Read multiple Parquet and CSV files. The dataset schema will be
        >>> # inferred from the first parquet file and used for explicit type
        >>> # conversion of all CSV files:
        >>> dc.io.read_redshift(
        >>>     ["/bucket/file.parquet", "/bucket/file.csv"],
        >>>     path_type=S3PathType.FILES_AND_FOLDERS)

    Args:
        paths: Paths to S3 files and folders to read. If `path_type` is
            `MANIFEST` then this must be an S3 Redshift Manifest JSON file. If
            `path_type` is `PREFIX` then this must be a valid S3 key prefix.
            All files matching the key prefix, including files in matching
            subdirectories, will be read. Unless custom
            `content_type_extensions` are specified, file content types will be
            inferred by file extension with ".parquet" used for Parquet files,
            and all others assumed to be delimited text (e.g. CSV). It's
            recommended to specify the path to a manifest unloaded with the
            VERBOSE option whenever possible to improve the correctness and
            performance of Dataset reads, compute operations, and writes.
            `FILES_AND_FOLDERS` is not recommended when reading thousands of
            files due to its relatively high-latency.
        path_type: Determines how the `paths` parameter is interpreted.
        filesystem: The filesystem implementation to read from. This should be
            either PyArrow's S3FileSystem or s3fs.
        columns: A list of column names to read. Reads all columns if None or
            empty.
        schema: PyArrow schema used to determine delimited text column
            names and types. If not specified and both Parquet and delimited
            text files are read as input, then the first Parquet file schema
            discovered is used instead.
        unload_text_args: Arguments used when running Redshift `UNLOAD` to
            text file formats (e.g. CSV). These arguments ensure that all input
            text files will be correctly parsed. If not specified, then all
            text files read are assumed to use Redshift UNLOAD's default
            pipe-delimited text format.
        partition_base_dir: Base directory to start searching for partitions
            (exclusive). File paths outside of this directory will not be parsed
            for partitions and automatically added to the dataset without passing
            through any partition filter. Specify `None` or an empty string to
            search for partitions in all file path directories.
        partition_filter_fn: Callback used to filter `PARTITION` columns. Receives a
            dictionary mapping partition keys to values as input, returns `True` to
            read a partition, and `False` to skip it. Each partition key and value
            is a string parsed directly from an S3 key using hive-style
            partition directory names of the form "{key}={value}". For example:
            ``lambda x:
            True if x["month"] == "January" and x["year"] == "2022" else False``
        content_type_provider: Takes a file path as input and returns the file
            content type as output.
        parallelism: The requested parallelism of the read. Parallelism may be
            limited by the number of files of the dataset.
        ray_remote_args: kwargs passed to `ray.remote` in the read tasks.
        arrow_open_stream_args: kwargs passed to to
            `pa.fs.open_input_stream()`.
        pa_read_func_kwargs_provider: Callback that takes a `ContentType` value
            string as input, and provides read options to pass to either
            `pa.csv.open_csv()` or `pa.parquet.read_table()` as output.
    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    dataset = read_datasource(
        RedshiftDatasource(),
        parallelism=parallelism,
        paths=paths,
        content_type_provider=content_type_provider,
        path_type=path_type,
        filesystem=filesystem,
        columns=columns,
        schema=schema,
        unload_args=unload_text_args,
        partitioning=partitioning,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        read_kwargs_provider=pa_read_func_kwargs_provider,
        **kwargs,
    )
    return DeltacatDataset.from_dataset(dataset)
