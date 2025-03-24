from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow as pa
from pyarrow.fs import FileSystem
from ray.data import read_datasource

from deltacat import ContentType

from deltacat.io.datasource.deltacat_datasource import DeltacatDatasource
from deltacat.io.datasource.s3_datasource import (
    S3Datasource,
    DelimitedTextReaderConfig,
    S3PathType,
    HivePartitionParser,
)
from deltacat.io.dataset.deltacat_dataset import DeltacatDataset
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.url import DeltacatUrl
from deltacat.io.datasource.deltacat_datasource import DeltacatReadType


def read_deltacat(
    urls: Union[DeltacatUrl, List[DeltacatUrl]],
    *,
    deltacat_read_type: DeltacatReadType = DeltacatReadType.DATA,
    timestamp_as_of: Optional[int] = None,
    merge_on_read: Optional[bool] = False,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> DeltacatDataset:
    """Reads the given DeltaCAT URLs into a Ray Dataset. DeltaCAT URLs can
    either reference objects registered in a DeltaCAT catalog, or unregistered
    external objects that are readable into a Ray Dataset.

    Unless `metadata_only` is `True`, all reads of registered DeltaCAT catalog
    object data must resolve to a single table version.

    When reading unregistered external objects, all additional keyword
    arguments specified are passed into the Ray Datasource resolved for the
    given DeltaCAT URLs.

    Examples:
        >>> # Read the latest active DeltaCAT table version:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table")
        >>> # If `my_catalog is the default catalog, this is equivalent to:
        >>> dc.io.read_deltacat("namespace://my_namespace/my_table")
        >>> # If `my_namespace` is the default namespace, this is equivalent to:
        >>> dc.io.read_deltacat("table://my_table")

        >>> # Read metadata from all partitions and deltas of the latest active
        >>> # DeltaCAT table version:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table", metadata_only=True)
        >>> # Since "default" always resolves to the latest active table version.
        >>> # This is equivalent to:
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default", metadata_only=True)

        >>> # Read only the latest active table version's top-level metadata:
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default", metadata_only=True, recursive=False)

        >>> # Read only top-level metadata from a DeltaCAT table:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table", metadata_only=True, recursive=False)

        >>> # Read top-level table metadata from all table versions:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/*", metadata_only=True, recursive=False)

        >>> # Read metadata from all partitions and deltas of all table versions:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/*", metadata_only=True)

        >>> # Read metadata from all tables and table versions of the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/*", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the catalog's default namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog", metadata_only=True)

        >>> # Read metadata from all table versions for each table in each
        >>> # catalog namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/*", metadata_only=True)

        >>> # Read the Iceberg stream of the latest active DeltaCAT table version,
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default/iceberg")
        >>> # Or, if `my_catalog is the default catalog, this is equivalent to:
        >>> dc.io.read_deltacat("namespace://my_namespace/my_table/default/iceberg")
        >>> # Or, if `my_namespace` is the default namespace, this is equivalent to:
        >>> dc.io.read_deltacat("table://my_table/default/iceberg")

        >>> # Read an external unregistered Iceberg table `my_db.my_table`:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("iceberg://my_db.my_table")

        >>> # Read an external unregistered audio file from /my/audio.mp4:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("audio+file:///my/audio.mp4")

        >>> # Read an external unregistered audio file from s3://my/audio.mp4:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("audio+s3://my/audio.mp4")

    Args:
        urls: The DeltaCAT URLs to read.
        deltacat_read_type: If METADATA, reads only DeltaCAT metadata for the
            given URL and skips both recursive metadata expansion and reads
            of the underlying data files. If METADATA_RECURSIVE then recursively
            expands child metadata but does not read underlying data files. If
            DATA then recursively expands child metadata to discover and read
            all underlying data files.
        timestamp_as_of: Reads a historic snapshot of the given paths as-of the
            given millisecond-precision epoch timestamp (only used when reading
            registered DeltaCAT catalog objects).
        merge_on_read: If True, merges all unmaterialized inserts, updates,
            and deletes in the registered DeltaCAT table version being read. Only
            applicable if `metadata_only` is False.
            ray_remote_args: kwargs passed to `ray.remote` in the read tasks.
        read_kwargs_provider: Dictionary of
            :class:`~deltacat.types.media.DatasourceType` string keys to
            kwarg dictionaries to pass to the resolved
            :class:`~ray.data.Datasource` implementation for each distinct
            DeltaCAT URL type.

    Returns:
        DeltacatDataset holding Arrow records read from the specified URL.
    """
    # TODO(pdames): The below implementation serializes reads of each URL and
    #   then unions their respective datasets together. While this was an easy
    #   starting point to implement, a more efficient implementation should push
    #   all URLs down into `DeltacatDatasource` to parallelize all reads
    #   (i.e., by returning the `ReadTask` for all datasources in
    #   `get_read_tasks()` and estimating the corresponding memory size across
    #   all datasources in `estimate_inmemory_data_size()`.
    external_datasets = []
    for url in urls:
        if not url.catalog_name:
            # this URL points to an external unregistered Ray Datasource
            external_datasets.append(
                url.reader(read_kwargs_provider(url.datasource_type)),
            )
        else:
            # this URL points to a registered DeltaCAT object
            dataset = read_datasource(
                DeltacatDatasource(),
                url=url,
                deltacat_read_type=deltacat_read_type,
                timestamp_as_of=timestamp_as_of,
                merge_on_read=merge_on_read,
                read_kwargs_provider=read_kwargs_provider,
            )
    return DeltacatDataset.from_dataset(dataset)


def read_s3(
    paths: Union[str, List[str]],
    *,
    path_type: S3PathType = S3PathType.MANIFEST,
    filesystem: Optional[pa.fs.S3FileSystem] = None,
    columns: Optional[List[str]] = None,
    schema: Optional[pa.Schema] = None,
    csv_reader_config: DelimitedTextReaderConfig = DelimitedTextReaderConfig(),
    partitioning: HivePartitionParser = None,
    content_type_provider: Callable[[str], ContentType] = lambda p: ContentType.PARQUET
    if p.endswith(".parquet")
    else ContentType.CSV,
    parallelism: int = 200,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **kwargs,
) -> DeltacatDataset:
    """Reads a DeltaCAT manifest from either S3 Parquet or delimited text
    files into a Ray Dataset.

    Examples:
        >>> # Read all files contained in a DeltaCAT Manifest:
        >>> import deltacat as dc
        >>> dc.io.read_s3("/bucket/dir/manifest")

        >>> # Read all files matching the given key prefix. If this prefix
        >>> # refers to multiple files, like s3://bucket/data.parquet,
        >>> # s3://bucket/data.1.csv, etc. then all will be read. The dataset
        >>> # schema will be inferred from the first parquet file and used for
        >>> # explicit type conversion of all CSV files:
        >>> dc.io.read_s3(
        >>>     "s3://bucket/data.txt",
        >>>     path_type=S3PathType.PREFIX)

        >>> # Read all files matching the given key prefix. If this prefix
        >>> # refers to multiple files or folders, like s3://bucket/dir/,
        >>> # s3://bucket/dir1/, s3://bucket/dir.txt, s3://bucket/dir.txt.1,
        >>> # then all files and subfolder contents will be read.
        >>> dc.io.read_s3(
        >>>     "/bucket/dir",
        >>>     path_type=S3PathType.PREFIX)

        >>> # Read multiple files and folders:
        >>> dc.io.read_s3(
        >>>     ["/bucket/file1", "/bucket/folder1/"],
        >>>     path_type=S3PathType.FILES_AND_FOLDERS)

        >>> # Read multiple Parquet and CSV files. The dataset schema will be
        >>> # inferred from the first parquet file and used for explicit type
        >>> # conversion of all CSV files:
        >>> dc.io.read_s3(
        >>>     ["/bucket/file.parquet", "/bucket/file.csv"],
        >>>     path_type=S3PathType.FILES_AND_FOLDERS)

    Args:
        paths: Paths to S3 files and folders to read. If `path_type` is
            `MANIFEST` then this must be a DeltaCAT Manifest file. If
            `path_type` is `PREFIX` then this must be a valid S3 key prefix.
            All files matching the key prefix, including files in matching
            subdirectories, will be read. Unless custom
            `content_type_extensions` are specified, file content types will be
            inferred by file extension with ".parquet" used for Parquet files,
            and all others assumed to be delimited text (e.g. CSV). It's
            recommended to specify the path to a manifest whenever possible to
            improve the correctness and performance of Dataset reads, compute
            operations, and writes.
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
        csv_reader_config: Configuration for correctly parsing delimited text
            file formats (e.g. CSV). These arguments ensure that all input
            text files will be correctly parsed. If not specified, then all
            text files read are assumed to use a default pipe-delimited text
            format.
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
        S3Datasource(),
        parallelism=parallelism,
        paths=paths,
        content_type_provider=content_type_provider,
        path_type=path_type,
        filesystem=filesystem,
        columns=columns,
        schema=schema,
        csv_reader_config=csv_reader_config,
        partitioning=partitioning,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_stream_args,
        read_kwargs_provider=pa_read_func_kwargs_provider,
        **kwargs,
    )
    return DeltacatDataset.from_dataset(dataset)
