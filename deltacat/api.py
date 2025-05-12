import time
from dataclasses import dataclass
from typing import Any, Union, List, Optional, Dict, Callable, Tuple

import ray
import deltacat as dc
import pyarrow.fs as pafs

from pyarrow.fs import FileType
from ray.exceptions import OutOfMemoryError

from deltacat.constants import BYTES_PER_GIBIBYTE
from deltacat.io import (
    read_deltacat,
    DeltacatReadType,
)
from deltacat.storage import (
    Dataset,
    DistributedDataset,
    ListResult,
    LocalTable,
    Metafile,
)
from deltacat.types.media import DatasetType
from deltacat.utils.url import (
    DeltaCatUrl,
    DeltaCatUrlReader,
    DeltaCatUrlWriter,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.types.tables import (
    get_table_size,
    get_table_length,
)
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    get_file_info,
)
from deltacat.utils.performance import timed_invocation
from deltacat.utils.ray_utils.runtime import (
    current_node_resources,
    live_cpu_waiter,
    live_node_resource_keys,
    other_live_node_resource_keys,
    find_max_single_node_resource_type,
)

"""
    # CLI Example of Copying from Source to Dest without file conversion
    # (i.e., register only - shallow copy):
    $ dcat cp json+s3://my_bucket/log_manager/ dc://my_deltacat_catalog/log_manager/json_table
    $ dcat cp json+s3://my_bucket/log_manager/ dc://my_deltacat_catalog/log_manager/json_table

    # CLI Example of Copying from Source to Dest without file conversion
    # (i.e., register only - deep copy):
    $ dcat cp json+s3://my_bucket/log_manager/ dc://my_deltacat_catalog/log_manager/json_table -r
    # The above command will make a deep copy of all JSON files found in the source
    # to the catalog data file directory in the destination.

    # CLI Example of Copying from Source to Dest with file conversion
    # (i.e., deep copy with file content type transformation):
    $ dcat convert json+s3://my_bucket/log_manager/ dc://my_deltacat_catalog/log_manager/ --type FEATHER
    # The above command will read JSON files found in the source, transform them to
    # Arrow Feather files, and register them in the destination.

    # Python Example of Copying from Source to Dest with file conversion
    # (i.e., deep copy with file content type transformation):
    >>> ds = dc.get("json+s3://my_bucket/log_manager/")
    >>> dc.put("dc://my_deltacat_catalog/log_manager/", dataset=ds, type=ContentType.FEATHER)
    # Or, equivalently, we can do the write directly from the dataset:
    >>> ds.write_deltacat("dc://my_deltacat_catalog/log_manager/", type=ContentType.FEATHER)
"""


def _copy_dc(
    source: DeltaCatUrl,
    destination: DeltaCatUrl,
    recursive: bool = False,
) -> Metafile:
    if recursive:
        src_obj = list(source, recursive=True)
    else:
        src_obj = get(source) if not source.url.endswith("/*") else list(source)
    """
    dc_dest_url = DeltacatUrl(destination)
    # TODO(pdames): Add writer with support for Ray Dataset DeltaCAT Sink &
    #  Recursive DeltaCAT source object copies. Ideally, the Ray Dataset read
    #  is lazy, and only indexes metadata about the objects at source instead
    #  of eagerly converting them to PyArrow-based Blocks.
    dc_dest_url.writer(src_obj, recursive=recursive)
    """

    src_parts = source.url.split("/")
    src_parts = [part for part in src_parts if part]
    dst_parts = destination.url.split("/")
    dst_parts = [part for part in dst_parts if part]
    dc.raise_if_not_initialized()
    if len(src_parts) != len(dst_parts):
        # TODO(pdames): Better error message.
        raise ValueError(
            f"Cannot copy {source} to {destination}. "
            f"Source and destination must share the same type."
        )
    return put(destination, metafile=src_obj)


def copy(
    src: DeltaCatUrl,
    dst: DeltaCatUrl,
    *,
    transforms: List[Callable[[Dataset, DeltaCatUrl], Dataset]] = [],
    extension_to_memory_multiplier: Dict[str, float] = {
        "pq": 5,
        "parquet": 5,
        "feather": 1.5,
        "arrow": 1.5,
        "csv": 1.5,
        "tsv": 1.5,
        "psv": 1.5,
        "txt": 1.5,
        "json": 1.5,
        "jsonl": 1.5,
        "gz": 35,
        "bz2": 35,
        "zip": 35,
        "7z": 35,
        "*": 2.5,
    },
    minimum_worker_cpus: int = 0,
    reader_args: Dict[str, Any] = {},
    writer_args: Dict[str, Any] = {},
    filesystem: Optional[pafs.FileSystem] = None,
) -> Union[Metafile, str]:
    """
    Copies data from the source datastore to the destination datastore. By
    default, this method launches one parallel Ray process to read/transform
    each input file found in the source followed by one parallel Ray process
    to write each output file to the destination. Files written to the
    destination are split or combined to contain uniform record counts. To
    ensure that adequate resources are available to complete the operation,
    you may optionally specify minimum cluster and/or worker CPUs to wait for
    before starting parallel processing.

    Args:
        src: DeltaCAT URL of the source datastore to read.
        dst: DeltaCAT URL of the destination datastore to write.
        transforms: List of transforms to apply to the source dataset prior
            to write it to the destination datastore. Transforms take the in-memory
            dataset type read (e.g., Polars DataFrame) and source DeltaCAT URL as
            input and return the same dataset type as output. Transforms are
            applied to the dataset in the order given.
        extension_to_memory_multiplier: Dictionary of file extensions to
            in-memory inflation estimates for that extension (i.e., the amount
            of memory required to read a source file, apply transforms, and write
            it back to a destination file).
        minimum_worker_cpus: The minimum number of Ray worker CPUs
            to wait for before starting distributed execution. Useful for cases
            where the operation is known to suffer from resource starvation (e.g.,
            out-of-memory errors) if started before the cluster has launched a
            minimum number of required worker nodes.
        reader_args: Additional keyword arguments to forward to the reader
            associated with the in-memory dataset and datastore type to read
            (e.g., polars.read_csv(**kwargs)).
        writer_args: Additional keyword arguments to forward to the writer
            associated with the in-memory dataset type read and datastore type to
            write (e.g., polars.DataFrame.write_parquet(**kwargs)).
        filesystem: Optional PyArrow filesystem to use for file IO. Will be
            automatically resolved from the input path if not specified, and
            will attempt to automatically resolve storage read/write
            credentials for the associated source/dest file cloud provider(s).
            Try providing your own filesystem with credentials, retry strategy,
            etc. pre-configured if you encounter latency issues or errors
            reading/writing files.

    Returns:
        None
    """
    if src.is_deltacat_catalog_url() or dst.is_deltacat_catalog_url():
        return _copy_dc(src, dst, recursive=src.url.endswith("/**"))
    else:
        return _copy_external_ray(
            src,
            dst,
            transforms=transforms,
            extension_to_memory_multiplier=extension_to_memory_multiplier,
            minimum_worker_cpus=minimum_worker_cpus,
            reader_args=reader_args,
            writer_args=writer_args,
            filesystem=filesystem,
        )


def concat(source, destination):
    raise NotImplementedError


def delete(source):
    raise NotImplementedError


def move(source, destination):
    raise NotImplementedError


def _list_all_metafiles(
    url: DeltaCatUrl,
    recursive: bool = False,
    **kwargs,
) -> List[Metafile]:
    reader = DeltaCatUrlReader(url)
    list_results: List[ListResult[Metafile]] = []
    lister = reader.listers.pop(0)[0]
    # the top-level lister doesn't have any missing keyword args
    metafiles: ListResult[Metafile] = lister(**kwargs)
    list_results.append(metafiles)
    if recursive:
        for lister, kwarg_name, kwarg_val_resolver_fn in reader.listers:
            # each subsequent lister needs to inject missing keyword args from the parent metafile
            for metafile in metafiles.all_items():
                kwargs_update = (
                    {kwarg_name: kwarg_val_resolver_fn(metafile)}
                    if kwarg_name and kwarg_val_resolver_fn
                    else {}
                )
                lister_kwargs = {
                    **kwargs,
                    **kwargs_update,
                }
                metafiles = lister(**lister_kwargs)
                list_results.append(metafiles)
    return [
        metafile for list_result in list_results for metafile in list_result.all_items()
    ]


class CustomReadKwargsProvider(ReadKwargsProvider):
    def __init__(
        self,
        datasource_type: str,
        kwargs: Dict[str, Any],
    ):
        self._datasource_type = datasource_type
        self._kwargs = kwargs

    def _get_kwargs(
        self,
        datasource_type: str,
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        if datasource_type == self._datasource_type:
            kwargs.update(self._kwargs)
        return kwargs


def list(
    url: DeltaCatUrl,
    *,
    recursive: bool = False,
    dataset_type: Optional[DatasetType] = None,
    **kwargs,
) -> Union[List[Metafile], LocalTable, DistributedDataset]:
    if not url.is_deltacat_catalog_url():
        raise NotImplementedError("List only supports DeltaCAT Catalog URLs.")
    if dataset_type in DatasetType.distributed():
        if dataset_type == DatasetType.RAY_DATASET:
            read_type = (
                DeltacatReadType.METADATA_LIST
                if not recursive
                else DeltacatReadType.METADATA_LIST_RECURSIVE
            )
            return read_deltacat(
                [url],
                deltacat_read_type=read_type,
                timestamp_as_of=None,
                merge_on_read=False,
                read_kwargs_provider=CustomReadKwargsProvider(
                    datasource_type=url.datastore_type,
                    kwargs=kwargs,
                ),
            )
        else:
            raise NotImplementedError(
                f"Unsupported dataset type: {dataset_type.name}. "
                f"Supported Dataset Types: {DatasetType.RAY_DATASET.name}",
            )
    else:
        # return a local list of metafiles
        # TODO(pdames): Cast the list to the appropriate local dataset type.
        return _list_all_metafiles(
            url=url,
            recursive=recursive,
            **kwargs,
        )


def get(
    url,
    *args,
    **kwargs,
) -> Union[Metafile, Dataset]:
    reader = DeltaCatUrlReader(url)
    return reader.read(*args, **kwargs)


def put(
    url: DeltaCatUrl,
    metafile: Optional[Metafile] = None,
    *args,
    **kwargs,
) -> Union[Metafile, str]:
    writer = DeltaCatUrlWriter(url, metafile)
    return writer.write(*args, **kwargs)


def touch(path):
    raise NotImplementedError


def exists(path):
    raise NotImplementedError


def query(expression):
    raise NotImplementedError


def tail(path):
    raise NotImplementedError


def head(path):
    raise NotImplementedError


def _copy_external_ray(
    src: DeltaCatUrl,
    dst: DeltaCatUrl,
    *,
    transforms: List[Callable[[Dataset, DeltaCatUrl], Dataset]] = [],
    extension_to_memory_multiplier: Dict[str, float] = {
        "pq": 5,
        "parquet": 5,
        "feather": 1.5,
        "arrow": 1.5,
        "csv": 1.5,
        "tsv": 1.5,
        "psv": 1.5,
        "txt": 1.5,
        "json": 1.5,
        "jsonl": 1.5,
        "gz": 35,
        "bz2": 35,
        "zip": 35,
        "7z": 35,
        "*": 2.5,
    },
    minimum_worker_cpus: int = 0,
    reader_args: Dict[str, Any] = {},
    writer_args: Dict[str, Any] = {},
    filesystem: pafs.FileSystem = None,
) -> str:
    print(f"DeltaCAT Copy Invocation Received at: {time.time_ns()}")

    if not isinstance(src, DeltaCatUrl):
        raise ValueError(f"Expected `src` to be a `DeltaCatUrl` but got `{src}`.")

    #  wait for required resources
    head_cpu_count = int(current_node_resources()["CPU"])
    if minimum_worker_cpus > 0:
        print(f"Waiting for {minimum_worker_cpus} worker CPUs...")
        live_cpu_waiter(
            min_live_cpus=minimum_worker_cpus + head_cpu_count,
        )
        print(f"{minimum_worker_cpus} worker CPUs found!")
    # start job execution
    cluster_resources = ray.cluster_resources()
    print(f"Cluster Resources: {cluster_resources}")
    print(f"Available Cluster Resources: {ray.available_resources()}")
    cluster_cpus = int(cluster_resources["CPU"])
    print(f"Cluster CPUs: {cluster_cpus}")
    all_node_resource_keys = live_node_resource_keys()
    print(f"Found {len(all_node_resource_keys)} live nodes: {all_node_resource_keys}")
    worker_node_resource_keys = other_live_node_resource_keys()
    print(
        f"Found {len(worker_node_resource_keys)} live worker nodes: {worker_node_resource_keys}"
    )
    worker_cpu_count = cluster_cpus - head_cpu_count
    print(f"Total worker CPUs: {worker_cpu_count}")

    # estimate memory requirements based on file extension
    estimated_memory_bytes = 0
    if extension_to_memory_multiplier:
        print(f"Resolving stats collection filesystem for: {src.url_path}.")
        path, filesystem = resolve_path_and_filesystem(src.url_path, filesystem)
        if isinstance(filesystem, pafs.GcsFileSystem):
            from datetime import timedelta

            # Configure a retry time limit for GcsFileSystem so that it
            # doesn't hang forever trying to get file info (e.g., when
            # trying to get a public file w/o anonymous=True).
            filesystem = pafs.GcsFileSystem(
                anonymous=True,
                retry_time_limit=timedelta(seconds=10),
            )
        print(f"Using filesystem {type(filesystem)} to get file size of: {path}")
        file_info = get_file_info(path, filesystem)
        if file_info.type != FileType.File:
            raise ValueError(
                f"Expected `src` to be a file but got `{file_info.type}` at "
                f"`{src.url_path}`."
            )
        inflation_multiplier = extension_to_memory_multiplier.get(file_info.extension)
        if inflation_multiplier is None:
            inflation_multiplier = extension_to_memory_multiplier.get("*")
        estimated_memory_bytes = inflation_multiplier * file_info.size
        print(
            f"Estimated Memory Required for Copy: "
            f"{estimated_memory_bytes/BYTES_PER_GIBIBYTE} GiB"
        )
    print(f"Starting DeltaCAT Copy at: {time.time_ns()}")

    index_result = None
    num_cpus = 1
    # TODO(pdames): remove hard-coding - issues encountered when going greater
    #  than 2 include verifying that the scope of schedulable nodes doesn't
    #  result in all large files lining up for the one large node in the cluster
    #  that can actually handle them (which is worse if it's also the head node)
    max_allowed_cpus = 2
    while not index_result:
        copy_task_pending, latency = timed_invocation(
            copy_task.options(num_cpus=num_cpus, memory=estimated_memory_bytes).remote,
            src=src,
            dest=dst,
            dataset_type=DatasetType.POLARS,
            transforms=transforms,
            reader_args=reader_args,
            writer_args=writer_args,
        )
        print(f"Time to Launch Copy Task: {latency} seconds")
        try:
            index_result, latency = timed_invocation(
                ray.get,
                copy_task_pending,
            )
        except OutOfMemoryError as e:
            print(f"Copy Task Ran Out of Memory: {e}")
            max_single_node_cpus = min(
                max_allowed_cpus, find_max_single_node_resource_type("CPU")
            )
            num_cpus += 1
            if num_cpus > max_single_node_cpus:
                raise e
            print(f"Retrying Failed Copy Task with {num_cpus} dedicated CPUs")

    print(f"Time to Launch Copy Task: {latency} seconds")
    print(f"Time to Complete Copy Task: {latency} seconds")

    total_gib_indexed = index_result.table_size / BYTES_PER_GIBIBYTE

    print(f"Records Copied: {index_result.table_length}")
    print(f"Bytes Copied: {total_gib_indexed} GiB")
    print(f"Conversion Rate: {total_gib_indexed/latency} GiB/s")
    print(f"Finished Copy at: {time.time_ns()}")

    return dst.url


@ray.remote(scheduling_strategy="SPREAD")
def copy_task(
    src: DeltaCatUrl,
    dest: DeltaCatUrl,
    dataset_type: DatasetType,
    transforms: List[Callable[[Dataset, DeltaCatUrl], Dataset]] = [],
    reader_args: Dict[str, Any] = {},
    writer_args: Dict[str, Any] = {},
) -> Tuple[Optional[int], int]:
    """
    Indexes a DeltaCAT source URL into a DeltaCAT destination URL.
    """
    table, latency = timed_invocation(
        read_table,
        src=src,
        dataset_type=dataset_type,
        transforms=transforms,
        reader_args=reader_args,
    )
    print(f"Time to read {src.url_path}: {latency} seconds")

    table_size = get_table_size(table)
    print(f"Table Size: {table_size/BYTES_PER_GIBIBYTE} GiB")

    table_length = get_table_length(table)
    print(f"Table Records: {table_length}")

    writer = DeltaCatUrlWriter(dest, dataset_type)
    written_file_path, latency = timed_invocation(
        writer.write,
        "",
        table,
        **writer_args,
    )
    print(f"Time to write {written_file_path}: {latency}")

    return CopyResult(table_size, table_length)


def read_table(
    src: DeltaCatUrl,
    dataset_type: DatasetType,
    transforms: List[Callable[[Dataset, DeltaCatUrl], Dataset]] = [],
    reader_args: Dict[str, Any] = {},
) -> LocalTable:
    reader = DeltaCatUrlReader(src, dataset_type)
    table: LocalTable = reader.read(**reader_args)
    for transform in transforms:
        table = transform(table, src)
    return table


@dataclass(frozen=True)
class CopyResult:
    table_size: int
    table_length: int
