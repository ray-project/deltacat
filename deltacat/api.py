import time
from dataclasses import dataclass
from typing import Any, Union, List, Optional, Dict, Callable, Tuple
import logging

import ray
import pyarrow.fs as pafs

from pyarrow.fs import FileType
from ray.exceptions import OutOfMemoryError

from deltacat.catalog.model.properties import CatalogProperties
from deltacat.constants import BYTES_PER_GIBIBYTE, DEFAULT_NAMESPACE
from deltacat.io import (
    read_deltacat,
    DeltacatReadType,
)
from deltacat.storage import (
    Namespace,
    Table,
    TableVersion,
    Stream,
    Partition,
    Delta,
    Dataset,
    DistributedDataset,
    ListResult,
    LocalTable,
    Metafile,
)
from deltacat.exceptions import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
    TableVersionAlreadyExistsError,
    StreamAlreadyExistsError,
    PartitionAlreadyExistsError,
    DeltaAlreadyExistsError,
    ObjectAlreadyExistsError,
)
from deltacat.types.media import (
    DatasetType,
    DatastoreType,
)
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
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

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


def copy(
    src: Union[DeltaCatUrl, str],
    dst: Union[DeltaCatUrl, str],
    *,
    shallow: bool = False,
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
        "zst": 35,
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
    to write each output file to the destination. To ensure that adequate
    resources are available to complete the operation, you may optionally
    specify minimum cluster and/or worker CPUs to wait for before starting
    parallel processing.

    Args:
        src: DeltaCAT URL of the source datastore to read.
        dst: DeltaCAT URL of the destination datastore to write.
        shallow: For deltacat catalog source/dest URLs, only copies top-level
            metadata and no underlying data. Defaults to False.
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
    src = _resolve_url(src)
    dst = _resolve_url(dst)
    if src.is_deltacat_catalog_url() and dst.is_deltacat_catalog_url():
        # anything at or beneath the table level is copied recursively by default
        is_table_copy = src.table is not None or (
            src.url.endswith("/*") and src.namespace is not None
        )
        recursive = not shallow and (src.url.endswith("/**") or is_table_copy)
        return _copy_dc(src, dst, recursive=recursive)
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


def _copy_objects_in_order(
    src_objects: List[Metafile],
    destination: DeltaCatUrl,
    source_catalog: Optional[CatalogProperties] = None,
) -> Union[Metafile, List[Metafile]]:
    dc_dest_url = DeltaCatUrl(destination.url)
    catalog_name = dc_dest_url.catalog_name
    dest_namespace = dc_dest_url.namespace
    dest_table_name = dc_dest_url.table

    copied_results = []

    # Group objects by type for hierarchical copying
    # Copy objects in strict hierarchical order
    # Namespace -> Table -> TableVersion -> Stream -> Partition -> Delta
    ordered_objects_by_type = {
        Namespace: [],
        Table: [],
        TableVersion: [],
        Stream: [],
        Partition: [],
        Delta: [],
    }

    already_exists_errors_by_type = {
        Namespace: NamespaceAlreadyExistsError,
        Table: TableAlreadyExistsError,
        TableVersion: TableVersionAlreadyExistsError,
        Stream: StreamAlreadyExistsError,
        Partition: PartitionAlreadyExistsError,
        Delta: DeltaAlreadyExistsError,
    }

    for obj in src_objects:
        if obj is not None:
            obj_class = Metafile.get_class(obj.to_serializable())
            ordered_objects_by_type[obj_class].append(obj)

    # TODO(pdames): Support copying uncommitted streams/partitions.
    # TODO(pdames): Support parallel/distributed copies.
    # TODO(pdames): More graceful error when objects already exist (right now just getting lower-level storage errors)
    # TODO(pdames): Support renamed ancestor creation in the dest tree (e.g., copying a table to a new namespace & table)
    # TODO(pdames): Provide option to overwrite or fail when objects already exist.
    for obj_class, objects in ordered_objects_by_type.items():
        if objects:
            logger.info(f"Copying {len(objects)} {obj_class} objects...")
        if obj_class == TableVersion:
            # sort table versions by ascending table version
            objects.sort(key=lambda x: x.current_version_number())
        if obj_class == Delta:
            # sort deltas by ascending stream position
            objects.sort(key=lambda x: x.stream_position)
        for i, obj in enumerate(objects):
            logger.info(f"Copying object {i+1}/{len(objects)}: {obj.url}")
            # Set ephemeral source catalog for Delta objects to enable cross-catalog path resolution
            if obj_class == Delta and source_catalog:
                obj.catalog = source_catalog
            # Create the destination URL (accounting for different target catalog/namespace/table names)
            dest_url = DeltaCatUrl(
                obj.url(
                    catalog_name=catalog_name,
                    namespace=dest_namespace,
                    table_name=dest_table_name,
                )
            )
            logger.info(f"Destination URL for object {i+1}/{len(objects)}: {dest_url}")
            try:
                result = put(dest_url, metafile=obj)
                copied_results.append(result)
                logger.info(f"Successfully copied object {i+1}/{len(objects)}")
            except (
                already_exists_errors_by_type[obj_class],
                ObjectAlreadyExistsError,
            ) as e:
                target_namespace = dest_namespace or obj.namespace
                if obj_class == Namespace and target_namespace == DEFAULT_NAMESPACE:
                    logger.info(
                        f"Skipping already exists error for default namespace {obj_class.__name__} at {obj.url}: {e}"
                    )
                else:
                    raise e
    return copied_results[0] if len(copied_results) == 1 else copied_results


def _copy_dc(
    source: DeltaCatUrl,
    destination: DeltaCatUrl,
    recursive: bool = False,
) -> Union[Metafile, List[Metafile]]:
    # Normalize source and destination URLs
    source_url = source.url.rstrip("/")
    source_url = source_url.rstrip("/**")
    source_url = source_url.rstrip("/*")
    source_url = source_url.rstrip("/")
    destination_url = destination.url.rstrip("/")

    if len(source_url.split("/")) != len(destination_url.split("/")):
        # TODO(pdames): Better error message.
        raise ValueError(
            f"Cannot copy {source} to {destination}. "
            f"Source and destination must share the same type."
        )
    if recursive:
        src_objects = list(DeltaCatUrl(source_url), recursive=True)
    elif source.url.endswith("/*"):
        src_objects = list(DeltaCatUrl(source_url))
    else:
        src_objects = [get(source)]
    # Get source catalog root for cross-catalog path resolution
    source_catalog = None

    # Save the source catalog for destination path resolution.
    source.resolve_catalog()
    source_catalog = source.catalog

    return _copy_objects_in_order(src_objects, destination, source_catalog)


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
        # Process each level of the hierarchy
        current_level_metafiles = [mf for mf in metafiles.all_items()]

        for lister, kwarg_name, kwarg_val_resolver_fn in reader.listers:
            next_level_metafiles = []
            # each subsequent lister needs to inject missing keyword args from the parent metafile
            for metafile in current_level_metafiles:
                kwargs_update = (
                    {kwarg_name: kwarg_val_resolver_fn(metafile)}
                    if kwarg_name and kwarg_val_resolver_fn
                    else {}
                )
                lister_kwargs = {
                    **kwargs,
                    **kwargs_update,
                }
                child_metafiles = lister(**lister_kwargs)
                list_results.append(child_metafiles)
                next_level_metafiles.extend(child_metafiles.all_items())
            # Move to the next level for the next iteration
            current_level_metafiles = next_level_metafiles
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
    url: Union[DeltaCatUrl, str],
    *,
    recursive: bool = False,
    dataset_type: Optional[DatasetType] = None,
    **kwargs,
) -> Union[List[Metafile], LocalTable, DistributedDataset]:
    url = _resolve_url(url)
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


def _resolve_url(url: Union[DeltaCatUrl, str]) -> DeltaCatUrl:
    if isinstance(url, str):
        try:
            url = DeltaCatUrl(url)
        except ValueError:
            url = DatastoreType.get_url(url)
            url = DeltaCatUrl(url)
    return url


def get(
    url: Union[DeltaCatUrl, str],
    read_as: DatasetType = DatasetType.RAY_DATASET,
    *args,
    **kwargs,
) -> Union[Metafile, Dataset]:
    """
    Reads a DeltaCAT URL into a Metafile or Dataset. DeltaCAT URLs can either
    reference objects registered in a DeltaCAT catalog, or unregistered external
    objects that are readable into a Dataset. DeltaCAT automatically infers the right
    Ray Data reader for the URL. If the URL is an unregistered external object,
    the reader will be inferred from the URL's datastore type.

    Args:
        url: The DeltaCAT URL to read.
        read_as: The DatasetType to read an unregistered external object as. Ignored for
            registered DeltaCAT objects. Defaults to DatasetType.RAY_DATASET.
        args: Additional arguments to pass to the reader.
        kwargs: Additional keyword arguments to pass to the reader.

    Returns:
        A Metafile for registered DeltaCAT URLs or a Dataset containing the
        data from the URL.
    """
    url = _resolve_url(url)
    reader = DeltaCatUrlReader(url, dataset_type=read_as)
    return reader.read(*args, **kwargs)


def put(
    url: Union[DeltaCatUrl, str],
    metafile: Optional[Metafile] = None,
    *args,
    **kwargs,
) -> Union[Metafile, str]:
    url = _resolve_url(url)
    writer = DeltaCatUrlWriter(url, metafile=metafile)
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
        "zst": 35,
        "7z": 35,
        "*": 2.5,
    },
    minimum_worker_cpus: int = 0,
    reader_args: Dict[str, Any] = {},
    writer_args: Dict[str, Any] = {},
    filesystem: pafs.FileSystem = None,
) -> str:
    logger.info(f"DeltaCAT Copy Invocation Received at: {time.time_ns()}")

    if not isinstance(src, DeltaCatUrl):
        raise ValueError(f"Expected `src` to be a `DeltaCatUrl` but got `{src}`.")

    #  wait for required resources
    head_cpu_count = int(current_node_resources()["CPU"])
    if minimum_worker_cpus > 0:
        logger.info(f"Waiting for {minimum_worker_cpus} worker CPUs...")
        live_cpu_waiter(
            min_live_cpus=minimum_worker_cpus + head_cpu_count,
        )
        logger.info(f"{minimum_worker_cpus} worker CPUs found!")
    # start job execution
    cluster_resources = ray.cluster_resources()
    logger.info(f"Cluster Resources: {cluster_resources}")
    logger.info(f"Available Cluster Resources: {ray.available_resources()}")
    cluster_cpus = int(cluster_resources["CPU"])
    logger.info(f"Cluster CPUs: {cluster_cpus}")
    all_node_resource_keys = live_node_resource_keys()
    logger.info(
        f"Found {len(all_node_resource_keys)} live nodes: {all_node_resource_keys}"
    )
    worker_node_resource_keys = other_live_node_resource_keys()
    logger.info(
        f"Found {len(worker_node_resource_keys)} live worker nodes: {worker_node_resource_keys}"
    )
    worker_cpu_count = cluster_cpus - head_cpu_count
    logger.info(f"Total worker CPUs: {worker_cpu_count}")

    # estimate memory requirements based on file extension
    estimated_memory_bytes = 0
    if extension_to_memory_multiplier:
        logger.info(f"Resolving stats collection filesystem for: {src.url_path}.")
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
        logger.info(f"Using filesystem {type(filesystem)} to get file size of: {path}")
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
        logger.info(
            f"Estimated Memory Required for Copy: "
            f"{estimated_memory_bytes/BYTES_PER_GIBIBYTE} GiB"
        )
    logger.info(f"Starting DeltaCAT Copy at: {time.time_ns()}")

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
        logger.info(f"Time to Launch Copy Task: {latency} seconds")
        try:
            index_result, latency = timed_invocation(
                ray.get,
                copy_task_pending,
            )
        except OutOfMemoryError as e:
            logger.warning(f"Copy Task Ran Out of Memory: {e}")
            max_single_node_cpus = min(
                max_allowed_cpus, find_max_single_node_resource_type("CPU")
            )
            num_cpus += 1
            if num_cpus > max_single_node_cpus:
                raise e
            logger.info(f"Retrying Failed Copy Task with {num_cpus} dedicated CPUs")

    logger.info(f"Time to Launch Copy Task: {latency} seconds")
    logger.info(f"Time to Complete Copy Task: {latency} seconds")

    total_gib_indexed = index_result.table_size / BYTES_PER_GIBIBYTE

    logger.info(f"Records Copied: {index_result.table_length}")
    logger.info(f"Bytes Copied: {total_gib_indexed} GiB")
    logger.info(f"Conversion Rate: {total_gib_indexed/latency} GiB/s")
    logger.info(f"Finished Copy at: {time.time_ns()}")

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
    logger.debug(f"Time to read {src.url_path}: {latency} seconds")

    table_size = get_table_size(table)
    logger.debug(f"Table Size: {table_size/BYTES_PER_GIBIBYTE} GiB")

    table_length = get_table_length(table)
    logger.debug(f"Table Records: {table_length}")

    writer = DeltaCatUrlWriter(dest, dataset_type)
    written_file_path, latency = timed_invocation(
        writer.write,
        "",
        table,
        **writer_args,
    )
    logger.debug(f"Time to write {written_file_path}: {latency}")

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
