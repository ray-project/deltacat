import functools
from typing import Callable, List, Tuple, Any, Union
from urllib.parse import urlparse, urlunparse

import ray

import deltacat as dc
from deltacat import Namespace
from deltacat.catalog import CatalogProperties
from deltacat.constants import DEFAULT_NAMESPACE
from deltacat.types.media import DatasourceType

from deltacat.storage import (
    metastore,
    Delta,
    DeltaLocator,
    ListResult,
    Metafile,
    Partition,
    Stream,
    StreamFormat,
    StreamLocator,
    PartitionLocator,
    TableVersionLocator,
)
from storage import TableVersion, Table, TableLocator, NamespaceLocator


def _stage_and_commit_stream(
    stream: Stream,
    *args,
    **kwargs,
) -> Stream:
    """
    Helper method to stage and commit a stream (e.g., as part of a copy
    operation from another catalog). The committed stream will be assigned a
    different unique ID than the input stream.
    """
    stream = metastore.stage_stream(
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=stream.stream_format,
        *args,
        **kwargs,
    )
    return metastore.commit_stream(
        stream=stream,
        *args,
        **kwargs,
    )


def _stage_and_commit_partition(
    partition: Partition,
    *args,
    **kwargs,
) -> Partition:
    """
    Helper method to stage and commit a partition (e.g., as part of a copy
    operation from another catalog). The committed partition will be assigned a
    different unique ID than the input partition.
    """
    stream = metastore.get_stream(
        namespace=partition.namespace,
        table_name=partition.table_name,
        table_version=partition.table_version,
        stream_format=partition.stream_format,
    )
    partition = metastore.stage_partition(
        stream=stream,
        partition_values=partition.partition_values,
        partition_scheme_id=partition.partition_scheme_id,
        *args,
        **kwargs,
    )
    return metastore.commit_partition(
        partition=partition,
        *args,
        **kwargs,
    )


class DeltacatUrl:
    """
    Class for parsing DeltaCAT URLs, which are used to unambiguously locate
    any internal object(s) already registered in a DeltaCAT catalog, or external
    object(s) that could be registered in a DeltaCAT catalog.

    Valid DeltaCAT URLs that reference internal catalog objects registered in a
    DeltaCAT catalog include:

    dc://<catalog>/[namespace]/[table]/[tableversion]/[stream]/[partition]/[delta]
    namespace://<namespace>/[table]/[tableversion]/[stream]/[partition]/[delta]
    table://<table>/[tableversion]/[stream]/[partition]/[delta]

    Where <arg> is a required part of the URL and [arg] is an optional part of
    the URL.

    Valid DeltaCAT URLs that reference external objects include most types
    readable into a Ray Dataset. Most of these can be located via a URL of
    the form <datasource_type>+<URL> or, to be more explicit,
    <datasource_type>+<scheme>://<path> where `datasource_type` is any value
    from :class:`~deltacat.types.media.DatasourceType`

    To reference a file on local disk, replace <scheme>:// with "file" or
    "local". To read an absolute local file path, use "file:///" or
    "local:///". To read a local file path relative to the current working
    directory, use "local://".

    audio+<scheme>://<path>
    avro+<scheme>://<path>
    binary+<scheme>://<path>
    csv+<scheme>://<path>
    deltasharing+<scheme>://<path>
    hudi+<scheme>://<path>
    images+<scheme>://<path>
    json+<scheme>://<path>
    lance+<scheme>://<path>
    mongo+<scheme>://<path>
    numpy+<scheme>://<path>
    parquet+<scheme>://<path>
    text+<scheme>://<path>
    tfrecords+<scheme>://<path>
    videos+<scheme>://<path>
    webdataset+<scheme>://<path>

    Some DeltaCAT URLs reference special types of external objects
    locatable via custom URLs that don't conform to the usual
    <datasource_type>+<URL> convention shown above, like:

    bigquery://<project_id>
    <clickhouse_dsn>?<clickhouse_table>
    databricks://<warehouse_id>
    iceberg://<table_identifier>

    Note that, for reads, each of the above URLs typically resolves directly
    to the equivalent ray.data.from_{scheme} API. For example, a read
    referencing a URL of the form "audio+file:///my/audio.mp4" would resolve to
    a call to ray.data.from_audio("/my/audio.mp4").
    """

    # Auto-resolved DeltaCAT catalog path default identifiers
    DELTACAT_URL_DEFAULT_CATALOG = "default"
    DELTACAT_URL_DEFAULT_NAMESPACE = "default"
    DELTACAT_URL_DEFAULT_TABLE_VERSION = "default"
    DELTACAT_URL_DEFAULT_STREAM = "default"

    def _resolve_deltacat_path_identifiers(self):
        if not dc.is_initialized():
            # TODO(pdames): Re-initialize DeltaCAT with all catalogs from the
            #  last session.
            raise RuntimeError(
                "DeltaCAT is not initialized. Please call `dc.init()` and try again."
            )
        self.namespace = self.table_version = self.stream = None
        if self.catalog_name:
            if self.catalog_name.lower() == DeltacatUrl.DELTACAT_URL_DEFAULT_CATALOG:
                self.catalog: CatalogProperties = None
            self.catalog: CatalogProperties = dc.get_catalog(
                self.catalog_name
            ).native_object
            if not isinstance(self.catalog, CatalogProperties):
                raise ValueError(
                    f"Expected catalog `{self.catalog_name}` to be a DeltaCAT "
                    f"catalog but found: {self.catalog}"
                )
        if self.unresolved_namespace:
            if (
                self.unresolved_namespace.lower()
                == DeltacatUrl.DELTACAT_URL_DEFAULT_NAMESPACE
            ):
                self.namespace = DEFAULT_NAMESPACE
            else:
                self.namespace = self.unresolved_namespace
        if (
            self.unresolved_table_version
            and self.unresolved_table_version.lower()
            != DeltacatUrl.DELTACAT_URL_DEFAULT_TABLE_VERSION
        ):
            self.table_version = self.unresolved_table_version
        if self.unresolved_stream:
            if (
                self.unresolved_stream.lower()
                == DeltacatUrl.DELTACAT_URL_DEFAULT_STREAM
            ):
                self.stream = StreamFormat.DELTACAT
            else:
                self.stream = StreamFormat(self.stream)

    def _resolve_dc_writer(self) -> Callable:
        if self.delta:
            delta: Delta = Metafile.based_on(
                other=self.metafile,
                new_id=self.delta,
            )
            delta.locator = DeltaLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_id=None,
                stream_format=self.stream,
                partition_values=self.partition,
                partition_id=None,
                stream_position=self.delta,
            )
            # TODO(pdames): Honor deep vs. shallow copies. Deep copies require
            #  first ensuring that all files in the source delta manifest are
            #  staged to the target catalog before commit. For deltas whose
            #  manifests reference local files, shallow delta copies will be
            #  invalid in the target catalog, and should be blocked or
            #  converted to a deep copy automatically.
            return functools.partial(
                metastore.commit_delta(
                    delta=self.delta,
                    catalog=self.catalog,
                ),
            )
        if self.partition:
            partition: Partition = self.metafile
            partition.locator = PartitionLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_id=None,
                stream_format=self.stream,
                partition_values=self.partition,
            )
            return functools.partial(
                _stage_and_commit_partition(
                    partition=partition,
                    catalog=self.catalog,
                ),
            )
        if self.unresolved_stream:
            stream: Stream = self.metafile
            stream.locator = StreamLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_id=None,
                stream_format=self.stream,
            )
            return functools.partial(
                _stage_and_commit_stream(
                    stream=stream,
                    catalog=self.catalog,
                ),
            )
        if self.unresolved_table_version:
            table_version: TableVersion = self.metafile
            table_version.locator = TableVersionLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
            )
            return functools.partial(
                metastore.create_table_version(
                    namespace=table_version.namespace,
                    table_name=table_version.table_name,
                    table_version=table_version.table_version,
                    schema=table_version.schema,
                    partition_scheme=table_version.partition_scheme,
                    sort_keys=table_version.sort_keys,
                    table_version_description=table_version.table_version_description,
                    table_version_properties=table_version.table_version_properties,
                    table_description=table_version.table_description,
                    table_properties=table_version.table_properties,
                    supported_content_types=table_version.supported_content_types,
                    catalog=self.catalog,
                ),
            )
        if self.table:
            table: Table = self.metafile
            table.locator = TableLocator.at(
                namespace=self.namespace,
                table_name=self.table,
            )
            return functools.partial(
                metastore.create_table_version(
                    namespace=table.namespace,
                    table_name=table.table_name,
                    table_description=table.table_description,
                    table_properties=table.properties,
                    catalog=self.catalog,
                ),
            )
        if self.unresolved_namespace:
            namespace: Namespace = self.metafile
            namespace.locator = NamespaceLocator.of(
                namespace=self.namespace,
            )
            return functools.partial(
                metastore.create_namespace,
                namespace=self.namespace,
                properties=namespace.properties,
                catalog=self.catalog,
            )
        if self.catalog_name:
            return functools.partial(
                dc.put_catalog,
                name=self.catalog_name,
            )
        raise ValueError("No DeltaCAT object to write.")

    def _resolve_dc_reader(self) -> Callable:
        if self.delta:
            return functools.partial(
                metastore.get_delta,
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                partition_values=self.partition,
                stream_position=self.delta,
                catalog=self.catalog,
            )
        if self.partition:
            return functools.partial(
                metastore.get_partition,
                stream_locator=StreamLocator.at(
                    namespace=self.namespace,
                    table_name=self.table,
                    table_version=self.table_version,
                    stream_id=None,
                    stream_format=self.stream,
                ),
                partition_values=self.partition,
                catalog=self.catalog,
            )
        if self.unresolved_stream:
            return functools.partial(
                metastore.get_stream,
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_format=self.stream,
                catalog=self.catalog,
            )
        if self.unresolved_table_version:
            return functools.partial(
                metastore.get_table_version,
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                catalog=self.catalog,
            )
        if self.table:
            return functools.partial(
                metastore.get_table,
                namespace=self.namespace,
                table_name=self.table,
                catalog=self.catalog,
            )
        if self.unresolved_namespace:
            return functools.partial(
                metastore.get_namespace,
                namespace=self.namespace,
                catalog=self.catalog,
            )
        if self.catalog_name:
            return functools.partial(
                dc.get_catalog,
                name=self.catalog_name,
            )
        raise ValueError("No DeltaCAT object to read.")

    def _resolve_dc_lister(
        self,
    ) -> List[
        Tuple[
            Callable[[Any], ListResult[Metafile]],
            str,
            Callable[[Metafile], Union[Metafile, str]],
        ]
    ]:
        if self.partition:
            partition_locator = PartitionLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_id=None,
                stream_format=self.stream,
                partition_values=self.partition,
                partition_id=None,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                partition_like=partition_locator,
                catalog=self.catalog,
            )
            return [(delta_lister, None, None)]
        if self.unresolved_stream:
            stream_locator = StreamLocator.at(
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                stream_id=None,
                stream_format=self.stream,
            )
            stream = Stream.of(
                locator=stream_locator,
                partition_scheme=None,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                stream=stream,
                catalog=self.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=self.catalog,
            )
            return [
                (partition_lister, None, None),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if self.unresolved_table_version:
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=self.namespace,
                table_name=self.table,
                table_version=self.table_version,
                catalog=self.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=self.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=self.catalog,
            )
            return [
                (stream_lister, None, None),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if self.table:
            table_version_lister = functools.partial(
                metastore.list_table_versions,
                namespace=self.namespace,
                table_name=self.table,
                catalog=self.catalog,
            )
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=self.namespace,
                table_name=self.table,
                catalog=self.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=self.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=self.catalog,
            )
            return [
                (table_version_lister, None, None),
                (stream_lister, "table_version", lambda x: x.table_version),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if self.unresolved_namespace:
            table_lister = functools.partial(
                metastore.list_tables,
                namespace=self.namespace,
                catalog=self.catalog,
            )
            table_version_lister = functools.partial(
                metastore.list_table_versions,
                namespace=self.namespace,
                catalog=self.catalog,
            )
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=self.namespace,
                table_name=self.table,
                catalog=self.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=self.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=self.catalog,
            )
            return [
                (table_lister, None, None),
                (table_version_lister, "table_name", lambda x: x.table_name),
                (stream_lister, "table_version", lambda x: x.table_version),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if self.catalog_name:
            namespace_lister = functools.partial(
                metastore.list_namespaces,
                catalog=self.catalog,
            )
            table_lister = functools.partial(
                metastore.list_tables,
                catalog=self.catalog,
            )
            table_version_lister = functools.partial(
                metastore.list_table_versions,
                namespace=self.namespace,
                catalog=self.catalog,
            )
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=self.namespace,
                table_name=self.table,
                catalog=self.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=self.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=self.catalog,
            )
            return [
                (namespace_lister, None, None),
                (table_lister, "namespace", lambda x: x.namespace),
                (table_version_lister, "table_name", lambda x: x.table_name),
                (stream_lister, "table_version", lambda x: x.table_version),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        raise ValueError("No DeltaCAT objects to list.")

    def __init__(self, url: str):
        # TODO(pdames): Handle wildcard `*` at end of url.

        self._parsed = urlparse(url, allow_fragments=False)  # support '#' in path
        self.url = self._parsed.geturl()
        path = self._parsed.path
        # Remove leading/trailing slashes and split the path into elements
        path_elements = [
            element for element in path.strip("/").split("/") if path and element
        ]
        # Split the scheme into the root DeltaCAT scheme and the path scheme
        scheme_elements = self._parsed.scheme.split("+")
        self.datasource_type = scheme_elements[0]
        if len(scheme_elements) == 2:
            # Remove the reader type from the scheme.
            self._parsed = self._parsed._replace(scheme=scheme_elements[1])
            # Save the URL path to read w/o the reader type.
            self.reader_url = urlunparse(self._parsed)
        elif len(scheme_elements) > 2:
            raise ValueError(f"Invalid DeltaCAT URL: {url}")
        if self.datasource_type == DatasourceType.DELTACAT:
            self.catalog_name = self._parsed.netloc
            self.unresolved_namespace = path_elements[0] if path_elements else None
            self.table = path_elements[1] if len(path_elements) > 1 else None
            self.unresolved_table_version = (
                path_elements[2] if len(path_elements) > 2 else None
            )
            self.unresolved_stream = (
                path_elements[3] if len(path_elements) > 3 else None
            )
            self.partition = path_elements[4] if len(path_elements) > 4 else None
            self.delta = path_elements[5] if len(path_elements) > 5 else None
            self._resolve_deltacat_path_identifiers()
            self.reader = self._resolve_dc_reader()
            self.listers = self._resolve_dc_lister()
            self.writer = self._resolve_dc_writer()
        elif self.datasource_type == DatasourceType.DELTACAT_NAMESPACE:
            self.catalog_name = DeltacatUrl.DELTACAT_URL_DEFAULT_CATALOG
            self.unresolved_namespace = self._parsed.netloc
            self.table = path_elements[0] if path_elements else None
            self.unresolved_table_version = (
                path_elements[1] if len(path_elements) > 1 else None
            )
            self.unresolved_stream = (
                path_elements[2] if len(path_elements) > 2 else None
            )
            self.partition = path_elements[3] if len(path_elements) > 3 else None
            self.delta = path_elements[4] if len(path_elements) > 4 else None
            self._resolve_deltacat_path_identifiers()
            self.reader = self._resolve_dc_reader()
            self.listers = self._resolve_dc_lister()
            self.writer = self._resolve_dc_writer()
        elif self.datasource_type == DatasourceType.DELTACAT_TABLE:
            self.unresolved_namespace = DeltacatUrl.DELTACAT_URL_DEFAULT_NAMESPACE
            self.table = self._parsed.netloc
            self.unresolved_table_version = path_elements[0] if path_elements else None
            self.unresolved_stream = (
                path_elements[1] if len(path_elements) > 1 else None
            )
            self.partition = path_elements[2] if len(path_elements) > 2 else None
            self.delta = path_elements[3] if len(path_elements) > 3 else None
            self._resolve_deltacat_path_identifiers()
            self.reader = self._resolve_dc_reader()
            self.listers = self._resolve_dc_lister()
            self.writer = self._resolve_dc_writer()
        elif self.datasource_type == DatasourceType.AUDIO:
            self.reader = functools.partial(
                ray.data.read_audio,
                paths=self.reader_url,
            )
            self.writer = None
        elif self.datasource_type == DatasourceType.AVRO:
            self.reader = functools.partial(
                ray.data.read_avro,
                paths=self.reader_url,
            )
            self.writer = None
        elif self.datasource_type == DatasourceType.BIGQUERY:
            self.reader = functools.partial(
                ray.data.read_bigquery,
                project_id=self._parsed.netloc,
            )
            self.writer = functools.partial(
                ray.data.Dataset.write_bigquery,
            )
        elif self.datasource_type == DatasourceType.BINARY_FILES:
            self.reader = functools.partial(
                ray.data.read_binary_files,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.CSV:
            self.reader = functools.partial(
                ray.data.read_csv,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.CLICKHOUSE:
            if not self._parsed.query:
                raise ValueError(
                    "Clickhouse table name must be specified as a URL query."
                )
            self.reader = functools.partial(
                ray.data.read_clickhouse,
                table=self._parsed.query,
                dsn=url,
            )
        elif self.datasource_type == DatasourceType.DATABRICKS_TABLES:
            self.reader = functools.partial(
                ray.data.read_databricks_tables,
                warehouse_id=self._parsed.netloc,
            )
        elif self.datasource_type == DatasourceType.DELTA_SHARING:
            self.reader = functools.partial(
                ray.data.read_delta_sharing_tables,
                url=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.HUDI:
            self.reader = functools.partial(
                ray.data.read_hudi,
                table_uri=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.ICEBERG:
            self.reader = functools.partial(
                ray.data.read_iceberg, table_identifier=self._parsed.netloc
            )
        elif self.datasource_type == DatasourceType.IMAGES:
            self.reader = functools.partial(
                ray.data.read_images,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.JSON:
            self.reader = functools.partial(
                ray.data.read_json,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.LANCE:
            self.reader = functools.partial(
                ray.data.read_lance,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.MONGO:
            self.reader = functools.partial(
                ray.data.read_mongo,
                uri=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.NUMPY:
            self.reader = functools.partial(
                ray.data.read_numpy,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.PARQUET:
            self.reader = functools.partial(
                ray.data.read_parquet,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.SQL:
            # TODO(pdames): Find a better way to nest a SQL statement within a DeltaCAT URL
            raise NotImplementedError("SQL is not yet implemented.")
            #    self.reader = functools.partial(ray.data.read_sql, sql=self._parsed.netloc)
        elif self.datasource_type == DatasourceType.TEXT:
            self.reader = functools.partial(
                ray.data.read_text,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.TFRECORDS:
            self.reader = functools.partial(
                ray.data.read_tfrecords,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.VIDEOS:
            self.reader = functools.partial(
                ray.data.read_videos,
                paths=self.reader_url,
            )
        elif self.datasource_type == DatasourceType.WEBDATASET:
            self.reader = functools.partial(
                ray.data.read_webdataset,
                paths=self.reader_url,
            )
        else:
            raise ValueError(
                f"Invalid DeltaCAT datasource type `{self.datasource_type}` "
                f"for URL `{url}`"
            )
