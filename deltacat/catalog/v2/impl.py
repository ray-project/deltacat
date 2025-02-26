import os
from typing import Any, Dict, List, Optional, Union

import pyarrow

from deltacat.storage.model.partition import PartitionScheme
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.table import TableProperties
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.storage.rivulet.dataset import Dataset as RivuletDataset
from deltacat.storage.main import impl as storage_impl
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.model.constants import (
    DEFAULT_NAMESPACE,
)

class PropertyCatalog:
    DEFAULT_ROOT=".deltacat"

    """
    This holds all configuration for a DeltaCAT catalog. 
    A catalog implementation that stores and retrieves properties for tables and namespaces.
        
    Attributes:
        root (str): URI string The root path where catalog metadata and data files are stored. If none provided,
          will be initialized as .deltacat/ relative to current working directory
      
        filesystem (pyarrow.fs.FileSystem): pyarrow filesystem implementation used for
            accessing files. If not provided, will be inferred via root 
    """

    def __init__(
        self,
        *,
        root: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ):
        """
        Initialize a PropertyCatalog instance.
        
        Args:
            root (str, optional): Root path for the catalog storage. If None, will be resolved later.
            filesystem (pyarrow.fs.FileSystem, optional): FileSystem implementation to use.
                If None, will be resolved based on the root path.
        """
        if root is None:
            self.root = os.path.join(os.getcwd(), ".deltacat")

        resolved_root, resolved_filesystem = resolve_path_and_filesystem(
            path=root,
            filesystem=filesystem,
        )
        self._root = resolved_root
        self._filesystem = resolved_filesystem    

    @property
    def root(self) -> str:
        return self._root

    @property
    def filesystem(self) -> Optional[pyarrow.fs.FileSystem]:
        return self._filesystem

        
class Catalog:
    """
    Main catalog implementation for managing tables and namespaces.
    
    The Catalog class provides a high-level interface for catalog operations such as
    creating tables, reading data, and managing namespaces. It uses a PropertyCatalog
    instance for the underlying storage and filesystem operations.
    
    Attributes:
        _property_catalog (PropertyCatalog): The underlying property catalog used for
            storage and filesystem operations.
    """

    def __init__(self, properties: Optional[PropertyCatalog]):
        """
        Initialize a Catalog instance.
        
        Args:
            properties (PropertyCatalog): holds configuration for catalog. If not provided, 
                will initialize properties with default configuration
        """
        self.properties = properties if properties else PropertyCatalog()
    
    # table functions
    def write_to_table(
        data: Union[LocalTable, LocalDataset, DistributedDataset],
        table: str,
        namespace: Optional[str] = None,
        mode: TableWriteMode = TableWriteMode.AUTO,
        content_type: ContentType = ContentType.PARQUET,
        *args,
        **kwargs
    ) -> None:
        """Write local or distributed data to a table. Raises an error if the
        table does not exist and the table write mode is not CREATE or AUTO.

        When creating a table, all `create_table` parameters may be optionally
        specified as additional keyword arguments. When appending to, or replacing,
        an existing table, all `alter_table` parameters may be optionally specified
        as additional keyword arguments."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        # Check if table exists
        table_exists_flag = table_exists(table, namespace, catalog=catalog)

        # Handle different write modes
        if mode == TableWriteMode.CREATE:
            if table_exists_flag:
                raise ValueError(f"Table {namespace}.{table} already exists")
            # Create table if it doesn't exist
            create_table(
                table=table,
                namespace=namespace,
                catalog=catalog,
                *args,
                **kwargs
            )
        elif mode == TableWriteMode.APPEND:
            if not table_exists_flag:
                raise ValueError(f"Table {namespace}.{table} does not exist")
        elif mode == TableWriteMode.AUTO:
            if not table_exists_flag:
                # Create table if it doesn't exist
                create_table(
                    table=table,
                    namespace=namespace,
                    catalog=catalog,
                    *args,
                    **kwargs
                )

        # Get the dataset
        dataset = catalog.get_dataset(table, namespace)
        if not dataset:
            raise ValueError(f"Failed to get dataset for {namespace}.{table}")

        # Determine file format based on content type
        file_format = "parquet" if content_type == ContentType.PARQUET else "feather"

        # Create writer
        writer = dataset.writer(file_format=file_format)

        # Write data
        if isinstance(data, LocalTable):
            writer.write(data.to_batches())
        elif isinstance(data, LocalDataset):
            for table in data:
                writer.write(table.to_batches())
        elif isinstance(data, DistributedDataset):
            for batch in data.to_arrow_batches():
                writer.write(batch)

        # Flush changes
        writer.flush()


    def read_table(
        table: str, namespace: Optional[str] = None, *args, **kwargs
    ) -> DistributedDataset:
        """Read a table into a distributed dataset."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        # Check if table exists
        if not table_exists(table, namespace, catalog=catalog):
            raise ValueError(f"Table {namespace}.{table} does not exist")

        # Get the dataset
        dataset = catalog.get_dataset(table, namespace)
        if not dataset:
            raise ValueError(f"Failed to get dataset for {namespace}.{table}")

        # Create a scan with optional filtering
        query = kwargs.get("query")
        if query:
            if not isinstance(query, QueryExpression):
                query = QueryExpression()
            scan = dataset.scan(query=query)
        else:
            scan = dataset.scan()

        # Return as a distributed dataset
        return scan.to_ray_dataset()


    def alter_table(
        table: str,
        namespace: Optional[str] = None,
        lifecycle_state: Optional[LifecycleState] = None,
        schema_updates: Optional[Dict[str, Any]] = None,
        partition_updates: Optional[Dict[str, Any]] = None,
        sort_keys: Optional[SortScheme] = None,
        description: Optional[str] = None,
        properties: Optional[TableProperties] = None,
        *args,
        **kwargs
    ) -> None:
        """Alter table definition."""
        raise NotImplementedError("alter_table not implemented")


    def create_table(
        table: str,
        namespace: Optional[str] = None,
        lifecycle_state: Optional[LifecycleState] = None,
        schema: Optional[Schema] = None,
        partition_scheme: Optional[PartitionScheme] = None,
        sort_keys: Optional[SortScheme] = None,
        description: Optional[str] = None,
        table_properties: Optional[TableProperties] = None,
        namespace_properties: Optional[NamespaceProperties] = None,
        content_types: Optional[List[ContentType]] = None,
        fail_if_exists: bool = True,
        *args,
        **kwargs
    ) -> TableDefinition:
        """Create an empty table. Raises an error if the table already exists and
        `fail_if_exists` is True (default behavior)."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        # Check if table exists
        if table_exists(table, namespace, catalog=catalog):
            if fail_if_exists:
                raise ValueError(f"Table {namespace}.{table} already exists")
            return get_table(table, namespace, catalog=catalog)

        # Create namespace if it doesn't exist
        if not namespace_exists(namespace, catalog=catalog):
            create_namespace(
                namespace=namespace,
                properties=namespace_properties,
                catalog=catalog
            )

        # Extract merge keys from sort keys if provided
        merge_keys = []
        if sort_keys:
            merge_keys = [key.name for key in sort_keys.keys]

        # Create a table version through the storage layer
        storage_impl.create_table_version(
            namespace=namespace,
            table_name=table,
            schema=schema,
            partition_scheme=partition_scheme,
            sort_keys=sort_keys,
            table_version_description=description,
            table_description=description,
            table_properties=table_properties,
            lifecycle_state=lifecycle_state or LifecycleState.ACTIVE,
            catalog=catalog
        )

        # Create a dataset
        dataset = RivuletDataset(
            dataset_name=table,
            metadata_uri=catalog.root,
            namespace=namespace,
            filesystem=catalog.filesystem
        )

        # Add schema if provided
        if schema and schema.arrow:
            from deltacat.storage.rivulet import Schema as RivuletSchema
            rivulet_schema = RivuletSchema.from_pyarrow(schema.arrow, merge_keys)
            dataset.add_schema(rivulet_schema)

        # Cache the dataset
        catalog.cache_dataset(dataset, namespace)

        # Return table definition
        return TableDefinition(
            namespace=namespace,
            name=table,
            schema=schema,
            partition_scheme=partition_scheme,
            sort_keys=sort_keys,
            lifecycle_state=lifecycle_state or LifecycleState.ACTIVE,
            description=description or "",
            properties=table_properties or {},
        )


    def drop_table(
        table: str, namespace: Optional[str] = None, purge: bool = False, *args, **kwargs
    ) -> None:
        """Drop a table from the catalog and optionally purge it. Raises an error
        if the table does not exist."""
        raise NotImplementedError("drop_table not implemented")


    def refresh_table(table: str, namespace: Optional[str] = None, *args, **kwargs) -> None:
        """Refresh metadata cached on the Ray cluster for the given table."""
        raise NotImplementedError("refresh_table not implemented")


    def list_tables(
        namespace: Optional[str] = None, *args, **kwargs
    ) -> ListResult[TableDefinition]:
        """List a page of table definitions. Raises an error if the given namespace
        does not exist."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        # Check if namespace exists
        if not namespace_exists(namespace, catalog=catalog):
            raise ValueError(f"Namespace {namespace} does not exist")

        # Get tables from storage layer
        tables_list_result = storage_impl.list_tables(
            namespace=namespace,
            catalog=catalog
        )

        # Convert to TableDefinition objects
        table_definitions = []
        for table in tables_list_result.all_items():
            table_definition = get_table(
                table=table.table_name,
                namespace=namespace,
                catalog=catalog
            )
            if table_definition:
                table_definitions.append(table_definition)

        return ListResult(items=table_definitions, next_token=tables_list_result.next_token)


    def get_table(
        table: str, namespace: Optional[str] = None, *args, **kwargs
    ) -> Optional[TableDefinition]:
        """
        Get table definition metadata. Returns None if the given table does not exist.

        """
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        # Get table from storage layer
        storage_table = storage_impl.get_table(
            table_name=table,
            namespace=namespace,
            catalog=catalog
        )

        if not storage_table:
            return None

        # Get the latest table version
        table_version = storage_impl.get_latest_table_version(
            namespace=namespace,
            table_name=table,
            catalog=catalog
        )

        if not table_version:
            return None

        # Create and return table definition
        return TableDefinition(
            namespace=namespace,
            name=table,
            schema=table_version.schema,
            partition_scheme=table_version.partition_scheme,
            sort_keys=table_version.sort_scheme,
            lifecycle_state=table_version.state,
            description=storage_table.description or "",
            properties=storage_table.properties or {},
        )


    def truncate_table(
        table: str, namespace: Optional[str] = None, *args, **kwargs
    ) -> None:
        """Truncate table data. Raises an error if the table does not exist."""
        raise NotImplementedError("truncate_table not implemented")


    def rename_table(
        table: str, new_name: str, namespace: Optional[str] = None, *args, **kwargs
    ) -> None:
        """Rename a table."""
        raise NotImplementedError("rename_table not implemented")


    def table_exists(table: str, namespace: Optional[str] = None, *args, **kwargs) -> bool:
        """Returns True if the given table exists, False if not."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        namespace = namespace or default_namespace()

        return storage_impl.table_exists(
            table_name=table,
            namespace=namespace,
            catalog=catalog
        )


    # namespace functions
    def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
        """List a page of table namespaces."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        return storage_impl.list_namespaces(
            catalog=catalog
        )


    def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
        """Gets table namespace metadata for the specified table namespace. Returns
        None if the given namespace does not exist."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        return storage_impl.get_namespace(
            namespace=namespace,
            catalog=catalog
        )


    def namespace_exists(namespace: str, *args, **kwargs) -> bool:
        """Returns True if the given table namespace exists, False if not."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        return storage_impl.namespace_exists(
            namespace=namespace,
            catalog=catalog
        )


    def create_namespace(
        namespace: str, properties: NamespaceProperties, *args, **kwargs
    ) -> Namespace:
        """Creates a table namespace with the given name and properties. Returns
        the created namespace. Raises an error if the namespace already exists."""
        catalog = kwargs.get("catalog")
        if not isinstance(catalog, PropertyCatalog):
            raise ValueError("Catalog must be a PropertyCatalog instance")

        # Check if namespace already exists
        if namespace_exists(namespace, catalog=catalog):
            raise ValueError(f"Namespace {namespace} already exists")

        # Create namespace through storage layer
        return storage_impl.create_namespace(
            namespace=namespace,
            properties=properties,
            catalog=catalog
        )


    def alter_namespace(
        namespace: str,
        properties: Optional[NamespaceProperties] = None,
        new_namespace: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        """Alter table namespace definition."""
        raise NotImplementedError("alter_namespace not implemented")


    def drop_namespace(namespace: str, purge: bool = False, *args, **kwargs) -> None:
        """Drop the given namespace and all of its tables from the catalog,
        optionally purging them."""
        raise NotImplementedError("drop_namespace not implemented")


    def default_namespace(*args, **kwargs) -> str:
        """Returns the default namespace for the catalog."""
        return DEFAULT_NAMESPACE


    # catalog functions
    def initialize(*args, **kwargs) -> Optional[Any]:
        """Initializes the data catalog by forwarding the given arguments to
        `PropertyCatalog(*args, **kwargs)`. Returns the initialized
        PropertyCatalog."""
        catalog = PropertyCatalog(
            *args,
            **kwargs,
        )
        return catalog
