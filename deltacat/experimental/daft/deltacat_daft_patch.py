"""DeltaCAT integration with Daft.

This file provides integration between DeltaCAT catalogs and Daft catalogs.
"""

from __future__ import annotations

from typing import Tuple, Optional

import deltacat
from deltacat.catalog.model.catalog import Catalog as DCCatalog
from deltacat.catalog.model.table_definition import TableDefinition

from daft.catalog import Catalog, Identifier, Table, TableSource
from daft.dataframe import DataFrame
from daft.logical.schema import Schema
from deltacat.constants import DEFAULT_CATALOG, DEFAULT_NAMESPACE


class DeltaCATCatalog(Catalog):
    _inner: DCCatalog

    def __init__(self,
                 *,
                 name: str = DEFAULT_CATALOG,
                 catalog: DCCatalog = None,
                 **kwargs):
        """
        Initialize given DeltaCAT catalog.

        This catalog is also registered with DeltaCAT (via deltacat.put_catalog) given the provided Name

        :param catalog: DeltaCAT Catalog object. If None, the catalog will be fetched from `deltacat.Catalogs` given the catalog name.

        :param name: Name of DeltaCAT catalog. If the name is not yet registered with `deltacat.Catalogs`, it will be registered upon creation to ensure that the DeltaCAT and Daft catalogs keep in sync.

        :param kwargs: Additional keyword arguments passed to deltacat.get_catalog or deltacat.put_catalog,
                       such as 'namespace' for tests.
        """
        if catalog is None:
            # Only name is provided, try to get the catalog from DeltaCAT
            try:
                self._inner = deltacat.get_catalog(name, **kwargs)
            except KeyError:
                raise ValueError(f"No catalog with name '{name}' found in DeltaCAT. Please provide a catalog instance or ensure a catalog with this name exists.")
        else:
            # Both name and catalog are provided
            try:
                # Check if catalog already exists in DeltaCAT
                existing_catalog = deltacat.get_catalog(name, **kwargs)
                # Validate that the existing catalog is equivalent to the provided one
                if existing_catalog != catalog:
                    raise ValueError(f"A catalog with name '{name}' already exists in DeltaCAT but has a different implementation.")
                # Use the existing catalog for consistency
                self._inner = existing_catalog
            except KeyError:
                # Catalog doesn't exist, add it
                deltacat.put_catalog(name, catalog=catalog, **kwargs)
                self._inner = catalog


    @staticmethod
    def _from_obj(obj: object) -> DeltaCATCatalog:
        """Returns a DeltaCATCatalog instance if the given object can be adapted so."""
        if isinstance(obj, DCCatalog):
            c = DeltaCATCatalog.__new__(DeltaCATCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported DeltaCAT catalog type: {type(obj)}")

    @property
    def name(self) -> str:
        """Return the catalog name."""
        # DeltaCAT catalogs don't have a standard name property, so use the class name
        return "DeltaCATCatalog"

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        """Create a new namespace in the catalog."""
        if isinstance(identifier, Identifier):
            identifier = str(identifier)

        self._inner.impl.create_namespace(
            namespace=identifier, inner=self._inner.inner
        )

    def create_table(self, identifier: Identifier | str, source: TableSource | object, **kwargs) -> Table:
        """
        Create a DeltaCAT table via Daft catalog API

        Refer to `deltacat.create_table` for full list of keyword arguments accepted by create_table.

        This implementation is modeled after daft/catalog/_iceberg, in particular that the TableSource
        input may be either a str/DataFrame/schema
        
        TODO (questions to discuss with Daft):
        1. Can we expect that `identifier` is always populated? 
            In that case, what is the purpose of `source` being a str?
        2. Do we expect end users to call this function? Does it make sense to plumb kwargs through to DeltaCAT createTable?
        3. What format do we expect identifier strings to be in?

        :param identifier: Daft table identifier. Sequence of strings of the format (namespace) or (namespace, table)
            or (namespace, table, table version)
        :param source: a TableSource, either a Daft DataFrame, Daft Schema, or str (expected to be table identifier?)
        """

        if isinstance(source, DataFrame):
            return self._create_table_from_df(identifier, source)
        elif isinstance(source, str) or isinstance(source, Identifier):
            return self._create_table_from_path(identifier, source)
        elif isinstance(source, Schema):
            return self._create_table_from_schema(identifier, source)
        else:
            raise Exception(f"Unknown table source: {source}")

    def _create_table_from_df(self, ident: Identifier | str, source: DataFrame, **kwargs) -> Table:
        """
        Create a table from a DataFrame.
        """
        t = self._create_table_from_schema(ident, source.schema(), **kwargs)
        # TODO (mccember) append data upon creation
        return t

    def _create_table_from_path(self, ident: Identifier | str, source: str, **kwargs) -> Table:
        """Create a table from a path."""
        raise ValueError("table from path not yet supported")

    def _create_table_from_schema(self, ident: Identifier | str, source: Schema, **kwargs) -> Table:
        """
        Create a table from a schema.
        """
        namespace, name, version = self._extract_namespace_name_version(ident)

        # Convert the Daft schema to a DeltaCAT schema
        # This is a simplified version, would need to be enhanced for production
        deltacat_schema = self._convert_schema_to_deltacat(source)

        # Create the table in DeltaCAT
        table_def = self._inner.impl.create_table(
            name=name,
            namespace=namespace,
            version=version,
            schema=deltacat_schema,
            inner=self._inner.inner,
            **kwargs
        )
        
        return DeltaCATTable._from_obj(table_def)

    def _extract_namespace_name_version(self, ident: Identifier | str)-> Tuple[str, str, Optional[str]]:
        """
        Extract namespace, name,version from identifier

        Returns a 3-tuple. If no namespace is provided, uses DeltaCAT defualt namespace
        """
        default_namespace = DEFAULT_NAMESPACE
        if isinstance(ident, Identifier):
            if len(ident)==1:
                return (default_namespace, ident[0], None)
            elif len(ident)==2:
                return (ident[0], ident[1], None)
            elif len(ident)==3:
                return (ident[0], ident[1], ident[2])
            else:
                raise ValueError(f"Expected table identifier to be in format (table) or (namespace, table)"
                                 f"or (namespace, table, version). Found: {ident}")
        elif isinstance(ident, str):
            # TODO (mccember) implement once format of string confirmed with Daft
            raise ValueError("Usage of str identifier for DeltaCAT Daft tables not currently support")
        else:
            raise ValueError(f"Expected identifier for createTable to be either Daft Identifier or str. "
                             f"Provided identifier was class: {type(ident)}")

    def _convert_schema_to_deltacat(self, schema: Schema):
        """Convert Daft schema to DeltaCAT schema.
        For now, just use PyArrow schema as intermediary
        TODO look into how enhancements on schema can be propagated between Daft<=>DeltaCAT
        """
        from deltacat.storage.model.schema import Schema as DeltaCATSchema, Field
        return DeltaCATSchema.of(schema=schema.to_pyarrow_schema())

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        raise NotImplementedError()

    def drop_table(self, identifier: Identifier | str):
        raise NotImplementedError()

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str) -> Table:
        """Get a table by identifier."""
        if isinstance(identifier, Identifier):
            namespace = str(identifier[0]) if len(identifier) > 1 else None
            table_name = str(identifier[-1])
        else:
            # Split the string by dots to get namespace and table
            parts = str(identifier).split(".")
            if len(parts) > 1:
                namespace = parts[0]
                table_name = parts[-1]
            else:
                namespace = None
                table_name = parts[0]
                
        table_def = self._inner.impl.get_table(
            name=table_name,
            namespace=namespace,
            inner=self._inner.inner
        )
        
        if not table_def:
            raise ValueError(f"Table {identifier} not found")
            
        return DeltaCATTable._from_obj(table_def)

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List namespaces in the catalog."""
        # DeltaCAT doesn't have pattern matching, so we ignore the pattern parameter
        namespaces = self._inner.impl.list_namespaces(inner=self._inner.inner)
        return [Identifier(ns.name) for ns in namespaces.all_items()]

    def list_tables(self, pattern: str | None = None) -> list[str]:
        """List tables in the catalog."""
        # Determine namespace from pattern or use default
        namespace = None
        if pattern:
            # Simple pattern handling: assume pattern is "namespace.*"
            if pattern.endswith(".*"):
                namespace = pattern[:-2]
        
        tables = self._inner.impl.list_tables(namespace=namespace, inner=self._inner.inner)
        return [table_def.table.name for table_def in tables.all_items()]


class DeltaCATTable(Table):
    _inner: TableDefinition
    
    _read_options = set()
    _write_options = set()

    def __init__(self, inner: TableDefinition):
        self._inner = inner

    @property
    def name(self) -> str:
        """Return the table name."""
        return self._inner.table.name

    @staticmethod
    def _from_obj(obj: object) -> DeltaCATTable:
        """Returns a DeltaCATTable if the given object can be adapted so."""
        if isinstance(obj, TableDefinition):
            t = DeltaCATTable.__new__(DeltaCATTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported DeltaCAT table type: {type(obj)}")

    def read(self, **options) -> DataFrame:
        """Read the table into a DataFrame."""
        Table._validate_options("DeltaCAT read", options, DeltaCATTable._read_options)
        
        # In a real implementation, this would use DeltaCAT's reading capabilities
        # to fetch the data and convert it to a Daft DataFrame.
        # For now, we'll just create an empty DataFrame.
        return DataFrame._from_pylist([])

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        """Write data to the table."""
        self._validate_options("DeltaCAT write", options, DeltaCATTable._write_options)
        
        # In a real implementation, this would convert the Daft DataFrame
        # to a format that DeltaCAT can write.
        # For now, this is just a placeholder.
        pass


# Add monkey patching to extend Daft's Catalog class with from_deltacat method
def _monkey_patch_daft_catalog():
    """Monkey patch the Daft Catalog class with DeltaCAT support."""
    from daft.catalog import Catalog as DaftCatalog
    
    # Only add the method if it doesn't exist yet
    if not hasattr(DaftCatalog, "from_deltacat"):
        @staticmethod
        def from_deltacat(catalog: object) -> Catalog:
            """Creates a Daft Catalog instance from a DeltaCAT catalog.
            
            Args:
                catalog (object): DeltaCAT catalog object
                
            Returns:
                Catalog: new daft catalog instance from the DeltaCAT catalog object.
            """
            return DeltaCATCatalog._from_obj(catalog)
            
        DaftCatalog.from_deltacat = from_deltacat

    # Also patch the Table class
    from daft.catalog import Table as DaftTable
    
    if not hasattr(DaftTable, "from_deltacat"):
        @staticmethod
        def from_deltacat(table: object) -> Table:
            """Creates a Daft Table instance from a DeltaCAT table.
            
            Args:
                table (object): DeltaCAT table object
                
            Returns:
                Table: new daft table instance from the DeltaCAT table object.
            """
            return DeltaCATTable._from_obj(table)
            
        DaftTable.from_deltacat = from_deltacat

    # Also update the Catalog._from_obj method to try DeltaCAT as well
    original_from_obj = DaftCatalog._from_obj
    
    @staticmethod
    def new_from_obj(obj: object) -> Catalog:
        """Returns a Daft Catalog from a supported object type or raises a ValueError."""
        try:
            return original_from_obj(obj)
        except ValueError:
            try:
                return DaftCatalog.from_deltacat(obj)
            except (ValueError, ImportError):
                raise ValueError(
                    f"Unsupported catalog type: {type(obj)}; please ensure all required extra dependencies are installed."
                )
    
    DaftCatalog._from_obj = new_from_obj


# Apply the monkey patching when this module is imported
_monkey_patch_daft_catalog()