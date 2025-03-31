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
    def __init__(
        self, *, catalog: DCCatalog = None, name: str = DEFAULT_CATALOG, **kwargs
    ):
        """
        Initialize given DeltaCAT catalog. This catalog is also registered with DeltaCAT (via deltacat.put_catalog) given the provided Name

        NOTE: we do NOT persist the catalog, but rather rely on DeltaCAT's put_catalog/get_catalog capabilities
            to persist the catalog. This class only persists catalog name

        :param catalog: DeltaCAT Catalog object. If None, the catalog will be fetched from `deltacat.Catalogs`
            given the catalog name.

        :param name: Name of DeltaCAT catalog. If the name is not yet registered with `deltacat.Catalogs`,
            it will be registered upon creation to ensure that the DeltaCAT and Daft catalogs keep in sync.

        :param kwargs: Additional keyword arguments passed to deltacat.get_catalog or deltacat.put_catalog,
                       such as 'namespace' for tests.

        TODO (questions for Daft):
        1. Should we raise a NotImplementedError and move this implementation to _from_obj, to follow pattern
            of iceberg/unity catalogs in daft?
        """
        self._name = name
        if catalog is None:
            # Only name is provided, try to fetch the catalog from DeltaCAT to assert it exists
            try:
                deltacat.get_catalog(name, **kwargs)
            except KeyError:
                raise ValueError(
                    f"No catalog with name '{name}' found in DeltaCAT. Please provide a catalog instance or ensure a catalog with this name exists."
                )
        else:
            # Both name and catalog are provided
            try:
                # Check if catalog already exists in DeltaCAT and equal to the catalog provided
                existing_catalog = deltacat.get_catalog(name, **kwargs)
                # Validate that the existing catalog is equivalent to the provided one
                if existing_catalog != catalog:
                    raise ValueError(
                        f"A catalog with name '{name}' already exists in DeltaCAT but has a different implementation."
                    )
            except KeyError:
                # Catalog doesn't exist, add it
                deltacat.put_catalog(name, catalog=catalog, **kwargs)

    @property
    def name(self) -> str:
        """
        Return the catalog name
        """
        return self._name

    ###
    # create_*
    ###
    def create_namespace(self, identifier: Identifier | str):
        """Create a new namespace in the catalog."""
        if isinstance(identifier, Identifier):
            identifier = str(identifier)
        deltacat.create_namespace(namespace=identifier, catalog=self._name)

    def create_table(
        self, identifier: Identifier | str, source: TableSource | object, **kwargs
    ) -> Table:
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

    def _create_table_from_df(
        self, ident: Identifier | str, source: DataFrame, **kwargs
    ) -> Table:
        """
        Create a table from a DataFrame.
        """
        t = self._create_table_from_schema(ident, source.schema(), **kwargs)
        # TODO (mccember) append data upon creation
        return t

    def _create_table_from_path(
        self, ident: Identifier | str, source: str, **kwargs
    ) -> Table:
        """Create a table from a path."""
        raise ValueError("table from path not yet supported")

    def _create_table_from_schema(
        self, ident: Identifier | str, source: Schema, **kwargs
    ) -> Table:
        """
        Create a table from a schema.
        """
        namespace, name, version = self._extract_namespace_name_version(ident)

        # Convert the Daft schema to a DeltaCAT schema
        # This is a simplified version, would need to be enhanced for production
        deltacat_schema = self._convert_schema_to_deltacat(source)

        # Create the table in DeltaCAT
        table_def = deltacat.create_table(
            name=name,
            namespace=namespace,
            version=version,
            schema=deltacat_schema,
            catalog=self._name,
            **kwargs,
        )

        return DeltaCATTable._from_obj(table_def)

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

    def get_table(self, identifier: Identifier | str, **kwargs) -> Table:
        namespace, table, version = self._extract_namespace_name_version(identifier)

        # TODO validate this works with iceberg if stream format not set
        table_def = deltacat.get_table(
            name=table,
            namespace=namespace,
            table_version=version,
            catalog=self._name,
            **kwargs,
        )

        if not table_def:
            raise ValueError(f"Table {identifier} not found")

        return DeltaCATTable._from_obj(table_def)

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        raise NotImplementedError("Not implemented")

    def list_tables(self, pattern: str | None = None) -> list[str]:
        raise NotImplementedError("Not implemented")

    def _extract_namespace_name_version(
        self, ident: Identifier | str
    ) -> Tuple[str, str, Optional[str]]:
        """
        Extract namespace, name,version from identifier

        Returns a 3-tuple. If no namespace is provided, uses DeltaCAT defualt namespace
        """
        default_namespace = DEFAULT_NAMESPACE
        if isinstance(ident, Identifier):
            if len(ident) == 1:
                return (default_namespace, ident[0], None)
            elif len(ident) == 2:
                return (ident[0], ident[1], None)
            elif len(ident) == 3:
                return (ident[0], ident[1], ident[2])
            else:
                raise ValueError(
                    f"Expected table identifier to be in format (table) or (namespace, table)"
                    f"or (namespace, table, version). Found: {ident}"
                )
        elif isinstance(ident, str):
            # TODO (mccember) implement once format of string confirmed with Daft
            raise ValueError(
                "Usage of str identifier for DeltaCAT Daft tables not currently support"
            )
        else:
            raise ValueError(
                f"Expected identifier for createTable to be either Daft Identifier or str. "
                f"Provided identifier was class: {type(ident)}"
            )

    def _convert_schema_to_deltacat(self, schema: Schema):
        """Convert Daft schema to DeltaCAT schema.
        For now, just use PyArrow schema as intermediary
        TODO look into how enhancements on schema can be propagated between Daft<=>DeltaCAT
        """
        from deltacat.storage.model.schema import Schema as DeltaCATSchema

        return DeltaCATSchema.of(schema=schema.to_pyarrow_schema())


class DeltaCATTable(Table):
    _inner: TableDefinition

    _read_options = set()
    _write_options = set()

    def __init__(self, inner: TableDefinition):
        self._inner = inner

    @property
    def name(self) -> str:
        """Return the table name."""
        return self._inner.table_version.table_name

    @staticmethod
    def _from_obj(obj: object) -> DeltaCATTable:
        """Returns a DeltaCATTable if the given object can be adapted so."""
        if isinstance(obj, TableDefinition):
            t = DeltaCATTable.__new__(DeltaCATTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported DeltaCAT table type: {type(obj)}")

    def read(self, **options) -> DataFrame:
        raise NotImplementedError("Not implemented")

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        raise NotImplementedError("Not implemented")


# Add monkey patching to extend Daft's Catalog class with from_deltacat method
def _monkey_patch_daft_catalog():
    """Monkey patch the Daft Catalog class with DeltaCAT support."""
    from daft.catalog import Catalog as DaftCatalog

    # Only add the method if it doesn't exist yet
    if not hasattr(DaftCatalog, "from_deltacat"):

        @staticmethod
        def from_deltacat(catalog: object, name: str = None, **kwargs) -> Catalog:
            """Creates a Daft Catalog instance from a DeltaCAT catalog.

            Args:
                catalog (object): DeltaCAT catalog object

            Returns:
                Catalog: new daft catalog instance from the DeltaCAT catalog object.
            """
            return DeltaCATCatalog(catalog=catalog, name=name, **kwargs)

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
