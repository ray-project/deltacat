from __future__ import annotations

from typing import Tuple, Optional

from deltacat.catalog.model.catalog import Catalog as DCCatalog
from deltacat.catalog.model.table_definition import TableDefinition

from daft.catalog import Catalog, Identifier, Table
from daft.dataframe import DataFrame
from daft.logical.schema import Schema
from deltacat.constants import DEFAULT_NAMESPACE


class DaftCatalog(Catalog):
    """
    Wrapper class to create a Daft catalog from a DeltaCAT catalog.

    The initialization of DeltaCAT and Daft catalogs is managed in `deltacat.catalog.catalog.py`. The user
    is just expected to initialize catalogs through the DeltaCAT public interface (init / put_catalog).

    TODO (mccember) in follow up PR we need to consider how to keep the DeltaCAT Catalogs class and Daft session in sync,
      and the user-facing entrypoint to get a Daft catalog

    This class itself expects a `Catalog` and will invoke the underlying implementation
    similar to `deltacat.catalog.delegate.py`, like:
      catalog.impl.create_namespace(namespace, inner=catalog.inner)

    We cannot route calls through the higher level catalog registry / delegate since this wrapper class is at a lower
     layer and does not manage registering catalogs.
    """

    def __init__(self, catalog: DCCatalog, name: str):
        """
        Initialize given DeltaCAT catalog. This catalog is also registered with DeltaCAT (via deltacat.put_catalog) given the provided Name

        :param catalog: DeltaCAT Catalog object. If None, the catalog will be fetched from `deltacat.Catalogs`
            given the catalog name.

        :param name: Name of DeltaCAT catalog. If the name is not yet registered with `deltacat.Catalogs`,
            it will be registered upon creation to ensure that the DeltaCAT and Daft catalogs keep in sync.

        :param kwargs: Additional keyword arguments passed to deltacat.get_catalog or deltacat.put_catalog,
                       such as 'namespace' for tests.
        """
        self.dc_catalog = catalog
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    ###
    # create_*
    ###
    def create_namespace(self, identifier: Identifier | str):
        """Create a new namespace in the catalog."""
        if isinstance(identifier, Identifier):
            identifier = str(identifier)
        self.dc_catalog.impl.create_namespace(identifier, inner=self.dc_catalog.inner)

    def create_table(
        self, identifier: Identifier | str, source: Schema | DataFrame, **kwargs
    ) -> Table:
        """
        Create a DeltaCAT table via Daft catalog API

        End users calling create_table through the daft table API may provide kwargs which will be plumbed through
        to deltacat create_table. For full list of keyword arguments accepted by create_table.

        Note: as of 4/22, Daft create_table does not yet support kwargs. Tracked at: https://github.com/Eventual-Inc/Daft/issues/4195

        :param identifier: Daft table identifier. Sequence of strings of the format (namespace) or (namespace, table)
            or (namespace, table, table version). If this is a string, it is a dot delimited string of the same format.
            Identifiers can be created either like Identifier("namespace", "table", "version") OR
                Identifier.from_str("namespace.table.version")

        :param source: a TableSource, either a Daft DataFrame, Daft Schema, or str (filesystem path)
        """
        if isinstance(source, DataFrame):
            return self._create_table_from_df(identifier, source)
        elif isinstance(source, Schema):
            return self._create_table_from_schema(identifier, source)
        else:
            raise Exception(
                f"Expected table source to be Schema or DataFrame. Found: {type(source)}"
            )

    def _create_table_from_df(
        self, ident: Identifier | str, source: DataFrame, **kwargs
    ) -> Table:
        """
        Create a table from a DataFrame.
        """
        t = self._create_table_from_schema(ident, source.schema(), **kwargs)
        # TODO (mccember) append data upon creation
        return t

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
        table_def = self.dc_catalog.impl.create_table(
            name,
            namespace=namespace,
            version=version,
            schema=deltacat_schema,
            inner=self.dc_catalog.inner,
            **kwargs,
        )

        return DaftTable._from_obj(table_def)

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

        table_def = self.dc_catalog.impl.get_table(
            table,
            namespace=namespace,
            table_version=version,
            inner=self.dc_catalog.inner,
            **kwargs,
        )

        if not table_def:
            raise ValueError(f"Table {identifier} not found")

        return DaftTable._from_obj(table_def)

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

        if isinstance(ident, str):
            ident = Identifier.from_str(ident)

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

    def _convert_schema_to_deltacat(self, schema: Schema):
        """Convert Daft schema to DeltaCAT schema.
        For now, just use PyArrow schema as intermediary
        TODO look into how enhancements on schema can be propagated between Daft<=>DeltaCAT
        """
        from deltacat.storage.model.schema import Schema as DeltaCATSchema

        return DeltaCATSchema.of(schema=schema.to_pyarrow_schema())


class DaftTable(Table):
    """
    Wrapper class to create a Daft table from a DeltaCAT table
    """

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
    def _from_obj(obj: object) -> DaftTable:
        """Returns a DeltaCATTable if the given object can be adapted so."""
        if isinstance(obj, TableDefinition):
            t = DaftTable.__new__(DaftTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported DeltaCAT table type: {type(obj)}")

    def read(self, **options) -> DataFrame:
        raise NotImplementedError("Not implemented")

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        raise NotImplementedError("Not implemented")
