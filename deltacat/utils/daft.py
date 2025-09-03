import logging
from typing import Optional, List, Any, Dict, Callable, Iterator, Union

from daft.daft import (
    StorageConfig,
    PartitionField,
    Pushdowns as DaftRustPushdowns,
    ScanTask,
    FileFormatConfig,
    ParquetSourceConfig,
    PartitionTransform as DaftTransform,
    PartitionField as DaftPartitionField,
)
from daft.expressions import Expression as DaftExpression
from daft.expressions.visitor import PredicateVisitor
from pyarrow import Field as PaField

import daft
import ray
from daft import (
    TimeUnit,
    DataFrame,
    Schema as DaftSchema,
    DataType,
)
from daft.logical.schema import Field as DaftField
from daft.recordbatch import read_parquet_into_pyarrow
from daft.io import (
    IOConfig,
    S3Config,
)
from daft.io.scan import (
    ScanOperator,
    make_partition_field,
)
import pyarrow as pa
import pyarrow.fs as pafs
from fsspec import AbstractFileSystem

from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.schema import coerce_pyarrow_table_to_schema
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.aws.constants import (
    BOTO_MAX_RETRIES,
    DAFT_MAX_S3_CONNECTIONS_PER_FILE,
    AWS_REGION,
)
from deltacat.constants import DEFAULT_FILE_READ_TIMEOUT_MS
from deltacat.utils.performance import timed_invocation

from deltacat.types.partial_download import (
    PartialFileDownloadParams,
)

# Import directly from storage model modules to avoid circular import
from deltacat.storage.model.transform import (
    Transform,
    IdentityTransform,
    HourTransform,
    DayTransform,
    MonthTransform,
    YearTransform,
    BucketTransform,
    BucketingStrategy,
    TruncateTransform,
    TruncateStrategy,
)
from deltacat.storage.model.partition import PartitionKey
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.interop import ModelMapper
from deltacat.storage.model.expression import (
    Expression,
    Reference,
    Literal,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanEqual,
    LessThanEqual,
    And,
    Or,
    Not,
    IsNull,
)
from deltacat.storage.model.scan.push_down import (
    PartitionFilter,
    Pushdown as DeltaCatPushdown,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def translate_pushdown(pushdown: DaftRustPushdowns) -> DeltaCatPushdown:
    """
    Helper method to translate a Daft Pushdowns object into a Deltacat Pushdown.
    Args:
        pushdown: Daft Daft Pushdowns object
    Returns:
        Pushdown: Deltacat Pushdown object with translated filters
    """
    translator = DaftToDeltacatVisitor()

    partition_filters = None
    if pushdown.partition_filters is not None:
        daft_expr = DaftExpression._from_pyexpr(pushdown.partition_filters)
        partition_filters = PartitionFilter.of(translator.visit(daft_expr))

    filters = None
    if pushdown.filters is not None:
        daft_expr = DaftExpression._from_pyexpr(pushdown.filters)
        # TODO: support deltacat row filters
        # filters = RowFilter.of(translator.visit(daft_expr))

    columns = None
    limit = None

    return DeltaCatPushdown.of(
        partition_filter=partition_filters,
        column_filter=columns,
        row_filter=filters,
        limit=limit,
    )


class DaftToDeltacatVisitor(PredicateVisitor[Expression]):
    """PredicateVisitor implementation to translate Daft Expressions into Deltacat Expressions"""

    def visit_col(self, name: str) -> Expression:
        return Reference.of(name)

    def visit_lit(self, value: Any) -> Expression:
        return Literal.of(value)

    def visit_cast(self, expr: DaftExpression, dtype: DataType) -> Expression:
        # deltacat expressions do not support explicit casting
        # pyarrow should handle any type casting
        return self.visit(expr)

    def visit_alias(self, expr: DaftExpression, alias: str) -> Expression:
        return self.visit(expr)

    def visit_function(self, name: str, args: List[DaftExpression]) -> Expression:
        # TODO: Add Deltacat expression function support
        raise ValueError("Function not supported")

    def visit_and(self, left: DaftExpression, right: DaftExpression) -> Expression:
        """Visit an 'and' expression."""
        return And.of(self.visit(left), self.visit(right))

    def visit_or(self, left: DaftExpression, right: DaftExpression) -> Expression:
        """Visit an 'or' expression."""
        return Or.of(self.visit(left), self.visit(right))

    def visit_not(self, expr: DaftExpression) -> Expression:
        """Visit a 'not' expression."""
        return Not.of(self.visit(expr))

    def visit_equal(self, left: DaftExpression, right: DaftExpression) -> Expression:
        """Visit an 'equals' comparison predicate."""
        return Equal.of(self.visit(left), self.visit(right))

    def visit_not_equal(
        self, left: DaftExpression, right: DaftExpression
    ) -> Expression:
        """Visit a 'not equals' comparison predicate."""
        return NotEqual.of(self.visit(left), self.visit(right))

    def visit_less_than(
        self, left: DaftExpression, right: DaftExpression
    ) -> Expression:
        """Visit a 'less than' comparison predicate."""
        return LessThan.of(self.visit(left), self.visit(right))

    def visit_less_than_or_equal(
        self, left: DaftExpression, right: DaftExpression
    ) -> Expression:
        """Visit a 'less than or equal' comparison predicate."""
        return LessThanEqual.of(self.visit(left), self.visit(right))

    def visit_greater_than(
        self, left: DaftExpression, right: DaftExpression
    ) -> Expression:
        """Visit a 'greater than' comparison predicate."""
        return GreaterThan.of(self.visit(left), self.visit(right))

    def visit_greater_than_or_equal(
        self, left: DaftExpression, right: DaftExpression
    ) -> Expression:
        """Visit a 'greater than or equal' comparison predicate."""
        return GreaterThanEqual.of(self.visit(left), self.visit(right))

    def visit_between(
        self, expr: DaftExpression, lower: DaftExpression, upper: DaftExpression
    ) -> Expression:
        """Visit a 'between' predicate."""
        # Implement BETWEEN as lower <= expr <= upper
        lower_bound = LessThanEqual.of(self.visit(lower), self.visit(expr))
        upper_bound = LessThanEqual.of(self.visit(expr), self.visit(upper))
        return And.of(lower_bound, upper_bound)

    def visit_is_in(
        self, expr: DaftExpression, items: list[DaftExpression]
    ) -> Expression:
        """Visit an 'is_in' predicate."""
        # For empty list, return false literal
        if not items:
            return Literal(pa.scalar(False))

        # Implement IN as a series of equality checks combined with OR
        visited_expr = self.visit(expr)
        equals_exprs = [Equal.of(visited_expr, self.visit(item)) for item in items]

        # Combine with OR
        result = equals_exprs[0]
        for eq_expr in equals_exprs[1:]:
            result = Or.of(result, eq_expr)

        return result

    def visit_is_null(self, expr: DaftExpression) -> Expression:
        """Visit an 'is_null' predicate."""
        return IsNull.of(self.visit(expr))

    def visit_not_null(self, expr: DaftExpression) -> Expression:
        """Visit an 'not_null' predicate."""
        # NOT NULL is implemented as NOT(IS NULL)
        return Not.of(IsNull.of(self.visit(expr)))


class DeltaCatScanOperator(ScanOperator):
    def __init__(self, table, storage_config: StorageConfig) -> None:
        # Import inside method to avoid circular import
        from deltacat.catalog.model.table_definition import TableDefinition

        if not isinstance(table, TableDefinition):
            raise TypeError("table must be a TableDefinition instance")
        super().__init__()
        self.table = table
        self._schema = self._infer_schema()
        self.partition_keys = self._infer_partition_keys()
        self.storage_config = storage_config

    def schema(self) -> DaftSchema:
        return self._schema

    def name(self) -> str:
        return "DeltaCatScanOperator"

    def display_name(self) -> str:
        return f"DeltaCATScanOperator({self.table.table.namespace}.{self.table.table.table_name})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self.partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys}",
            f"Storage config = {self.storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: DaftRustPushdowns) -> Iterator[ScanTask]:
        dc_pushdown = translate_pushdown(pushdowns)
        dc_scan_plan = self.table.create_scan_plan(pushdown=dc_pushdown)
        scan_tasks = []
        file_format_config = FileFormatConfig.from_parquet_config(
            # maybe this: ParquetSourceConfig(field_id_mapping=self._field_id_mapping)
            ParquetSourceConfig()
        )
        for dc_scan_task in dc_scan_plan.scan_tasks:
            for data_file in dc_scan_task.data_files():
                st = ScanTask.catalog_scan_task(
                    file=data_file.file_path,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    storage_config=self.storage_config,
                    pushdowns=pushdowns,
                )
                scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def _infer_schema(self) -> DaftSchema:

        if not (
            self.table and self.table.table_version and self.table.table_version.schema
        ):
            raise RuntimeError(
                f"Failed to infer schema for DeltaCAT Table "
                f"{self.table.table.namespace}.{self.table.table.table_name}"
            )

        return DaftSchema.from_pyarrow_schema(self.table.table_version.schema.arrow)

    def _infer_partition_keys(self) -> list[PartitionField]:
        if not (
            self.table
            and self.table.table_version
            and self.table.table_version.partition_scheme
            and self.table.table_version.schema
        ):
            raise RuntimeError(
                f"Failed to infer partition keys for DeltaCAT Table "
                f"{self.table.table.namespace}.{self.table.table.table_name}"
            )

        schema = self.table.table_version.schema
        partition_keys = self.table.table_version.partition_scheme.keys
        if not partition_keys:
            return []

        partition_fields = []
        for key in partition_keys:
            field = DaftPartitionKeyMapper.unmap(key, schema)
            # Assert that the returned value is not None.
            assert field is not None, f"Unmapping failed for key {key}"
            partition_fields.append(field)

        return partition_fields


def read_csv(
    path: Union[str, List[str]],
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    content_type: Optional[str] = None,
    **read_kwargs,
) -> DataFrame:
    """
    Read a CSV file into a Daft DataFrame.

    Args:
        path: Path to the CSV file
        filesystem: Optional filesystem to use
        fs_open_kwargs: Optional filesystem open kwargs
        content_encoding: Content encoding (IDENTITY or GZIP supported)
        content_type: Optional content type (PARQUET, JSON, CSV, etc.)
        **read_kwargs: Additional arguments passed to daft.read_csv

    Returns:
        DataFrame: The Daft DataFrame
    """
    logger.debug(
        f"Reading CSV file {path} into Daft DataFrame with kwargs: {read_kwargs}"
    )

    # If content_type is provided, add appropriate reader kwargs
    if content_type is not None:
        content_kwargs = content_type_to_reader_kwargs(content_type)
        read_kwargs.update(content_kwargs)
        logger.debug(f"Added content type kwargs for {content_type}: {content_kwargs}")

    # Files should now be written with proper extensions, so we can read them directly
    logger.debug(f"Reading CSV with Daft from: {path}")
    df, latency = timed_invocation(daft.read_csv, path, **read_kwargs)

    logger.debug(f"Time to read CSV {path} into Daft DataFrame: {latency}s")
    return df


def read_json(
    path: Union[str, List[str]],
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> DataFrame:
    """
    Read a JSON file into a Daft DataFrame.

    Args:
        path: Path to the JSON file (supports line-delimited JSON)
        filesystem: Optional filesystem to use
        fs_open_kwargs: Optional filesystem open kwargs
        content_encoding: Content encoding (IDENTITY or GZIP supported)
        **read_kwargs: Additional arguments passed to daft.read_json

    Returns:
        DataFrame: The Daft DataFrame
    """
    logger.debug(
        f"Reading JSON file {path} into Daft DataFrame with kwargs: {read_kwargs}"
    )

    # Files should now be written with proper extensions, so we can read them directly
    logger.debug(f"Reading JSON with Daft from: {path}")
    df, latency = timed_invocation(daft.read_json, path, **read_kwargs)

    logger.debug(f"Time to read JSON {path} into Daft DataFrame: {latency}s")
    return df


def read_parquet(
    path: Union[str, List[str]],
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> DataFrame:
    """
    Read a Parquet file into a Daft DataFrame.

    Args:
        path: Path to the Parquet file
        filesystem: Optional filesystem to use
        fs_open_kwargs: Optional filesystem open kwargs
        content_encoding: Content encoding (IDENTITY or GZIP supported)
        **read_kwargs: Additional arguments passed to daft.read_parquet

    Returns:
        DataFrame: The Daft DataFrame
    """
    logger.debug(
        f"Reading Parquet file {path} into Daft DataFrame with kwargs: {read_kwargs}"
    )
    logger.debug(f"Reading Parquet with Daft from: {path}")
    df, latency = timed_invocation(daft.read_parquet, path=path, **read_kwargs)
    logger.debug(f"Time to read Parquet {path} into Daft DataFrame: {latency}s")
    return df


# Map content types to their respective Daft read functions
CONTENT_TYPE_TO_READ_FN: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: read_csv,
    ContentType.TSV.value: read_csv,
    ContentType.CSV.value: read_csv,
    ContentType.PSV.value: read_csv,
    ContentType.PARQUET.value: read_parquet,
    ContentType.JSON.value: read_json,
}


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    """
    Returns reader kwargs for the given content type when reading with Daft.
    """
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "delimiter": "\t",
            "has_headers": False,
            "double_quote": False,
            "allow_variable_columns": True,
        }
    if content_type == ContentType.TSV.value:
        return {
            "delimiter": "\t",
            "has_headers": False,
            "allow_variable_columns": True,
        }
    if content_type == ContentType.CSV.value:
        return {
            "delimiter": ",",
            "has_headers": False,
            "allow_variable_columns": True,
        }
    if content_type == ContentType.PSV.value:
        return {
            "delimiter": "|",
            "has_headers": False,
            "allow_variable_columns": True,
        }
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.JSON.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type for Daft reader: {content_type}")


class DaftFieldMapper(ModelMapper[DaftField, PaField]):
    @staticmethod
    def map(
        obj: Optional[DaftField],
        **kwargs,
    ) -> Optional[PaField]:
        """Convert Daft Field to PyArrow Field.

        Args:
            obj: The Daft Field to convert
            **kwargs: Additional arguments

        Returns:
            Converted PyArrow Field object
        """
        if obj is None:
            return None

        return pa.field(
            name=obj.name,
            type=obj.dtype.to_arrow_dtype(),
        )

    @staticmethod
    def unmap(
        obj: Optional[PaField],
        **kwargs,
    ) -> Optional[DaftField]:
        """Convert PyArrow Field to Daft Field.

        Args:
            obj: The PyArrow Field to convert
            **kwargs: Additional arguments

        Returns:
            Converted Daft Field object
        """
        if obj is None:
            return None

        return DaftField.create(
            name=obj.name,
            dtype=DataType.from_arrow_type(obj.type),  # type: ignore
        )


class DaftTransformMapper(ModelMapper[DaftTransform, Transform]):
    @staticmethod
    def map(
        obj: Optional[DaftTransform],
        **kwargs,
    ) -> Optional[Transform]:
        """Convert DaftTransform to DeltaCAT Transform.

        Args:
            obj: The DaftTransform to convert
            **kwargs: Additional arguments

        Returns:
            Converted Transform object
        """

        # daft.PartitionTransform doesn't have a Python interface for accessing its attributes,
        # thus conversion is not possible.
        # TODO: request Daft to expose Python friendly interface for daft.PartitionTransform
        raise NotImplementedError(
            "Converting transform from Daft to DeltaCAT is not supported"
        )

    @staticmethod
    def unmap(
        obj: Optional[Transform],
        **kwargs,
    ) -> Optional[DaftTransform]:
        """Convert DeltaCAT Transform to DaftTransform.

        Args:
            obj: The Transform to convert
            **kwargs: Additional arguments

        Returns:
            Converted DaftTransform object
        """
        if obj is None:
            return None

        # Map DeltaCAT transforms to Daft transforms using isinstance

        if isinstance(obj, IdentityTransform):
            return DaftTransform.identity()
        elif isinstance(obj, HourTransform):
            return DaftTransform.hour()
        elif isinstance(obj, DayTransform):
            return DaftTransform.day()
        elif isinstance(obj, MonthTransform):
            return DaftTransform.month()
        elif isinstance(obj, YearTransform):
            return DaftTransform.year()
        elif isinstance(obj, BucketTransform):
            if obj.parameters.bucketing_strategy == BucketingStrategy.ICEBERG:
                return DaftTransform.iceberg_bucket(obj.parameters.num_buckets)
            else:
                raise ValueError(
                    f"Unsupported Bucketing Strategy: {obj.parameters.bucketing_strategy}"
                )
        elif isinstance(obj, TruncateTransform):
            if obj.parameters.truncate_strategy == TruncateStrategy.ICEBERG:
                return DaftTransform.iceberg_truncate(obj.parameters.width)
            else:
                raise ValueError(
                    f"Unsupported Truncate Strategy: {obj.parameters.truncate_strategy}"
                )

        raise ValueError(f"Unsupported Transform: {obj}")


class DaftPartitionKeyMapper(ModelMapper[DaftPartitionField, PartitionKey]):
    @staticmethod
    def map(
        obj: Optional[DaftPartitionField],
        schema: Optional[DaftSchema] = None,
        **kwargs,
    ) -> Optional[PartitionKey]:
        """Convert DaftPartitionField to PartitionKey.

        Args:
            obj: The DaftPartitionField to convert
            schema: The Daft schema containing field information
            **kwargs: Additional arguments

        Returns:
            Converted PartitionKey object
        """
        # Daft PartitionField only exposes 1 attribute `field` which is not enough
        # to convert to DeltaCAT PartitionKey
        # TODO: request Daft to expose more Python friendly interface for PartitionField
        raise NotImplementedError(
            f"Converting Daft PartitionField to DeltaCAT PartitionKey is not supported"
        )

    @staticmethod
    def unmap(
        obj: Optional[PartitionKey],
        schema: Optional[Schema] = None,
        **kwargs,
    ) -> Optional[DaftPartitionField]:
        """Convert PartitionKey to DaftPartitionField.

        Args:
            obj: The DeltaCAT PartitionKey to convert
            schema: The Schema containing field information
            **kwargs: Additional arguments

        Returns:
            Converted DaftPartitionField object
        """
        if obj is None:
            return None
        if obj.name is None:
            raise ValueError("Name is required for PartitionKey conversion")
        if not schema:
            raise ValueError("Schema is required for PartitionKey conversion")
        if len(obj.key) < 1:
            raise ValueError(
                f"At least 1 PartitionKey FieldLocator is expected, instead got {len(obj.key)}. FieldLocators: {obj.key}."
            )

        # Get the source field from schema - FieldLocator in PartitionKey.key points to the source field of partition field
        dc_source_field = schema.field(obj.key[0]).arrow
        daft_source_field = DaftFieldMapper.unmap(obj=dc_source_field)
        # Convert transform if present
        daft_transform = DaftTransformMapper.unmap(obj.transform)
        daft_partition_field = DaftPartitionKeyMapper.get_daft_partition_field(
            partition_field_name=obj.name,
            daft_source_field=daft_source_field,
            dc_transform=obj.transform,
        )

        # Create DaftPartitionField
        return make_partition_field(
            field=daft_partition_field,
            source_field=daft_source_field,
            transform=daft_transform,
        )

    @staticmethod
    def get_daft_partition_field(
        partition_field_name: str,
        daft_source_field: Optional[DaftField],
        # TODO: replace DeltaCAT transform with Daft Transform for uniformality
        # We cannot use Daft Transform here because Daft Transform doesn't have a Python interface for us to
        # access its attributes.
        # TODO: request Daft to provide a more python friendly interface for Daft Tranform
        dc_transform: Optional[Transform],
    ) -> DaftField:
        """Generate Daft Partition Field given partition field name, source field and transform.
        Partition field type is inferred using source field type and transform.

        Args:
            partition_field_name (str): the specified result field name
            daft_source_field (DaftField): the source field of the partition field
            daft_transform (DaftTransform): transform applied on the source field to create partition field

        Returns:
            DaftField: Daft Field representing the partition field
        """
        if daft_source_field is None:
            raise ValueError("Source field is required for PartitionField conversion")
        if dc_transform is None:
            raise ValueError("Transform is required for PartitionField conversion")

        result_type = None
        # Below type conversion logic references Daft - Iceberg conversion logic:
        # https://github.com/Eventual-Inc/Daft/blob/7f2e9b5fb50fdfe858be17572f132b37dd6e5ab2/daft/iceberg/iceberg_scan.py#L61-L85
        if isinstance(dc_transform, IdentityTransform):
            result_type = daft_source_field.dtype
        elif isinstance(dc_transform, YearTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, MonthTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, DayTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, HourTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, BucketTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, TruncateTransform):
            result_type = daft_source_field.dtype
        else:
            raise ValueError(f"Unsupported transform: {dc_transform}")

        return DaftField.create(
            name=partition_field_name,
            dtype=result_type,
        )


def files_to_dataframe(
    uris: List[str],
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Optional[Callable[[int, Any], Dict[str, Any]]] = None,
    ray_init_options: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> DataFrame:
    """
    Read multiple files into a Daft DataFrame using any filesystem.

    This function supports reading PARQUET, CSV, JSON, TSV, and PSV files.

    Args:
        uris: List of file URIs to read
        content_type: The content type (PARQUET, CSV, JSON, TSV, UNESCAPED_TSV, PSV)
        content_encoding: The content encoding (currently only IDENTITY is supported)
        column_names: Optional column names to assign
        include_columns: Optional columns to include in the result
        read_func_kwargs_provider: Optional kwargs provider for customization
        ray_options_provider: Optional Ray options provider
        ray_init_options: Optional Ray initialization options
        **kwargs: Additional kwargs, including optional 'io_config' for filesystem configuration

    Returns:
        DataFrame: The Daft DataFrame

    Raises:
        AssertionError: If content_type is not supported or content_encoding is not IDENTITY

    Examples:
        # Read local parquet files (filesystem auto-inferred)
        df = files_to_dataframe(
            uris=["file1.parquet", "file2.parquet"],
            content_type=ContentType.PARQUET.value,
            content_encoding=ContentEncoding.IDENTITY.value
        )

        # Read CSV files
        df = files_to_dataframe(
            uris=["file1.csv", "file2.csv"],
            content_type=ContentType.CSV.value,
            content_encoding=ContentEncoding.IDENTITY.value
        )

        # Read S3 files with custom IOConfig
        from daft.io import IOConfig, S3Config
        s3_config = IOConfig(s3=S3Config(...))
        df = files_to_dataframe(
            uris=["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
            content_type=ContentType.PARQUET.value,
            content_encoding=ContentEncoding.IDENTITY.value,
            io_config=s3_config
        )
    """
    if ray_init_options is None:
        ray_init_options = {}

    if content_type not in CONTENT_TYPE_TO_READ_FN.keys():
        raise NotImplementedError(
            f"Daft native reader supports {CONTENT_TYPE_TO_READ_FN.keys()}, got {content_type}."
            f"Try using the Ray Dataset reader instead."
        )

    # Handle content encoding - for now, we only support identity and gzip
    if content_encoding not in [
        ContentEncoding.IDENTITY.value,
        ContentEncoding.GZIP.value,
    ]:
        raise NotImplementedError(
            f"Daft native reader currently supports identity and gzip encoding, got {content_encoding}"
        )

    if not ray.is_initialized():
        ray.init(**ray_init_options)

    daft.context.set_runner_ray(noop_if_initialized=True)

    read_kwargs = {}
    if read_func_kwargs_provider is not None:
        read_kwargs = read_func_kwargs_provider(content_type, read_kwargs)

    # Add content-type-specific reader kwargs
    content_type_kwargs = content_type_to_reader_kwargs(content_type)
    read_kwargs.update(content_type_kwargs)

    # Extract io_config from kwargs if provided, otherwise use None
    io_config = kwargs.pop("io_config", None)

    # Merge any remaining kwargs into read_kwargs (including file_path_column for native Daft support)
    read_kwargs.update(kwargs)

    logger.debug(f"Preparing to read {len(uris)} files into daft dataframe")
    logger.debug(f"Content type: {content_type}")
    logger.debug(f"Final read_kwargs: {read_kwargs}")

    # Get the appropriate Daft reader function based on content type
    daft_read_func = CONTENT_TYPE_TO_READ_FN.get(content_type)
    if not daft_read_func:
        raise NotImplementedError(
            f"Daft reader for content type '{content_type}' not implemented. "
            f"Known content types: {list(CONTENT_TYPE_TO_READ_FN.keys())}"
        )

    # Handle schema for all supported formats
    table_version_schema = kwargs.get("table_version_schema")
    if table_version_schema is not None:
        # Convert PyArrow schema to Daft schema using the official API
        daft_schema = daft.Schema.from_pyarrow_schema(table_version_schema)
        # Convert DaftSchema to dictionary format required by Daft readers
        schema_dict = {field.name: field.dtype for field in daft_schema}
        # Remove table_version_schema from kwargs since Daft readers don't recognize it
        read_kwargs.pop("table_version_schema", None)
        # Use explicit schema with infer_schema=False for correctness and performance
        read_kwargs.update({"infer_schema": False, "schema": schema_dict})
    else:
        # Remove table_version_schema parameter if present but None
        read_kwargs.pop("table_version_schema", None)

    logger.debug(f"Reading {len(uris)} files with Daft using {daft_read_func}.")

    # Call the appropriate Daft reader function
    if io_config is not None and content_type == ContentType.PARQUET.value:
        # Only parquet reader supports io_config parameter
        df, latency = timed_invocation(
            daft_read_func, path=uris, io_config=io_config, **read_kwargs
        )
    else:
        df, latency = timed_invocation(daft_read_func, path=uris, **read_kwargs)

    logger.debug(f"Daft read {len(uris)} files in {latency}s.")

    # Apply column selection after reading
    columns_to_read = include_columns or column_names
    file_path_column = read_kwargs.get("file_path_column")
    if file_path_column and columns_to_read and file_path_column not in columns_to_read:
        # Add file_path_column to selection if it was specified
        columns_to_read.append(file_path_column)

    if columns_to_read:
        logger.debug(f"Selecting columns {columns_to_read} with Daft.")
        return df.select(*columns_to_read)
    else:
        return df


def daft_file_to_pyarrow_table(
    path: str,
    content_type: str,
    content_encoding: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    **kwargs,
) -> pa.Table:
    assert (
        content_type == ContentType.PARQUET.value
    ), f"daft native reader currently only supports parquet, got {content_type}"

    assert (
        content_encoding == ContentEncoding.IDENTITY.value
    ), f"daft native reader currently only supports identity encoding, got {content_encoding}"

    kwargs = {}
    if pa_read_func_kwargs_provider is not None:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )
    file_timeout_ms = kwargs.get("file_timeout_ms", DEFAULT_FILE_READ_TIMEOUT_MS)

    row_groups = None
    if (
        partial_file_download_params is not None
        and partial_file_download_params.row_groups_to_download is not None
    ):
        row_groups = partial_file_download_params.row_groups_to_download

    # Extract io_config from kwargs if provided
    io_config = kwargs.pop("io_config", None)
    if not io_config and path.startswith("s3://"):
        io_config = _get_s3_io_config(kwargs)

    logger.debug(f"Preparing to read object from {path} into daft table")

    pa_table, latency = timed_invocation(
        read_parquet_into_pyarrow,
        path=path,
        columns=include_columns or column_names,
        row_groups=row_groups,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        multithreaded_io=False,
        file_timeout_ms=file_timeout_ms,
    )

    logger.debug(f"Time to read object from {path} into daft table: {latency}s")

    if kwargs.get("schema") is not None:
        input_schema = kwargs["schema"]
        if include_columns is not None:
            input_schema = pa.schema(
                [input_schema.field(col) for col in include_columns],
                metadata=input_schema.metadata,
            )
        elif column_names is not None:
            input_schema = pa.schema(
                [input_schema.field(col) for col in column_names],
                metadata=input_schema.metadata,
            )
        return coerce_pyarrow_table_to_schema(pa_table, input_schema)
    else:
        return pa_table


def _get_s3_io_config(s3_client_kwargs) -> IOConfig:
    return IOConfig(
        s3=S3Config(
            key_id=s3_client_kwargs.get("aws_access_key_id"),
            access_key=s3_client_kwargs.get("aws_secret_access_key"),
            session_token=s3_client_kwargs.get("aws_session_token"),
            region_name=AWS_REGION,
            retry_mode="adaptive",
            num_tries=BOTO_MAX_RETRIES,
            max_connections=DAFT_MAX_S3_CONNECTIONS_PER_FILE,
            connect_timeout_ms=5_000,  # Timeout to connect to server
            read_timeout_ms=10_000,  # Timeout for first byte from server
        )
    )
