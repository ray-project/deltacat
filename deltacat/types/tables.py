# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
import multiprocessing
from enum import Enum
from functools import partial
from typing import (
    Callable,
    Dict,
    Type,
    Union,
    Optional,
    Any,
    List,
    Tuple,
    TYPE_CHECKING,
)
from uuid import uuid4

import daft
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs
import pyarrow.parquet as papq
import ray
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data._internal.pandas_block import PandasBlockSchema
from ray.data.dataset import Dataset as RayDataset, MaterializedDataset
from ray.data.datasource import FilenameProvider
from ray.data.read_api import (
    from_arrow,
    from_arrow_refs,
    from_numpy,
    from_pandas,
    from_pandas_refs,
)
from tenacity import (
    Retrying,
    wait_random_exponential,
    stop_after_delay,
    retry_if_exception_type,
)

from deltacat.compute.compactor_v2.constants import MAX_RECORDS_PER_COMPACTED_FILE
from deltacat import logs
from deltacat.constants import (
    UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY,
    RETRYABLE_TRANSIENT_ERRORS,
    DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY,
)
from deltacat.storage.model.types import (
    Dataset,
    LocalTable,
    DistributedDataset,
    LocalDataset,
)
from deltacat.storage.model.schema import SchemaConsistencyType
from deltacat.types.media import (
    DatasetType,
    DistributedDatasetType,
    ContentType,
    EXPLICIT_COMPRESSION_CONTENT_TYPES,
    ContentEncoding,
    CONTENT_TYPE_TO_EXT,
    CONTENT_ENCODING_TO_EXT,
)
from deltacat.utils import numpy as np_utils
from deltacat.utils import pandas as pd_utils
from deltacat.utils import polars as pl_utils
from deltacat.utils import pyarrow as pa_utils
from deltacat.utils.ray_utils import dataset as ds_utils
from deltacat.storage.model.manifest import (
    group_manifest_urls_by_content_type,
    reconstruct_manifest_entry_url,
    ManifestEntryList,
    ManifestEntry,
    ManifestMeta,
    EntryParams,
    EntryType,
    Manifest,
)
from deltacat.exceptions import (
    RetryableError,
    RetryableUploadTableError,
    NonRetryableUploadTableError,
    categorize_errors,
    RetryableDownloadTableError,
    NonRetryableDownloadTableError,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.types.partial_download import PartialFileDownloadParams
from deltacat.utils.ray_utils.concurrency import invoke_parallel
from deltacat.utils.filesystem import append_protocol_prefix_by_type, FilesystemType

if TYPE_CHECKING:
    from deltacat.storage.model.schema import Schema
    from deltacat.storage.model.table import Table
    from deltacat.storage.model.table_version import TableVersion

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


TABLE_TYPE_TO_READER_FUNC: Dict[str, Callable] = {
    DatasetType.PYARROW_PARQUET.value: pa_utils.file_to_parquet,
    DatasetType.PYARROW.value: pa_utils.file_to_table,
    DatasetType.PANDAS.value: pd_utils.file_to_dataframe,
    DatasetType.NUMPY.value: np_utils.file_to_ndarray,
    DatasetType.POLARS.value: pl_utils.file_to_dataframe,
}


TABLE_CLASS_TO_WRITER_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.table_to_file,
    pd.DataFrame: pd_utils.dataframe_to_file,
    pl.DataFrame: pl_utils.dataframe_to_file,
    np.ndarray: np_utils.ndarray_to_file,
    RayDataset: ds_utils.dataset_to_file,
    MaterializedDataset: ds_utils.dataset_to_file,
}

TABLE_CLASS_TO_SLICER_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.slice_table,
    pd.DataFrame: pd_utils.slice_dataframe,
    pl.DataFrame: pl_utils.slice_table,
    np.ndarray: np_utils.slice_ndarray,
    RayDataset: ds_utils.slice_dataset,
    MaterializedDataset: ds_utils.slice_dataset,
}

TABLE_CLASS_TO_SIZE_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.table_size,
    papq.ParquetFile: pa_utils.parquet_file_size,
    pd.DataFrame: pd_utils.dataframe_size,
    pl.DataFrame: pl_utils.dataframe_size,
    np.ndarray: np_utils.ndarray_size,
    RayDataset: ds_utils.dataset_size,
    MaterializedDataset: ds_utils.dataset_size,
}

TABLE_CLASS_TO_COLUMN_NAMES_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: lambda table: table.schema.names,
    papq.ParquetFile: lambda table: table.schema.names,
    pd.DataFrame: lambda table: table.columns.tolist(),
    pl.DataFrame: lambda table: table.columns,
    np.ndarray: lambda table: [f"{i}" for i in range(table.shape[1])],
    daft.DataFrame: lambda table: table.column_names,
    RayDataset: lambda table: table.schema().names,
    MaterializedDataset: lambda table: table.schema().names,
}

TABLE_CLASS_TO_SCHEMA_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: lambda table: table.schema,
    papq.ParquetFile: lambda table: table.schema_arrow,
    pd.DataFrame: lambda table: pa.Schema.from_pandas(table),
    pl.DataFrame: lambda table: table.to_arrow().schema,
    np.ndarray: lambda table: pa.Schema.from_pandas(pd.DataFrame(table)),
    daft.DataFrame: lambda table: table.schema().to_pyarrow_schema(),
    RayDataset: lambda table: table.schema().base_schema,
    MaterializedDataset: lambda table: table.schema().base_schema,
}

TABLE_TYPE_TO_EMPTY_TABLE_FUNC: Dict[str, Callable] = {
    DatasetType.PYARROW.value: lambda: pa.Table.from_pydict({}),
    DatasetType.PANDAS.value: lambda: pd.DataFrame(),
    DatasetType.POLARS.value: lambda: pl.DataFrame(),
    DatasetType.NUMPY.value: lambda: np.array([]),
    DatasetType.DAFT.value: lambda: daft.DataFrame(),
    DatasetType.RAY_DATASET.value: lambda: ray.data.from_items([]),
    MaterializedDataset: lambda: ray.data.from_items([]),
}


def _numpy_array_to_pyarrow(table: np.ndarray, schema: pa.Schema) -> pa.Table:
    """Convert NumPy array to PyArrow Table via Pandas for complex type support."""
    # Convert NumPy -> Pandas -> PyArrow to handle complex types like structs
    # This follows the same path as Pandas conversion which handles all complex types properly
    pandas_df = _numpy_array_to_pandas(table, schema=schema)
    return pa.Table.from_pandas(pandas_df, schema=schema)


def _numpy_array_to_pandas(
    table: np.ndarray, *, schema: Optional[pa.Schema] = None, **kwargs
) -> pd.DataFrame:
    """Convert NumPy array to pandas DataFrame."""
    if schema and isinstance(schema, pa.Schema):
        if table.ndim == 1:
            # 1D array: single column
            column_names = [schema.names[0]] if schema.names else ["0"]
            return pd.DataFrame({column_names[0]: table}, **kwargs)
        elif table.ndim == 2:
            # 2D array: multiple columns
            column_names = (
                schema.names
                if len(schema.names) == table.shape[1]
                else [f"{i}" for i in range(table.shape[1])]
            )
            return pd.DataFrame(table, columns=column_names, **kwargs)
        else:
            raise ValueError(
                f"NumPy arrays with {table.ndim} dimensions are not supported. "
                f"Only 1D and 2D arrays are supported."
            )

    # Fallback to generic column names
    return pd.DataFrame(table, **kwargs)


def _ray_dataset_to_pyarrow(table, *, schema, **kwargs):
    """Convert Ray Dataset to PyArrow tables and concatenate."""
    arrow_refs = table.to_arrow_refs(**kwargs)
    arrow_tables = ray.get(arrow_refs)
    if len(arrow_tables) == 1:
        return arrow_tables[0]
    # Unify schemas to support schema evolution across blocks/files
    try:
        return pa.concat_tables(
            arrow_tables, promote_options="permissive", unify_schemas=True
        )
    except TypeError:
        return pa.concat_tables(arrow_tables, promote_options="permissive")


TABLE_CLASS_TO_PYARROW_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: lambda table, *, schema, **kwargs: table,
    papq.ParquetFile: lambda table, *, schema, **kwargs: table.read(**kwargs),
    pd.DataFrame: lambda table, *, schema, **kwargs: pa.Table.from_pandas(
        table, schema=schema, preserve_index=False, **kwargs
    ),
    pl.DataFrame: lambda table, *, schema, **kwargs: pl.DataFrame.to_arrow(
        table, **kwargs
    ),
    np.ndarray: lambda table, *, schema, **kwargs: _numpy_array_to_pyarrow(
        table, schema, **kwargs
    ),
    RayDataset: _ray_dataset_to_pyarrow,
    MaterializedDataset: _ray_dataset_to_pyarrow,
    daft.DataFrame: lambda table, *, schema, **kwargs: table.to_arrow(**kwargs),
}

TABLE_CLASS_TO_PANDAS_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: lambda table, *, schema=None, **kwargs: table.to_pandas(**kwargs),
    papq.ParquetFile: lambda table, *, schema=None, **kwargs: table.read(
        **kwargs
    ).to_pandas(**kwargs),
    pd.DataFrame: lambda table, *, schema=None, **kwargs: table,
    pl.DataFrame: lambda table, *, schema=None, **kwargs: table.to_pandas(**kwargs),
    np.ndarray: lambda table, *, schema=None, **kwargs: _numpy_array_to_pandas(
        table, schema=schema, **kwargs
    ),
    RayDataset: lambda table, *, schema=None, **kwargs: table.to_pandas(**kwargs),
    MaterializedDataset: lambda table, *, schema=None, **kwargs: table.to_pandas(
        **kwargs
    ),
    daft.DataFrame: lambda table, *, schema=None, **kwargs: table.to_pandas(**kwargs),
}


def _pyarrow_to_polars(pa_table: pa.Table, **kwargs) -> pl.DataFrame:
    """Convert PyArrow table to Polars DataFrame with clean schema."""
    # PyArrow metadata can contain invalid UTF-8 sequences that cause Polars to raise an error
    # Create a new table without metadata that might contain invalid UTF-8
    clean_schema = pa.schema(
        [
            pa.field(field.name, field.type, nullable=field.nullable)
            for field in pa_table.schema
        ]
    )
    clean_table = pa.Table.from_arrays(pa_table.columns, schema=clean_schema)
    return pl.from_arrow(clean_table, **kwargs)


def _pyarrow_to_numpy(pa_table: pa.Table, **kwargs) -> np.ndarray:
    """Convert PyArrow table to numpy array."""
    if pa_table.num_columns == 1:
        return pa_table.column(0).to_numpy(**kwargs)
    else:
        return pa_table.to_pandas().values


def _pandas_to_numpy(pd_df: pd.DataFrame, **kwargs) -> np.ndarray:
    """Convert Pandas DataFrame to numpy array."""
    if len(pd_df.columns) == 1:
        return pd_df.iloc[:, 0].to_numpy(**kwargs)
    else:
        return pd_df.values


DATASET_TYPE_FROM_PYARROW: Dict[DatasetType, Callable[[pa.Table, Dataset], Any]] = {
    DatasetType.PYARROW: lambda pa_table, **kwargs: pa_table,
    DatasetType.PANDAS: lambda pa_table, **kwargs: pa_table.to_pandas(**kwargs),
    DatasetType.POLARS: lambda pa_table, **kwargs: _pyarrow_to_polars(
        pa_table, **kwargs
    ),
    DatasetType.DAFT: lambda pa_table, **kwargs: daft.from_arrow(pa_table, **kwargs),
    DatasetType.NUMPY: lambda pa_table, **kwargs: _pyarrow_to_numpy(pa_table, **kwargs),
    DatasetType.RAY_DATASET: lambda pa_table, **kwargs: ray.data.from_arrow(pa_table),
    DatasetType.PYARROW_PARQUET: lambda pa_table, **kwargs: pa_table,  # ParquetFile is read as PyArrow Table
}


DATASET_TYPE_FROM_PANDAS: Dict[DatasetType, Callable[[pd.DataFrame, Dataset], Any]] = {
    DatasetType.PANDAS: lambda pd_df, **kwargs: pd_df,
    DatasetType.PYARROW: lambda pd_df, **kwargs: pa.Table.from_pandas(pd_df, **kwargs),
    DatasetType.POLARS: lambda pd_df, **kwargs: pl.from_pandas(pd_df, **kwargs),
    DatasetType.DAFT: lambda pd_df, **kwargs: daft.from_pandas(pd_df, **kwargs),
    DatasetType.NUMPY: lambda pd_df, **kwargs: _pandas_to_numpy(pd_df, **kwargs),
    DatasetType.RAY_DATASET: lambda pd_df, **kwargs: ray.data.from_pandas(
        pd_df, **kwargs
    ),
}


def append_column_to_parquet_file(
    parquet_file: papq.ParquetFile,
    column_name: str,
    column_value: Any,
) -> pa.Table:
    """
    Append a column to a ParquetFile by converting to PyArrow Table first.

    Args:
        parquet_file: The ParquetFile to add column to
        column_name: Name of the new column
        column_value: Value to populate in all rows of the new column

    Returns:
        PyArrow Table with the new column
    """
    # Convert ParquetFile to Table
    table = parquet_file.read()

    # Use the existing PyArrow append column function
    num_rows = table.num_rows
    column_array = pa.array([column_value] * num_rows)
    return table.append_column(column_name, column_array)


TABLE_CLASS_TO_APPEND_COLUMN_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.append_column_to_table,
    papq.ParquetFile: append_column_to_parquet_file,
    pd.DataFrame: pd_utils.append_column_to_dataframe,
    pl.DataFrame: pl_utils.append_column_to_table,
    np.ndarray: np_utils.append_column_to_ndarray,
}

TABLE_CLASS_TO_SELECT_COLUMNS_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.select_columns,
    pd.DataFrame: pd_utils.select_columns,
    pl.DataFrame: pl_utils.select_columns,
}

TABLE_CLASS_TO_TABLE_TYPE: Dict[Union[LocalTable, DistributedDataset], str] = {
    pa.Table: DatasetType.PYARROW.value,
    papq.ParquetFile: DatasetType.PYARROW_PARQUET.value,
    pl.DataFrame: DatasetType.POLARS.value,
    pd.DataFrame: DatasetType.PANDAS.value,
    np.ndarray: DatasetType.NUMPY.value,
    daft.DataFrame: DatasetType.DAFT.value,
    RayDataset: DatasetType.RAY_DATASET.value,
    MaterializedDataset: DatasetType.RAY_DATASET.value,
}

TABLE_TYPE_TO_DATASET_CREATE_FUNC: Dict[str, Callable] = {
    DatasetType.PYARROW.value: from_arrow,
    DatasetType.PYARROW_PARQUET.value: from_arrow,
    DatasetType.NUMPY.value: from_numpy,
    DatasetType.PANDAS.value: from_pandas,
}

TABLE_TYPE_TO_DATASET_CREATE_FUNC_REFS: Dict[str, Callable] = {
    DatasetType.PYARROW.value: from_arrow_refs,
    DatasetType.PYARROW_PARQUET.value: from_arrow_refs,
    DatasetType.NUMPY.value: from_numpy,
    DatasetType.PANDAS.value: from_pandas_refs,
    DatasetType.POLARS.value: from_arrow_refs,  # We cast Polars to Arrow for Ray Datasets
    DatasetType.RAY_DATASET.value: from_arrow_refs,  # Ray Datasets are created from Arrow refs
}

TABLE_TYPE_TO_CONCAT_FUNC: Dict[str, Callable] = {
    DatasetType.PYARROW_PARQUET.value: pa_utils.concat_tables,
    DatasetType.PYARROW.value: pa_utils.concat_tables,
    DatasetType.PANDAS.value: pd_utils.concat_dataframes,
    DatasetType.NUMPY.value: np_utils.concat_ndarrays,
    DatasetType.POLARS.value: pl_utils.concat_dataframes,
}


def _infer_schema_from_numpy_array(data: np.ndarray) -> Schema:
    """Infer schema from NumPy array."""
    if data.ndim > 2:
        raise ValueError(
            f"NumPy arrays with {data.ndim} dimensions are not supported. "
            f"Only 1D and 2D arrays are supported."
        )
    # Handle object dtype by converting to pandas first
    df = pd.DataFrame(data)
    arrow_schema = pa.Schema.from_pandas(df)

    from deltacat.storage.model.schema import Schema

    return Schema.of(schema=arrow_schema)


def _infer_schema_from_ray_dataset(data: RayDataset) -> Schema:
    """Infer schema from Ray Dataset."""
    ray_schema = data.schema()
    base_schema = ray_schema.base_schema

    if isinstance(base_schema, pa.Schema):
        arrow_schema = base_schema
    elif isinstance(base_schema, PandasBlockSchema):
        try:
            dtype_dict = {
                name: dtype for name, dtype in zip(base_schema.names, base_schema.types)
            }
            empty_df = pd.DataFrame(columns=base_schema.names).astype(dtype_dict)
            arrow_schema = pa.Schema.from_pandas(empty_df)
        except Exception as e:
            raise ValueError(
                f"Failed to convert Ray Dataset PandasBlockSchema to PyArrow schema: {e}"
            )
    else:
        raise ValueError(
            f"Unsupported Ray Dataset schema type: {type(base_schema)}. "
            f"Expected PyArrow Schema or PandasBlockSchema, got {base_schema}"
        )

    from deltacat.storage.model.schema import Schema

    return Schema.of(schema=arrow_schema)


def _infer_schema_from_pandas_dataframe(data: pd.DataFrame) -> Schema:
    """Infer schema from Pandas DataFrame."""
    from deltacat.storage.model.schema import Schema

    arrow_schema = pa.Schema.from_pandas(data)
    return Schema.of(schema=arrow_schema)


def _infer_schema_from_polars_dataframe(data: pl.DataFrame) -> Schema:
    """Infer schema from Polars DataFrame."""
    from deltacat.storage.model.schema import Schema

    arrow_table = data.to_arrow()
    return Schema.of(schema=arrow_table.schema)


def _infer_schema_from_pyarrow(
    data: Union[pa.Table, pa.RecordBatch, ds.Dataset]
) -> Schema:
    """Infer schema from PyArrow Table, RecordBatch, or Dataset."""
    from deltacat.storage.model.schema import Schema

    return Schema.of(schema=data.schema)


def _infer_schema_from_daft_dataframe(data: daft.DataFrame) -> Schema:
    """Infer schema from Daft DataFrame."""
    from deltacat.storage.model.schema import Schema

    daft_schema = data.schema()
    arrow_schema = daft_schema.to_pyarrow_schema()
    return Schema.of(schema=arrow_schema)


TABLE_CLASS_TO_SCHEMA_INFERENCE_FUNC: Dict[
    Type[Union[LocalTable, DistributedDataset], Callable]
] = {
    pd.DataFrame: _infer_schema_from_pandas_dataframe,
    pl.DataFrame: _infer_schema_from_polars_dataframe,
    pa.Table: _infer_schema_from_pyarrow,
    pa.RecordBatch: _infer_schema_from_pyarrow,
    ds.Dataset: _infer_schema_from_pyarrow,
    RayDataset: _infer_schema_from_ray_dataset,
    MaterializedDataset: _infer_schema_from_ray_dataset,  # MaterializedDataset uses same schema inference as RayDataset
    daft.DataFrame: _infer_schema_from_daft_dataframe,
    np.ndarray: _infer_schema_from_numpy_array,
}


def infer_table_schema(data: Union[LocalTable, DistributedDataset]) -> Schema:
    """Infer schema from a table or dataset."""
    infer_schema_func = _get_table_function(
        data,
        TABLE_CLASS_TO_SCHEMA_INFERENCE_FUNC,
        "schema inference",
    )
    return infer_schema_func(data)


def concat_tables(tables: List[LocalTable], table_type: DatasetType) -> LocalTable:
    """
    Concatenate a list of tables into a single table using the appropriate
    concatenation function for the given table type.

    Args:
        tables: List of tables to concatenate
        table_type: The DatasetType indicating which concatenation function to use

    Returns:
        Single concatenated table of the appropriate type

    Raises:
        ValueError: If no concatenation function is found for the table type
    """
    concat_func = _get_table_type_function(
        table_type, TABLE_TYPE_TO_CONCAT_FUNC, "concatenation"
    )
    return concat_func(tables)


def _daft_reader_wrapper(*args, **kwargs):
    """Wrapper for daft reader with lazy import to avoid circular import."""
    from deltacat.utils.daft import files_to_dataframe

    return files_to_dataframe(*args, **kwargs)


DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC: Dict[int, Callable] = {
    DistributedDatasetType.DAFT.value: _daft_reader_wrapper,
}


class TableWriteMode(str, Enum):
    """
    Enum controlling how a given dataset will be written to a table.

    AUTO: CREATE if the table doesn't exist, APPEND if the table exists
    without merge keys, and MERGE if the table exists with merge keys.
    CREATE: Create the table if it doesn't exist, throw an error if it does.
    ADD: Add unordered data to the table if it exists, throw an error if it doesn't.
    APPEND: Append ordered data to the table if it exists, throw an error if it doesn't.
    REPLACE: Replace existing table contents with the data to write.
    MERGE: Insert or update records matching table merge keys.
    Updates or inserts records based on the table's merge and sort keys by
    default.
    DELETE: Delete records matching table merge keys.
    """

    AUTO = "auto"
    CREATE = "create"
    ADD = "add"
    APPEND = "append"
    REPLACE = "replace"
    MERGE = "merge"
    DELETE = "delete"


class SchemaEvolutionMode(str, Enum):
    """
    Enum controlling how schema changes are handled when writing to a table.

    MANUAL: Schema changes must be explicitly handled by the user. New fields
    not in the existing schema will cause an error.
    AUTO: Schema changes are automatically handled. New fields are added to
    the schema using the table's default_schema_consistency_type.
    DISABLED: Schema changes are disabled. The schema that the table was
    created with is immutable.
    """

    MANUAL = "manual"
    AUTO = "auto"
    DISABLED = "disabled"


class TableProperty(str, Enum):
    """
    Enum defining known table property key names.
    """

    READ_OPTIMIZATION_LEVEL = "read_optimization_level"
    RECORDS_PER_COMPACTED_FILE = "records_per_compacted_file"
    APPENDED_RECORD_COUNT_COMPACTION_TRIGGER = (
        "appended_record_count_compaction_trigger"
    )
    APPENDED_FILE_COUNT_COMPACTION_TRIGGER = "appended_file_count_compaction_trigger"
    APPENDED_DELTA_COUNT_COMPACTION_TRIGGER = "appended_delta_count_compaction_trigger"
    DEFAULT_COMPACTION_HASH_BUCKET_COUNT = "default_compaction_hash_bucket_count"
    SCHEMA_EVOLUTION_MODE = "schema_evolution_mode"
    DEFAULT_SCHEMA_CONSISTENCY_TYPE = "default_schema_consistency_type"
    SUPPORTED_READER_TYPES = "supported_reader_types"

    def read_table_property(
        table_or_table_version: Union[Table, TableVersion], property: TableProperty
    ) -> Any:
        properties = table_or_table_version.properties or {}
        value = properties.get(property.value, TablePropertyDefaultValues[property])

        # Handle property type conversion
        if property == TableProperty.SUPPORTED_READER_TYPES and isinstance(value, list):
            # Convert string values back to DatasetType enums
            return [DatasetType(v) for v in value]
        if property == TableProperty.SCHEMA_EVOLUTION_MODE:
            return SchemaEvolutionMode(value)
        if property == TableProperty.DEFAULT_SCHEMA_CONSISTENCY_TYPE:
            return SchemaConsistencyType(value)
        if property == TableProperty.READ_OPTIMIZATION_LEVEL:
            return TableReadOptimizationLevel(value)
        return value


class TableReadOptimizationLevel(str, Enum):
    """
    Enum controlling the how much to optimize reads when writing to a table. Different levels
    here correspond to different tradeoffs between write and read performance.

    NONE: No read optimization. Deletes and updates are resolved by finding the values
    that match merge key predicates by running compaction at read time. Provides the
    fastest/cheapest writes but slow/expensive reads. Resilient to conflicts with concurrent
    writes, including table management jobs like compaction.

    MODERATE: Discover record indexes that match merge key predicates at write time and record
    those values as logically deleted (e.g., using a bitmask). Provides faster/cheaper reads but
    slower/more-expensive writes. May conflict with concurrent writes that remove/replace data
    files like compaction.

    MAX: Materialize all deletes and updates at write time by running compaction during
    every write. Provides fast/cheap reads but slow/expensive writes. May conflict with
    concurrent writes, including table management jobs like compaction.
    """

    NONE = "none"
    MODERATE = "moderate"
    MAX = "max"


TablePropertyDefaultValues: Dict[TableProperty, Any] = {
    TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX,
    TableProperty.RECORDS_PER_COMPACTED_FILE: MAX_RECORDS_PER_COMPACTED_FILE,
    TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER: MAX_RECORDS_PER_COMPACTED_FILE
    * 16,  # DEFAULT_COMPACTION_HASH_BUCKET_COUNT * 2
    TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER: 1000,
    TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER: 100,
    TableProperty.DEFAULT_COMPACTION_HASH_BUCKET_COUNT: 8,
    TableProperty.SCHEMA_EVOLUTION_MODE: SchemaEvolutionMode.AUTO,
    TableProperty.DEFAULT_SCHEMA_CONSISTENCY_TYPE: SchemaConsistencyType.NONE,
    TableProperty.SUPPORTED_READER_TYPES: [d for d in DatasetType],
}


def _get_table_function(
    table: Union[LocalTable, DistributedDataset],
    function_map: Dict[Type, Callable],
    operation_name: str,
) -> Callable:
    """Generic helper to look up table-type-specific functions."""
    table_func = function_map.get(type(table))
    if table_func is None:
        msg = (
            f"No {operation_name} function found for table type: {type(table)}.\n"
            f"Known table types: {list(function_map.keys())}"
        )
        raise ValueError(msg)
    return table_func


def _get_table_type_function(
    table_type: DatasetType, function_map: Dict[str, Callable], operation_name: str
) -> Callable:
    """Generic helper to look up DatasetType-specific functions."""
    table_func = function_map.get(table_type.value)
    if table_func is None:
        msg = (
            f"No {operation_name} function found for table type: {table_type}.\n"
            f"Known table types: {list(function_map.keys())}"
        )
        raise ValueError(msg)
    return table_func


def _convert_all(tables: List[LocalTable], conversion_fn: Callable, **kwargs):
    if not tables:  # Empty list
        return pd.DataFrame()

    # Convert list elements to the same type
    all_tables = []
    for i, table in enumerate(tables):
        try:
            converted_table = conversion_fn(table, **kwargs)
            all_tables.append(converted_table)
        except Exception as e:
            raise ValueError(f"Failed to convert list element {i}: {e}") from e

    # Concatenate with error handling
    try:
        return concat_tables(all_tables, get_dataset_type(all_tables[0]))
    except Exception as e:
        raise ValueError(f"Failed to concatenate {len(all_tables)} tables: {e}") from e


def get_table_length(
    table: Union[LocalTable, DistributedDataset, BlockAccessor]
) -> int:
    """
    Generic function to get the length of a table or distributed dataset.

    Args:
        table: The local table or distributed dataset to get the length of

    Returns:
        Length of the table or distributed dataset in rows
    """
    # Handle DAFT DataFrames dynamically
    if hasattr(table, "count_rows") and str(type(table).__module__).startswith("daft"):
        return table.count_rows()
    elif isinstance(table, RayDataset):
        return table.count()
    elif isinstance(table, papq.ParquetFile):
        return table.metadata.num_rows
    else:
        return len(table)


def dataset_length(table: Dataset) -> int:
    """
    Generic function to get the length of a dataset in records.
    If the input is a list of tables, the length is the sum of the
    lengths of the tables.

    Args:
        table: The dataset to get the length of

    Returns:
        Length of the dataset in records
    """
    if isinstance(table, list):
        return sum(get_table_length(t) for t in table)
    return get_table_length(table)


def get_table_size(table: Union[LocalTable, DistributedDataset]) -> int:
    """
    Generic function to get the size of a table or distributed dataset.

    Args:
        table: The local table or distributed dataset to get the size of

    Returns:
        Size of the table or distributed dataset
    """
    table_size_func = _get_table_function(table, TABLE_CLASS_TO_SIZE_FUNC, "size")
    return table_size_func(table)


def dataset_size(table: Dataset) -> int:
    """
    Generic function to get the size of a dataset in bytes.
    If the input is a list of tables, the size is the sum of the
    sizes of the tables.

    Args:
        table: The dataset to get the size of

    Returns:
        Size of the dataset in bytes
    """
    if isinstance(table, list):
        return sum(get_table_size(t) for t in table)
    return get_table_size(table)


def get_table_column_names(table: Union[LocalTable, DistributedDataset]) -> List[str]:
    """
    Generic function to get the column names of a table or distributed dataset.

    Args:
        table: The local table or distributed dataset to get the column names of

    Returns:
        List of column names
    """
    column_names_func = _get_table_function(
        table, TABLE_CLASS_TO_COLUMN_NAMES_FUNC, "column names"
    )
    return column_names_func(table)


def dataset_column_names(table: Dataset) -> List[str]:
    """
    Generic function to get the column names of a dataset.
    If the input is a list of tables, unique column names are
    returned in the order they are first seen in the list.

    Args:
        table: The dataset to get the column names of

    Returns:
        List of column names
    """
    if isinstance(table, list):
        # use dictionary keys as an ordered set
        column_names = {}
        for t in table:
            for column_name in get_table_column_names(t):
                column_names[column_name] = None
        return list(column_names.keys())
    return get_table_column_names(table)


def get_table_schema(table: Union[LocalTable, DistributedDataset]) -> pa.Schema:
    """
    Generic function to get the PyArrow schema of a table or distributed dataset.

    Args:
        table: The local table or distributed dataset to get the schema of

    Returns:
        PyArrow Schema object
    """
    schema_func = _get_table_function(table, TABLE_CLASS_TO_SCHEMA_FUNC, "schema")
    return schema_func(table)


def dataset_schema(table: Dataset) -> pa.Schema:
    """
    Generic function to get the PyArrow schema of a dataset. If the input is a list of
    tables, uses pyarrow.unify_schemas(schemas, promote_options="permissive").

    Args:
        table: The dataset to get the schema of

    Returns:
        PyArrow Schema object
    """
    if isinstance(table, list):
        return pa.unify_schemas(
            [get_table_schema(t) for t in table], promote_options="permissive"
        )
    return get_table_schema(table)


def get_table_writer(table: Union[LocalTable, DistributedDataset]) -> Callable:
    """
    Generic function to get a table writer function for a given dataset type.

    Args:
        table: The local table or distributed dataset to get the writer function for

    Returns:
        Writer function for the given dataset type
    """
    return _get_table_function(table, TABLE_CLASS_TO_WRITER_FUNC, "writer")


def get_table_slicer(table: Union[LocalTable, DistributedDataset]) -> Callable:
    """
    Generic function to get a table slicer function for a given dataset type.

    Args:
        table: The local table or distributed dataset to get the slicer function for

    Returns:
        Slicer function for the given dataset type
    """
    return _get_table_function(table, TABLE_CLASS_TO_SLICER_FUNC, "slicer")


def get_dataset_type(dataset: Union[LocalTable, DistributedDataset]) -> DatasetType:
    """Get the DatasetType enum value for a given dataset object.

    Args:
        dataset: The dataset object to identify

    Returns:
        DatasetType enum value corresponding to the dataset type

    Raises:
        ValueError: If the dataset type is not supported
    """
    dataset_type_str = _get_table_function(
        dataset, TABLE_CLASS_TO_TABLE_TYPE, "dataset type identification"
    )
    return DatasetType(dataset_type_str)


def table_to_pyarrow(
    table: Union[LocalTable, DistributedDataset],
    *,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> pa.Table:
    """
    Convert a single table or distributed dataset to PyArrow Table format.

    Args:
        table: The local table or distributed dataset to convert
        schema: Optional schema to use for the conversion
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        PyArrow Table created from the provided dataset
    """
    to_pyarrow_func = _get_table_function(
        table, TABLE_CLASS_TO_PYARROW_FUNC, "pyarrow conversion"
    )
    return to_pyarrow_func(table, schema=schema, **kwargs)


def table_to_pandas(
    table: Union[LocalTable, DistributedDataset],
    *,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Convert a single table or distributed dataset to pandas DataFrame format.

    Args:
        table: The local table or distributed dataset to convert
        schema: Optional schema to use for the conversion
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        pandas DataFrame created from the provided dataset
    """
    to_pandas_func = _get_table_function(
        table, TABLE_CLASS_TO_PANDAS_FUNC, "pandas conversion"
    )
    return to_pandas_func(table, schema=schema, **kwargs)


def to_pyarrow(
    table: Dataset, *, schema: Optional[pa.Schema] = None, **kwargs
) -> pa.Table:
    """
    Convert any supported dataset type to PyArrow Table format.

    Args:
        table: The table/dataset to convert
        schema: Optional schema to use for the conversion
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        PyArrow Table created from the provided dataset
    """
    if isinstance(table, list):
        return _convert_all(table, table_to_pyarrow, schema=schema, **kwargs)
    return table_to_pyarrow(table, schema=schema, **kwargs)


def to_pandas(
    table: Dataset, *, schema: Optional[pa.Schema] = None, **kwargs
) -> pd.DataFrame:
    """
    Convert any supported dataset type to pandas DataFrame format.

    Args:
        table: The table/dataset to convert
        schema: Optional schema to use for the conversion
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        pandas DataFrame created from the provided dataset
    """
    if isinstance(table, list):
        return _convert_all(table, table_to_pandas, schema=schema, **kwargs)
    return table_to_pandas(table, schema=schema, **kwargs)


def from_pyarrow(pa_table: pa.Table, target_type: DatasetType, **kwargs) -> Dataset:
    """Convert PyArrow Table to the specified dataset type.

    Args:
        pa_table: PyArrow Table to convert
        target_type: Target DatasetType to convert to
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        Dataset converted to the target type

    Raises:
        ValueError: If target_type is not supported
    """
    conversion_func = _get_table_type_function(
        target_type,
        DATASET_TYPE_FROM_PYARROW,
        f"{target_type} conversion",
    )
    return conversion_func(pa_table, **kwargs)


def from_pandas(pd_df: pd.DataFrame, target_type: DatasetType, **kwargs) -> Dataset:
    """Convert Pandas DataFrame to the specified dataset type.

    Args:
        pd_df: Pandas DataFrame to convert
        target_type: Target DatasetType to convert to
        **kwargs: Additional arguments passed to the conversion function

    Returns:
        Dataset converted to the target type

    Raises:
        ValueError: If target_type is not supported
    """
    conversion_func = _get_table_type_function(
        target_type,
        DATASET_TYPE_FROM_PANDAS,
        f"{target_type} conversion",
    )
    return conversion_func(pd_df, **kwargs)


def empty_table(table_type: DatasetType) -> Dataset:
    """
    Create an empty table of the given type.
    """
    empty_table_func = _get_table_type_function(
        table_type, TABLE_TYPE_TO_EMPTY_TABLE_FUNC, "empty table"
    )
    return empty_table_func()


def append_column_to_table(
    table: LocalTable,
    column_name: str,
    column_value: Any,
) -> LocalTable:
    """
    Generic function to append a column with a specified value to any supported dataset type.

    Args:
        table: The table/dataset to add column to
        column_name: Name of the new column
        column_value: Value to populate in all rows of the new column
        table_type: Type of the dataset

    Returns:
        Updated table with the new column
    """
    append_column_to_table_func = _get_table_function(
        table, TABLE_CLASS_TO_APPEND_COLUMN_FUNC, "append column"
    )
    return append_column_to_table_func(table, column_name, column_value)


def select_columns_from_table(
    table: LocalTable,
    column_names: List[str],
) -> LocalTable:
    """
    Generic function to select columns from any supported dataset type.

    Args:
        table: The table/dataset to select columns from
        column_names: List of column names to select

    Returns:
        Updated table with the selected columns
    """
    select_columns_func = _get_table_function(
        table, TABLE_CLASS_TO_SELECT_COLUMNS_FUNC, "select columns"
    )
    return select_columns_func(table, column_names)


def write_sliced_table(
    table: Union[LocalTable, DistributedDataset],
    base_path: str,
    filesystem: Optional[pa.fs.FileSystem],
    max_records_per_entry: Optional[int],
    table_writer_fn: Callable,
    table_slicer_fn: Callable,
    table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
    skip_manifest_write: bool = False,
) -> Union[ManifestEntryList, List[str]]:
    """
    Writes the given table to 1 or more files and return either manifest entries
    or file paths depending on skip_manifest_write parameter.

    Args:
        table: The local table or distributed dataset to write
        base_path: The base path to write the table to
        filesystem: The filesystem to write the table to
        max_records_per_entry: Maximum records per file (None = write whole table)
        table_writer_fn: The function to write the table to
        table_slicer_fn: The function to slice the table into multiple files
        table_writer_kwargs: Additional arguments to pass to the table writer
        content_type: The content type to write the table to
        entry_params: Manifest entry parameters
        entry_type: The manifest entry types to write
        skip_manifest_write: If True, calls table_writer_fn directly and returns file paths as strings.
                           If False (default), creates manifest entries and returns ManifestEntryList.

    Returns:
        Manifest entries describing the uploaded files or list of file paths
    """
    # @retry decorator can't be pickled by Ray, so wrap upload in Retrying
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )

    # Determine if we should write the whole table or slice it
    should_write_whole_table = max_records_per_entry is None or not get_table_length(
        table
    )

    if should_write_whole_table:
        # Write the whole table to a single file
        return retrying(
            write_table,
            table,
            f"{base_path}",
            filesystem,
            table_writer_fn,
            table_writer_kwargs,
            content_type,
            entry_params,
            entry_type,
            skip_manifest_write,
        )
    else:
        # Iteratively write table slices
        results = ManifestEntryList() if not skip_manifest_write else []
        table_slices = table_slicer_fn(table, max_records_per_entry)

        for table_slice in table_slices:
            slice_result = retrying(
                write_table,
                table_slice,
                f"{base_path}",
                filesystem,
                table_writer_fn,
                table_writer_kwargs,
                content_type,
                entry_params,
                entry_type,
                skip_manifest_write,
            )

            if skip_manifest_write:
                results.extend(slice_result)  # slice_result is List[str]
            else:
                results.extend(slice_result)  # slice_result is ManifestEntryList

        return results


def write_table(
    table: Union[LocalTable, DistributedDataset],
    base_path: str,
    filesystem: Optional[pa.fs.FileSystem],
    table_writer_fn: Callable,
    table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
    skip_manifest_write: bool = False,
) -> Union[ManifestEntryList, List[str]]:
    """
    Writes the given table to 1 or more files and return either manifest entries
    or file paths depending on skip_manifest_write parameter.

    Args:
        table: The local table or distributed dataset to write
        base_path: The base path to write the table to
        filesystem: The filesystem to write the table to
        table_writer_fn: The function to write the table to
        table_writer_kwargs: Additional arguments to pass to the table writer
        content_type: The content type to write the table to
        entry_params: Manifest entry parameters
        entry_type: The manifest entry types to write
        skip_manifest_write: If True, returns file paths as strings.
                           If False (default), creates manifest entries and returns ManifestEntryList.

    Returns:
        Manifest entries describing the uploaded files or list of file paths
    """
    if table_writer_kwargs is None:
        table_writer_kwargs = {}

    # Determine content_encoding before writing files so we can include it in filenames
    content_encoding = None
    if content_type in EXPLICIT_COMPRESSION_CONTENT_TYPES and not skip_manifest_write:
        # TODO(pdames): Support other user-specified encodings at write time.
        content_encoding = ContentEncoding.GZIP

    wrapped_obj = (
        CapturedBlockWritePathsActor.remote()
        if isinstance(table, RayDataset)
        else CapturedBlockWritePathsBase()
    )
    capture_object = CapturedBlockWritePaths(wrapped_obj)

    # Create block write path provider with appropriate parameters
    if skip_manifest_write:
        block_write_path_provider = UuidBlockWritePathProvider(
            capture_object,
            base_path=base_path,
        )
    else:
        block_write_path_provider = UuidBlockWritePathProvider(
            capture_object,
            base_path=base_path,
            content_type=content_type,
            content_encoding=content_encoding,
        )

    # Extract schema, schema_id, sort_scheme_id, and catalog_properties from table_writer_kwargs
    schema = table_writer_kwargs.pop("schema", None)
    schema_id = table_writer_kwargs.pop("schema_id", None)
    sort_scheme_id = table_writer_kwargs.pop("sort_scheme_id", None)
    catalog_properties = table_writer_kwargs.pop("catalog_properties", None)

    table_writer_fn(
        table,
        base_path,
        filesystem,
        block_write_path_provider,
        content_type.value,
        schema=schema,
        **table_writer_kwargs,
    )

    # TODO: Add a proper fix for block_refs and write_paths not persisting in Ray actors
    del block_write_path_provider
    blocks = capture_object.blocks()
    write_paths = capture_object.write_paths()

    # If skip_manifest_write is True, return file paths with protocol prefixes
    if skip_manifest_write:
        filesystem_type = FilesystemType.from_filesystem(filesystem)
        return [
            append_protocol_prefix_by_type(path, filesystem_type)
            for path in write_paths
        ]

    # Create manifest entries
    metadata = get_block_metadata_list(table, write_paths, blocks)
    manifest_entries = ManifestEntryList()
    for block_idx, path in enumerate(write_paths):
        try:
            manifest_entry = ManifestEntry.from_path(
                path=path,
                filesystem=filesystem,
                record_count=metadata[block_idx].num_rows,
                source_content_length=metadata[block_idx].size_bytes,
                content_type=content_type.value
                if hasattr(content_type, "value")
                else content_type,
                content_encoding=content_encoding,
                entry_type=entry_type,
                entry_params=entry_params,
                schema_id=schema_id,
                sort_scheme_id=sort_scheme_id,
                catalog_root=catalog_properties.root if catalog_properties else None,
            )
            manifest_entries.append(manifest_entry)
        except RETRYABLE_TRANSIENT_ERRORS as e:
            _handle_retryable_error(e, path, "write", RetryableUploadTableError)
        except BaseException as e:
            _handle_non_retryable_error(
                e,
                path,
                "upload",
                NonRetryableUploadTableError,
                f"and content_type={content_type}",
            )
    return manifest_entries


@ray.remote
class CapturedBlockWritePathsActor:
    def __init__(self):
        self._wrapped = CapturedBlockWritePathsBase()

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        self._wrapped.extend(write_paths, blocks)

    def write_paths(self) -> List[str]:
        return self._wrapped.write_paths()

    def blocks(self) -> List[Block]:
        return self._wrapped.blocks()


class CapturedBlockWritePathsBase:
    def __init__(self):
        self._write_paths: List[str] = []
        self._blocks: List[Block] = []

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        try:
            iter(write_paths)
        except TypeError:
            pass
        else:
            self._write_paths.extend(write_paths)
        try:
            iter(blocks)
        except TypeError:
            pass
        else:
            self._blocks.extend(blocks)

    def write_paths(self) -> List[str]:
        return self._write_paths

    def blocks(self) -> List[Block]:
        return self._blocks


class CapturedBlockWritePaths:
    def __init__(self, wrapped=CapturedBlockWritePathsBase()):
        self._wrapped = wrapped

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        return (
            self._wrapped.extend(write_paths, blocks)
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.extend.remote(write_paths, blocks))
        )

    def write_paths(self) -> List[str]:
        return (
            self._wrapped.write_paths()
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.write_paths.remote())
        )

    def blocks(self) -> List[Block]:
        return (
            self._wrapped.blocks()
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.blocks.remote())
        )


class UuidBlockWritePathProvider(FilenameProvider):
    """Block write path provider implementation that writes each
    dataset block out to a file of the form: {base_path}/{uuid}
    """

    def __init__(
        self,
        capture_object: CapturedBlockWritePaths,
        base_path: Optional[str] = None,
        content_type: Optional[ContentType] = None,
        content_encoding: Optional[ContentEncoding] = None,
    ):
        self.base_path = base_path
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.write_paths: List[str] = []
        self.blocks: List[Block] = []
        self.capture_object = capture_object

    def __del__(self):
        if self.write_paths or self.blocks:
            self.capture_object.extend(
                self.write_paths,
                self.blocks,
            )

    def get_filename_for_block(
        self,
        block: Block,
        task_index: int,
        block_index: int,
    ) -> str:
        if self.base_path is None:
            raise ValueError(
                "Base path must be provided to UuidBlockWritePathProvider",
            )
        return self._get_write_path_for_block(
            base_path=self.base_path,
            block=block,
            block_index=block_index,
        )

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        block: Optional[Block] = None,
        **kwargs,
    ) -> str:
        # Generate base UUID filename
        filename = str(uuid4())

        # Add content type extension if available
        if self.content_type:
            content_type_extension = None
            content_type_extension = CONTENT_TYPE_TO_EXT.get(self.content_type)
            if content_type_extension:
                filename += content_type_extension

        # Add content encoding extension if available
        if self.content_encoding:
            encoding_extension = None
            encoding_extension = CONTENT_ENCODING_TO_EXT.get(self.content_encoding)
            if encoding_extension:
                filename += encoding_extension

        write_path = f"{base_path}/{filename}"
        self.write_paths.append(write_path)
        if block is not None:
            self.blocks.append(block)
        return filename

    def __call__(
        self,
        base_path: str,
        *,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[Block] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return self._get_write_path_for_block(
            base_path,
            filesystem=filesystem,
            dataset_uuid=dataset_uuid,
            block=block,
            block_index=block_index,
            file_format=file_format,
        )


def get_block_metadata_list(
    table: LocalTable,
    write_paths: List[str],
    blocks: List[Block],
) -> List[BlockMetadata]:
    """
    Get the block metadata for a given table.

    Args:
        table: The local table or distributed dataset to get the block metadata for
        write_paths: The list of write paths for the table
        blocks: The list of blocks to get the metadata for

    Returns:
        List of block metadata
    """
    block_meta_list: List[BlockMetadata] = []
    if not blocks:
        # this must be a local table - ensure it was written to only 1 file
        assert len(write_paths) == 1, (
            f"Expected table of type '{type(table)}' to be written to 1 "
            f"file, but found {len(write_paths)} files."
        )
        blocks = [table]
    for block in blocks:
        block_meta_list.append(get_block_metadata(block))
    return block_meta_list


def get_block_metadata(
    table: Union[LocalTable, DistributedDataset, BlockAccessor],
) -> BlockMetadata:
    """
    Get the block metadata for a given table.

    Args:
        table: The local table or distributed dataset to get the block metadata for

    Returns:
        Block metadata
    """
    table_size = None
    table_size_func = TABLE_CLASS_TO_SIZE_FUNC.get(type(table))
    if table_size_func:
        table_size = table_size_func(table)
    else:
        logger.warning(f"Unable to estimate '{type(table)}' table size.")
    if isinstance(table, BlockAccessor):
        table = table.to_block()
    return BlockMetadata(
        num_rows=get_table_length(table),
        size_bytes=table_size,
        schema=None,
        input_files=None,
        exec_stats=None,
    )


def _filter_kwargs_for_external_readers(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter out DeltaCAT system kwargs that external file readers don't expect.

    Use this when passing kwargs to external libraries like PyArrow, Pandas, Polars, etc.
    This removes all DeltaCAT-specific parameters that would cause TypeErrors in external readers.

    Args:
        kwargs: The dictionary of arguments to filter

    Returns:
        Dictionary of arguments with DeltaCAT system kwargs removed
    """
    return {
        k: v
        for k, v in kwargs.items()
        if k
        not in [
            # DeltaCAT catalog/storage system kwargs
            "inner",
            "catalog",
            "ray_options_provider",
            "distributed_dataset_type",
            # DeltaCAT schema/reader kwargs
            "table_version_schema",
            "entry_params",
            # Daft-specific kwargs
            "io_config",
            "ray_init_options",
            # DeltaCAT processing kwargs
            "column_names",
            "include_columns",
            "file_reader_kwargs_provider",
            "file_path_column",
            "max_parallelism",
        ]
    }


def _filter_kwargs_for_catalog_properties(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter kwargs to only include those that CatalogProperties accepts.

    Use this when calling get_catalog_properties() or CatalogProperties.__init__().
    Uses a whitelist approach - only passes known compatible parameters.

    CatalogProperties.__init__ accepts: root, filesystem, storage
    get_catalog_properties also accepts: catalog, inner

    Args:
        kwargs: The dictionary of arguments to filter

    Returns:
        Dictionary containing only CatalogProperties-compatible kwargs
    """
    return {
        k: v
        for k, v in kwargs.items()
        if k in ["root", "filesystem", "storage", "catalog", "inner"]
    }


def _filter_kwargs_for_reader_functions(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter kwargs for internal DeltaCAT reader functions that need most params but not catalog-specific ones.

    Use this for internal DeltaCAT functions that need file reader kwargs, schema kwargs, etc.
    but should not receive catalog/storage system parameters.
    Preserves table_version_schema, entry_params, file reader kwargs, etc.

    Args:
        kwargs: The dictionary of arguments to filter

    Returns:
        Dictionary with catalog/storage system kwargs removed
    """
    return {
        k: v
        for k, v in kwargs.items()
        if k
        not in ["inner", "catalog", "ray_options_provider", "distributed_dataset_type"]
    }


def _extract_content_metadata(
    manifest_entry: ManifestEntry,
) -> Tuple[ContentType, ContentEncoding, str]:
    """
    Extract content type, encoding, and path from manifest entry.

    Args:
        manifest_entry: The manifest entry to extract the content metadata from

    Returns:
        Tuple of content type, encoding, and path
    """
    content_type = manifest_entry.meta.content_type
    assert content_type, f"Unknown content type for manifest entry: {manifest_entry}"
    content_type = ContentType(content_type)

    content_encoding = manifest_entry.meta.content_encoding
    assert (
        content_encoding
    ), f"Unknown content encoding for manifest entry: {manifest_entry}"
    content_encoding = ContentEncoding(content_encoding)

    path = manifest_entry.uri
    if path is None:
        path = manifest_entry.url

    return content_type, content_encoding, path


def _extract_partial_download_params(
    manifest_entry: ManifestEntry,
) -> Optional[PartialFileDownloadParams]:
    """
    Extract partial file download parameters from manifest entry.

    Args:
        manifest_entry: The manifest entry to extract the partial file download parameters from

    Returns:
        Partial file download parameters
    """
    if not manifest_entry.meta or not manifest_entry.meta.content_type_parameters:
        return None

    for type_params in manifest_entry.meta.content_type_parameters:
        if isinstance(type_params, PartialFileDownloadParams):
            return type_params
    return None


def _create_retry_wrapper():
    """
    Create a standardized Tenacity Retrying wrapper for file operations.

    Returns:
        Tenacity Retrying wrapper
    """
    return Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )


def _remove_file_path_column(
    include_columns: Optional[List[str]],
    file_path_column: Optional[str],
) -> Optional[List[str]]:
    """Remove the file path system column from the include_columns list.

    Args:
        include_columns: The list of columns to include in a selection
        file_path_column: Optional file path system column name to remove from the selection

    Returns:
        List of columns to include without the file path system column
    """
    if file_path_column and include_columns:
        return [col for col in include_columns if col != file_path_column]
    return include_columns


def _prepare_download_arguments(
    table_type: DatasetType,
    column_names: Optional[List[str]],
    include_columns: Optional[List[str]],
    file_reader_kwargs_provider: Optional[ReadKwargsProvider],
    file_path_column: Optional[str],
    **kwargs,
) -> Dict[str, Any]:
    """Prepare standardized arguments for download operations.

    Args:
        table_type: The type of table to download
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs
        file_path_column: The file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Dictionary of arguments for the download operation
    """
    reader_kwargs = _filter_kwargs_for_external_readers(kwargs)
    processed_include_columns = _remove_file_path_column(
        include_columns, file_path_column
    )

    return {
        "table_type": table_type,
        "column_names": column_names,
        "include_columns": processed_include_columns,
        "file_reader_kwargs_provider": file_reader_kwargs_provider,
        "file_path_column": file_path_column,
        **reader_kwargs,
    }


def _handle_retryable_error(e: Exception, path: str, operation: str, error_class: type):
    """Handle retryable errors with standardized error message."""
    raise error_class(
        f"Retry {operation} for: {path} after receiving {type(e).__name__}: {e}"
    ) from e


def _handle_non_retryable_error(
    e: Exception, path: str, operation: str, error_class: type, extra_context: str = ""
):
    """Handle non-retryable errors with logging and standardized error message."""
    context = f" {extra_context}" if extra_context else ""
    logger.warning(
        f"{operation.title()} has failed for {path}{context}. Error: {e}",
        exc_info=True,
    )
    raise error_class(
        f"{operation.title()} has failed for {path}{context}: Error: {e}"
    ) from e


def from_manifest_table(
    manifest_table: Union[LocalDataset, DistributedDataset],
    read_as: DatasetType = DatasetType.DAFT,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> Dataset:
    """
    Read a manifest table (containing file paths and metadata) and download the actual data.

    This utility function takes the output from a schemaless table read (which returns
    manifest entries instead of data) and downloads the actual file contents.

    Args:
        manifest_table: Dataset containing manifest entries with file paths and metadata
        read_as: The type of dataset to return (DAFT, RAY_DATASET, PYARROW, etc.)
        schema: Optional PyArrow schema to enforce consistent column names across formats
        **kwargs: Additional arguments forwarded to download functions

    Returns:
        Dataset containing the actual file contents
    """
    # Convert the manifest table to pandas for easier processing
    # TODO(pdames): Iterate over each input manifest table in its native format
    manifest_df = to_pandas(manifest_table)

    # Reconstruct ManifestEntry objects from the manifest data
    manifest_entries = []
    for _, row in manifest_df.iterrows():
        # Create ManifestMeta from the row data
        meta = ManifestMeta.of(
            content_length=row.get("meta_content_length"),
            record_count=row.get("meta_record_count"),
            content_type=row.get("meta_content_type"),
            content_encoding=row.get("meta_content_encoding"),
        )

        # Create ManifestEntry
        entry = ManifestEntry.of(
            url=row["path"],
            meta=meta,
            mandatory=row.get("mandatory", True),
            uuid=row.get("id"),
        )
        manifest_entries.append(entry)

    # Create a new Manifest from the entries
    reconstructed_manifest = Manifest.of(entries=manifest_entries)

    # Add schema to kwargs if provided
    if schema is not None:
        kwargs["table_version_schema"] = schema

    # Choose the appropriate download function based on dataset type
    if read_as in DatasetType.distributed():
        # Use distributed download function
        # Map DatasetType to DistributedDatasetType
        distributed_type_map = {
            DatasetType.DAFT: DistributedDatasetType.DAFT,
            DatasetType.RAY_DATASET: DistributedDatasetType.RAY_DATASET,
        }
        distributed_dataset_type = distributed_type_map.get(read_as)
        if distributed_dataset_type is None:
            raise ValueError(f"Unsupported distributed dataset type: {read_as}")

        return download_manifest_entries_distributed(
            manifest=reconstructed_manifest,
            distributed_dataset_type=distributed_dataset_type,
            **kwargs,
        )
    else:
        # Use local download function
        return download_manifest_entries(
            manifest=reconstructed_manifest,
            table_type=read_as,
            **kwargs,
        )


def download_manifest_entries(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = 1,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> LocalDataset:
    """Download all entries in the manifest.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        max_parallelism: Maximum parallelism to use
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local dataset
    """
    if max_parallelism and max_parallelism <= 1:
        return _download_manifest_entries(
            manifest,
            table_type,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            file_path_column,
            **kwargs,
        )
    else:
        return _download_manifest_entries_parallel(
            manifest,
            table_type,
            max_parallelism,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            file_path_column,
            **kwargs,
        )


def _download_manifest_entries(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> LocalDataset:
    """Download all entries in the manifest.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local dataset
    """
    download_args = _prepare_download_arguments(
        table_type,
        column_names,
        include_columns,
        file_reader_kwargs_provider,
        file_path_column,
        **kwargs,
    )
    result = []
    for e in manifest.entries:
        catalog_kwargs = _filter_kwargs_for_catalog_properties(kwargs)
        manifest_entry = reconstruct_manifest_entry_url(e, **catalog_kwargs)
        result.append(
            download_manifest_entry(manifest_entry=manifest_entry, **download_args)
        )

    return result


@ray.remote
def download_manifest_entry_ray(
    manifest_entry: ManifestEntry,
    table_type: DatasetType = DatasetType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    content_type: Optional[ContentType] = None,
    content_encoding: Optional[ContentEncoding] = None,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> LocalTable:
    """
    Ray remote function for downloading manifest entries.

    Args:
        manifest_entry: The manifest entry to download
        table_type: Dataset type to load the entry into
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        content_type: Optional content type of the file
        content_encoding: Optional content encoding of the file
        filesystem: Optional PyArrow filesystem to use to read the file
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local table
    """
    # Make sure we normalize the table type to PyArrow to provide the correct
    # input type to from_arrow_refs
    effective_table_type = table_type
    if table_type == DatasetType.RAY_DATASET:
        effective_table_type = DatasetType.PYARROW

    # Call the regular download function
    reconstructed_manifest_entry = reconstruct_manifest_entry_url(
        manifest_entry, **kwargs
    )
    result = download_manifest_entry(
        manifest_entry=reconstructed_manifest_entry,
        table_type=effective_table_type,
        column_names=column_names,
        include_columns=include_columns,
        file_reader_kwargs_provider=file_reader_kwargs_provider,
        content_type=content_type,
        content_encoding=content_encoding,
        filesystem=filesystem,
        file_path_column=file_path_column,
        **kwargs,
    )

    # Convert Polars DataFrame to Arrow Table for Ray dataset compatibility
    if isinstance(result, pl.DataFrame):
        result = result.to_arrow()

    # Cast string_view columns to string to avoid cloudpickle issues
    if isinstance(result, pa.Table):
        result = _cast_string_view_to_string(result)

    return result


def download_manifest_entries_distributed(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: Optional[
        DistributedDatasetType
    ] = DistributedDatasetType.RAY_DATASET,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> DistributedDataset:
    """Download all entries in the manifest using the given distributed dataset type.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        max_parallelism: Maximum parallelism to use
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        ray_options_provider: Optional provider of Ray options
        distributed_dataset_type: Optional distributed dataset type to use
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Distributed dataset
    """
    params = {
        "manifest": manifest,
        "table_type": table_type,
        "max_parallelism": max_parallelism,
        "column_names": column_names,
        "include_columns": include_columns,
        "file_reader_kwargs_provider": file_reader_kwargs_provider,
        "ray_options_provider": ray_options_provider,
        "file_path_column": file_path_column,
        **kwargs,
    }

    if (
        distributed_dataset_type
        and distributed_dataset_type.value == DistributedDatasetType.RAY_DATASET.value
    ):
        result = _download_manifest_entries_ray_data_distributed(**params)
        return result
    elif distributed_dataset_type is not None:
        params["distributed_dataset_type"] = distributed_dataset_type
        return _download_manifest_entries_all_dataset_distributed(**params)
    else:
        raise ValueError(
            f"Distributed dataset type {distributed_dataset_type} not supported."
        )


def _cast_string_view_to_string(table: pa.Table) -> pa.Table:
    """
    Cast any string_view columns to string type for Ray dataset compatibility.

    This addresses compatibility issues where Ray datasets may have trouble with
    string_view columns written by Polars to Feather.

    Args:
        table: PyArrow table that may contain string_view columns

    Returns:
        PyArrow table with string_view columns cast to string type
    """
    if not isinstance(table, pa.Table):
        return table

    schema = table.schema
    has_string_view = False

    # Check if any columns are string_view
    for field in schema:
        if pa.types.is_string_view(field.type):
            has_string_view = True
            break

    if not has_string_view:
        return table

    # Convert to pandas and back to normalize string types
    # This is a workaround since direct casting from string_view to string is not supported
    try:
        pandas_df = table.to_pandas()
        # Convert back to PyArrow table, which should use regular string type
        return pa.Table.from_pandas(pandas_df, preserve_index=False)
    except Exception:
        # If pandas conversion fails, return original table
        return table


def _download_manifest_entries_ray_data_distributed(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> DistributedDataset:
    """Download all entries in the manifest into a Ray dataset.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        max_parallelism: Maximum parallelism to use
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        ray_options_provider: Optional provider of Ray options
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Ray dataset
    """
    table_pending_ids = []
    manifest_entries = manifest.entries

    if manifest_entries:
        table_pending_ids = invoke_parallel(
            manifest_entries,
            download_manifest_entry_ray,
            table_type,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            max_parallelism=max_parallelism,
            options_provider=ray_options_provider,
            file_path_column=file_path_column,
            **kwargs,  # Pass through kwargs like include_paths
        )

    create_func = _get_table_type_function(
        table_type, TABLE_TYPE_TO_DATASET_CREATE_FUNC_REFS, "dataset create"
    )
    return create_func(table_pending_ids)


def _download_manifest_entries_all_dataset_distributed(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: Optional[DatasetType] = DatasetType.DAFT,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> DistributedDataset:
    """Download all entries in the manifest into a distributed dataset other than Ray Dataset.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        max_parallelism: Maximum parallelism to use
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        ray_options_provider: Optional provider of Ray options
        distributed_dataset_type: Optional distributed dataset type to use
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Distributed dataset
    """
    # Group manifest entries by content type instead of validating consistency
    # Filter out table_version_schema from kwargs passed to catalog properties
    filtered_kwargs = _filter_kwargs_for_catalog_properties(kwargs)
    urls_by_content_type = group_manifest_urls_by_content_type(
        manifest,
        **filtered_kwargs,
    )

    # If only one content type, use the original single-reader logic
    if len(urls_by_content_type) == 1:
        content_type, content_encoding = next(iter(urls_by_content_type.keys()))
        urls = next(iter(urls_by_content_type.values()))

        # Keep table_version_schema for the reader, but filter other system kwargs
        reader_kwargs = _filter_kwargs_for_reader_functions(kwargs)

        try:
            reader_func = DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC[
                distributed_dataset_type.value
            ]
        except KeyError:
            raise ValueError(
                f"Unsupported distributed dataset type={distributed_dataset_type}. "
                f"Supported types: {list(DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC.keys())}"
            )

        return reader_func(
            uris=urls,
            content_type=content_type,
            content_encoding=content_encoding,
            column_names=column_names,
            include_columns=include_columns,
            read_func_kwargs_provider=file_reader_kwargs_provider,
            ray_options_provider=ray_options_provider,
            file_path_column=file_path_column,
            **reader_kwargs,
        )

    # Multiple content types - read each group and union them (only for Daft)
    if distributed_dataset_type != DistributedDatasetType.DAFT:
        raise ValueError(
            f"Mixed content types are only supported for Daft datasets. "
            f"Got {len(urls_by_content_type)} different content types with dataset type {distributed_dataset_type}"
        )

    # Keep table_version_schema for the reader, but filter other system kwargs
    reader_kwargs = _filter_kwargs_for_reader_functions(kwargs)

    try:
        reader_func = DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC[
            distributed_dataset_type.value
        ]
    except KeyError:
        raise ValueError(
            f"Unsupported distributed dataset type={distributed_dataset_type}. "
            f"Supported types: {list(DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC.keys())}"
        )

    # Read each content type group into a separate DataFrame
    dataframes = []
    for (content_type, content_encoding), urls in urls_by_content_type.items():
        df = reader_func(
            uris=urls,
            content_type=content_type,
            content_encoding=content_encoding,
            column_names=column_names,
            include_columns=include_columns,
            read_func_kwargs_provider=file_reader_kwargs_provider,
            ray_options_provider=ray_options_provider,
            file_path_column=file_path_column,
            **reader_kwargs,
        )
        dataframes.append(df)

    # Union all DataFrames using Daft's union_all
    if len(dataframes) == 1:
        return dataframes[0]

    result = dataframes[0]
    for df in dataframes[1:]:
        result = result.union_all(df)

    return result


def _download_manifest_entries_parallel(
    manifest: Manifest,
    table_type: DatasetType = DatasetType.PYARROW,
    max_parallelism: Optional[int] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> LocalDataset:
    """Download all entries in the manifest into a local dataset using multiprocessing.

    Args:
        manifest: The manifest containing the entries to download
        table_type: Dataset type to load the entries into
        max_parallelism: Maximum parallel processes to use for entry downloads
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local dataset
    """
    download_args = _prepare_download_arguments(
        table_type,
        column_names,
        include_columns,
        file_reader_kwargs_provider,
        file_path_column,
        **kwargs,
    )

    entries_to_process = []
    for e in manifest.entries:
        catalog_kwargs = _filter_kwargs_for_catalog_properties(kwargs)
        manifest_entry = reconstruct_manifest_entry_url(e, **catalog_kwargs)
        entries_to_process.append(manifest_entry)

    tables = []
    pool = multiprocessing.Pool(max_parallelism)

    downloader = partial(download_manifest_entry, **download_args)
    for table in pool.map(downloader, entries_to_process):
        tables.append(table)
    return tables


def download_manifest_entry(
    manifest_entry: ManifestEntry,
    table_type: DatasetType = DatasetType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    content_type: Optional[ContentType] = None,
    content_encoding: Optional[ContentEncoding] = None,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    file_path_column: Optional[str] = None,
    **kwargs,
) -> LocalTable:
    """Download a single entry in the manifest into a local table.

    Args:
        manifest_entry: The manifest entry to download
        table_type: Dataset type to load the entry into
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        content_type: Optional content type of the file
        content_encoding: Optional content encoding of the file
        filesystem: Optional PyArrow filesystem to use to read the file
        file_path_column: Optional file path system column name
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local table
    """
    # Extract manifest metadata
    (
        extracted_content_type,
        extracted_content_encoding,
        path,
    ) = _extract_content_metadata(manifest_entry)
    content_type = content_type or extracted_content_type
    content_encoding = content_encoding or extracted_content_encoding

    # Extract partial download parameters
    partial_file_download_params = _extract_partial_download_params(manifest_entry)

    # Filter kwargs and process file path column
    reader_kwargs = _filter_kwargs_for_external_readers(kwargs)
    processed_include_columns = _remove_file_path_column(
        include_columns, file_path_column
    )

    # Create retry wrapper and read file
    retrying = _create_retry_wrapper()
    table = retrying(
        read_file,
        path,
        content_type,
        content_encoding,
        table_type,
        column_names,
        processed_include_columns,
        file_reader_kwargs_provider,
        partial_file_download_params,
        filesystem,
        **reader_kwargs,
    )

    # Add file path column if requested
    if file_path_column:
        if isinstance(table, papq.ParquetFile):
            logger.warning(
                f"Skipping file_path_column '{file_path_column}' for lazily materialized ParquetFile. "
                f"File path information can be retrieved from the ParquetFile object's metadata. "
                f"Use read_as=DatasetType.PYARROW to materialize with file path column."
            )
        else:
            table = append_column_to_table(table, file_path_column, manifest_entry.uri)

    return table


@categorize_errors
def read_file(
    path: str,
    content_type: ContentType,
    content_encoding: ContentEncoding = ContentEncoding.IDENTITY,
    table_type: DatasetType = DatasetType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    **kwargs,
) -> LocalTable:
    """Read a file into a local table.

    Args:
        path: The path to the file to read
        content_type: The content type of the file
        content_encoding: The content encoding of the file
        table_type: Dataset type to load the file into
        column_names: The list of column names in the table
        include_columns: The list of columns to include in the selection
        file_reader_kwargs_provider: Optional per-content-type provider of file reader kwargs,
            (e.g., to pass in a custom schema for a Parquet file)
        partial_file_download_params: Optional partial file download parameters
        filesystem: Optional PyArrow filesystem to use to read the file
        **kwargs: Additional arguments to pass to the file reader

    Returns:
        Local table
    """
    reader = TABLE_TYPE_TO_READER_FUNC[table_type.value]
    try:
        table = reader(
            path,
            content_type.value,
            content_encoding.value,
            filesystem,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            partial_file_download_params,
            **kwargs,
        )
        return table
    except FileNotFoundError as e:
        # FileNotFoundError is a not retryable like other OSError types - catch and handle first
        _handle_non_retryable_error(
            e,
            path,
            "read",
            NonRetryableDownloadTableError,
            f"and content_type={content_type} and encoding={content_encoding}",
        )
    except RETRYABLE_TRANSIENT_ERRORS as e:
        _handle_retryable_error(e, path, "download", RetryableDownloadTableError)
    except BaseException as e:
        _handle_non_retryable_error(
            e,
            path,
            "read",
            NonRetryableDownloadTableError,
            f"and content_type={content_type} and encoding={content_encoding}",
        )
